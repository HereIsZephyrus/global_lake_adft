"""Batch EOT fitting for all lakes with per-chunk parallelism.

Each chunk of ``--chunk-size`` consecutive hylak_ids is processed as follows:
  1. Lake area and frozen-month data are read from the DB in two bulk queries.
  2. The chunk is split into ``--workers`` equal sub-batches.
  3. Each sub-batch is submitted to a persistent ``ProcessPoolExecutor``.
  4. The main process waits for **all workers** to finish (barrier sync).
  5. Results are written to ``eot_results`` and ``eot_extremes`` in bulk upserts.
  6. The next chunk begins only after both upserts complete.

Each lake is fitted for every combination of tail (high/low) and threshold
quantile supplied via ``--threshold-quantiles``.  Failed fits always produce
a row in ``eot_results`` with ``converged=False`` and an ``error_message``,
so the checkpoint counter is never under-counted.

Usage examples:
    uv run python scripts/run_eot_batch.py
    uv run python scripts/run_eot_batch.py --workers 10 --chunk-size 10000
    uv run python scripts/run_eot_batch.py --threshold-quantiles 0.95 0.98
    uv run python scripts/run_eot_batch.py --tail high --limit-id 50000
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import math
from decimal import Decimal
from pathlib import Path

import pandas as pd

from lakesource.postgres import (
    ensure_eot_results_table,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_eot_extremes,
    upsert_eot_results,
)
from lakeanalysis.eot import EOTEstimator
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Worker helpers — must be module-level so ProcessPoolExecutor can pickle them
# ---------------------------------------------------------------------------

def _fit_one(
    estimator: EOTEstimator,
    hylak_id: int,
    df: pd.DataFrame,
    tail: str,
    quantile: float,
    frozen_year_months: set[int],
) -> tuple[dict, list[dict]]:
    """Fit one (lake, tail, quantile) triple.

    Returns:
        result_row:   A dict for ``eot_results``.
        extreme_rows: A list of dicts for ``eot_extremes`` (empty on failure).
    """
    q_decimal = str(Decimal(str(quantile)))
    error_row = {
        "hylak_id": hylak_id,
        "tail": tail,
        "threshold_quantile": q_decimal,
        "converged": False,
        "log_likelihood": None,
        "threshold": None,
        "n_extremes": None,
        "n_observations": None,
        "n_frozen_months": int(len(frozen_year_months)),
        "beta0": None,
        "beta1": None,
        "sin_1": None,
        "cos_1": None,
        "sigma": None,
        "xi": None,
        "error_message": None,
    }
    try:
        fit = estimator.fit(
            df,
            tail=tail,
            threshold_quantile=quantile,
            frozen_year_months=frozen_year_months,
        )
        p = fit.params
        ll = fit.log_likelihood
        result_row = {
            "hylak_id": hylak_id,
            "tail": tail,
            "threshold_quantile": q_decimal,
            "converged": bool(fit.converged),
            "log_likelihood": float(ll) if math.isfinite(ll) else None,
            "threshold": float(fit.threshold),
            "n_extremes": int(len(fit.extremes)),
            "n_observations": int(fit.series.n_obs),
            "n_frozen_months": int(len(frozen_year_months)),
            "beta0": p.get("beta0"),
            "beta1": p.get("beta1"),
            "sin_1": p.get("sin_1"),
            "cos_1": p.get("cos_1"),
            "sigma": p.get("sigma"),
            "xi": p.get("xi"),
            "error_message": None,
        }
        extreme_rows = [
            {
                "hylak_id": hylak_id,
                "tail": tail,
                "threshold_quantile": q_decimal,
                "cluster_id": int(row["cluster_id"]),
                "cluster_size": int(row["cluster_size"]),
                "year": int(row["year"]),
                "month": int(row["month"]),
                "water_area": float(row["original_value"]),
                "threshold_at_event": float(row["threshold"]),
            }
            for _, row in fit.extremes.iterrows()
        ]
        return result_row, extreme_rows
    except Exception as exc:
        log.debug("hylak_id=%d tail=%s q=%.2f error: %s", hylak_id, tail, quantile, exc)
        error_row["error_message"] = str(exc)[:500]
        return error_row, []


def _process_sub_batch(
    sub_batch: list[tuple[int, pd.DataFrame]],
    frozen_map: dict[int, set[int]],
    tails: list[str],
    quantiles: list[float],
) -> tuple[list[dict], list[dict]]:
    """Fit every (lake, tail, quantile) combination in a sub-batch.

    Called inside a worker process.  Creates one ``EOTEstimator`` per call so
    the object is never shared across processes.

    Returns:
        (result_rows, extreme_rows) — two flat lists ready for bulk upsert.
    """
    estimator = EOTEstimator()
    result_rows: list[dict] = []
    extreme_rows: list[dict] = []
    for hylak_id, df in sub_batch:
        frozen = frozen_map.get(hylak_id, set())
        for tail in tails:
            for quantile in quantiles:
                r, exts = _fit_one(estimator, hylak_id, df, tail, quantile, frozen)
                result_rows.append(r)
                extreme_rows.extend(exts)
    return result_rows, extreme_rows


# ---------------------------------------------------------------------------
# Chunk-level orchestration
# ---------------------------------------------------------------------------

def _split_evenly(items: list, n: int) -> list[list]:
    """Split *items* into at most *n* roughly equal sub-lists."""
    if not items:
        return []
    n = min(n, len(items))
    sub_size = math.ceil(len(items) / n)
    return [items[i : i + sub_size] for i in range(0, len(items), sub_size)]


# Count processable lakes: area_quality is the complement of area_anomalies.
_COUNT_SOURCE_SQL = """
SELECT COUNT(*)
FROM area_quality
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
"""

# Count distinct (hylak_id, threshold_quantile) pairs already written.
# A chunk is done when this equals source_count × len(quantiles).
_COUNT_DONE_SQL = """
SELECT COUNT(*)
FROM (
    SELECT DISTINCT hylak_id, threshold_quantile
    FROM eot_results
    WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
      AND threshold_quantile = ANY(%(quantiles)s)
) sub
"""


def _is_chunk_done(conn, chunk_start: int, chunk_end: int, quantiles: list[float]) -> bool:
    """Return True when every (lake, quantile) combination has been written."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_COUNT_SOURCE_SQL, params)
        source_count: int = cur.fetchone()[0]
        if source_count == 0:
            return True
        q_strings = [str(Decimal(str(q))) for q in quantiles]
        cur.execute(_COUNT_DONE_SQL, {**params, "quantiles": q_strings})
        done_count: int = cur.fetchone()[0]
    return done_count >= source_count * len(quantiles)


def _iter_chunks(chunk_size: int, limit_id: int | None) -> list[tuple[int, int]]:
    """Return all (chunk_start, chunk_end) pairs for the full hylak_id space."""
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(hylak_id) FROM lake_info")
            row = cur.fetchone()
    max_id = int(row[0]) if row and row[0] is not None else 0
    if limit_id is not None:
        max_id = min(max_id, limit_id - 1)
    chunks: list[tuple[int, int]] = []
    for start in range(0, max_id + 1, chunk_size):
        end = start + chunk_size
        if limit_id is not None:
            end = min(end, limit_id)
        chunks.append((start, end))
    return chunks


def _process_chunk(
    chunk_start: int,
    chunk_end: int,
    workers: int,
    tails: list[str],
    quantiles: list[float],
    executor: concurrent.futures.ProcessPoolExecutor,
) -> tuple[list[dict], list[dict]]:
    """Read one chunk, fan out to worker processes, barrier-sync, collect results."""
    with series_db.connection_context() as conn:
        lake_map = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
        frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

    if not lake_map:
        return [], []

    lakes = list(lake_map.items())
    sub_batches = _split_evenly(lakes, workers)

    futures = [
        executor.submit(_process_sub_batch, sub, frozen_map, tails, quantiles)
        for sub in sub_batches
    ]

    # Barrier: all workers in this chunk must finish before we proceed.
    concurrent.futures.wait(futures)

    all_results: list[dict] = []
    all_extremes: list[dict] = []
    for fut in futures:
        try:
            result_rows, extreme_rows = fut.result()
            all_results.extend(result_rows)
            all_extremes.extend(extreme_rows)
        except Exception as exc:
            log.error(
                "Worker raised an unhandled exception for chunk [%d, %d): %s",
                chunk_start,
                chunk_end,
                exc,
            )
    return all_results, all_extremes


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    tails = ["high", "low"] if args.tail == "both" else [args.tail]
    quantiles: list[float] = args.threshold_quantiles

    with series_db.connection_context() as conn:
        ensure_eot_results_table(conn)

    all_chunks = _iter_chunks(args.chunk_size, args.limit_id)
    total = len(all_chunks)
    log.info(
        "Starting batch EOT: %d chunk(s), chunk_size=%d, workers=%d, "
        "tails=%s, quantiles=%s",
        total,
        args.chunk_size,
        args.workers,
        tails,
        quantiles,
    )

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        for idx, (chunk_start, chunk_end) in enumerate(all_chunks, 1):
            with series_db.connection_context() as conn:
                if _is_chunk_done(conn, chunk_start, chunk_end, quantiles):
                    log.debug(
                        "[%d/%d] chunk %d-%d: already done, skipping",
                        idx, total, chunk_start, chunk_end - 1,
                    )
                    continue

            log.info(
                "[%d/%d] chunk %d-%d: processing...",
                idx, total, chunk_start, chunk_end - 1,
            )

            result_rows, extreme_rows = _process_chunk(
                chunk_start, chunk_end, args.workers, tails, quantiles, executor
            )

            with series_db.connection_context() as conn:
                if result_rows:
                    upsert_eot_results(conn, result_rows)
                if extreme_rows:
                    upsert_eot_extremes(conn, extreme_rows)

            log.info(
                "[%d/%d] chunk %d-%d: done (%d result row(s), %d extreme row(s))",
                idx, total, chunk_start, chunk_end - 1,
                len(result_rows), len(extreme_rows),
            )

    log.info("Batch EOT complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch EOT fitting for all lakes with per-chunk parallelism.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel worker processes per chunk.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="Number of consecutive hylak_id values processed per chunk.",
    )
    parser.add_argument(
        "--tail",
        choices=["high", "low", "both"],
        default="both",
        help="Tail(s) to fit.",
    )
    parser.add_argument(
        "--threshold-quantiles",
        nargs="+",
        type=float,
        default=[0.95, 0.98],
        help="One or more threshold quantile levels to fit per lake.",
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        help="Only process hylak_id < limit_id (useful for smoke-tests).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_eot_batch")
    run(args)


if __name__ == "__main__":
    main()
