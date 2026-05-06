"""Plot UpSet diagram of anomaly set intersections.

Computes all four anomaly flags (median_zero, flat, area_ratio, outside_range)
from full lake_info + lake_area parquet data, then produces an UpSet plot.

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
    uv run python scripts/plot_anomaly_upset.py

    uv run python scripts/plot_anomaly_upset.py --output-dir data/figures/upset
    uv run python scripts/plot_anomaly_upset.py --min-size 5
    uv run python scripts/plot_anomaly_upset.py --limit 5000
    uv run python scripts/plot_anomaly_upset.py --skip-flat
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd

from lakeanalysis.logger import Logger
from lakeanalysis.quality import (
    AgreementConfig,
    FlatnessFilterConfig,
    classify_agreement,
    classify_outside_range,
    compute_area_ratio,
    compute_flatness_metrics,
)
from lakeviz.quality import plot_anomaly_upset
from lakeviz.layout import save

log = logging.getLogger(__name__)

_CHUNK_SIZE = 10_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot UpSet diagram of anomaly set intersections."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        metavar="DIR",
        help="Parquet data directory (default from PARQUET_DATA_DIR env var).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/figures/upset",
        metavar="DIR",
        help="Output directory (default: data/figures/upset).",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=0,
        metavar="N",
        help="Minimum intersection size to display (default: 0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit number of lakes (for testing).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=_CHUNK_SIZE,
        metavar="N",
        help="Chunk size for flatness computation (default: 10000).",
    )
    parser.add_argument(
        "--skip-flat",
        action="store_true",
        help="Skip flatness computation (is_flat will always be False).",
    )
    return parser.parse_args()


def _aggregate_lake_stats(data_dir: Path, limit: int | None = None) -> pd.DataFrame:
    """Phase 1: DuckDB aggregation for min/max/median/atlas_area."""
    from lakesource.parquet.client import DuckDBClient

    client = DuckDBClient(data_dir=data_dir)

    limit_sql = f"LIMIT {limit}" if limit else ""
    df = client.query_df(
        f"SELECT li.hylak_id, li.lake_area AS atlas_area, "
        f"MIN(la.water_area) / 1e6 AS min_area_km2, "
        f"MAX(la.water_area) / 1e6 AS max_area_km2, "
        f"MEDIAN(la.water_area) / 1e6 AS rs_area_median "
        f"FROM lake_info li "
        f"JOIN lake_area la ON li.hylak_id = la.hylak_id "
        f"GROUP BY li.hylak_id, li.lake_area "
        f"ORDER BY li.hylak_id "
        f"{limit_sql}"
    )
    return df


def _compute_flatness_chunked(
    data_dir: Path,
    hylak_ids: list[int],
    chunk_size: int,
) -> dict[int, bool]:
    """Phase 2: Compute flatness per lake in chunks."""
    from lakesource.parquet.client import DuckDBClient

    flat_config = FlatnessFilterConfig()
    result: dict[int, bool] = {}
    n_chunks = (len(hylak_ids) + chunk_size - 1) // chunk_size

    for i in range(n_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, len(hylak_ids))
        chunk_ids = hylak_ids[start:end]

        client = DuckDBClient(data_dir=data_dir)
        placeholders = ",".join("?" for _ in chunk_ids)
        df = client.query_df(
            f"SELECT hylak_id, water_area FROM lake_area "
            f"WHERE hylak_id IN ({placeholders}) "
            f"ORDER BY hylak_id, year_month",
            parameters=chunk_ids,
        )

        for hid in chunk_ids:
            lake_df = df[df["hylak_id"] == hid]
            if lake_df.empty:
                result[hid] = False
                continue
            metrics = compute_flatness_metrics(
                lake_df,
                value_column="water_area",
                round_digits=flat_config.round_digits,
            )
            result[hid] = metrics["dominant_ratio"] >= flat_config.dominant_ratio_threshold

        log.info(
            "[%d/%d] flatness chunk %d-%d: %d lakes",
            i + 1, n_chunks, chunk_ids[0], chunk_ids[-1], end - start,
        )

    return result


def _load_flags_parquet(
    data_dir: Path,
    limit: int | None = None,
    chunk_size: int = _CHUNK_SIZE,
    skip_flat: bool = False,
) -> pd.DataFrame:
    """Compute anomaly flags from full lake_info + lake_area via parquet."""
    log.info("Phase 1: DuckDB aggregation for lake stats...")
    stats = _aggregate_lake_stats(data_dir, limit=limit)
    if stats.empty:
        return pd.DataFrame()
    log.info("Aggregated stats for %d lakes", len(stats))

    agreement_config = AgreementConfig()
    ratio = compute_area_ratio(
        stats["rs_area_median"].values,
        stats["atlas_area"].values,
    )
    agreement = classify_agreement(ratio, agreement_config)

    is_median_zero = stats["rs_area_median"].values == 0.0
    is_area_ratio = np.isin(agreement, ["poor", "extreme"])

    is_outside_range = np.zeros(len(stats), dtype=bool)
    for idx, row in stats.iterrows():
        or_result = classify_outside_range(
            atlas_area=float(row["atlas_area"]),
            min_area=float(row["min_area_km2"]),
            max_area=float(row["max_area_km2"]),
        )
        is_outside_range[idx] = or_result["is_outside_range"]

    is_flat = np.zeros(len(stats), dtype=bool)
    if not skip_flat:
        log.info("Phase 2: Computing flatness per lake (chunk_size=%d)...", chunk_size)
        hylak_ids = stats["hylak_id"].astype(int).tolist()
        flat_map = _compute_flatness_chunked(data_dir, hylak_ids, chunk_size)
        for idx, row in stats.iterrows():
            is_flat[idx] = flat_map.get(int(row["hylak_id"]), False)

    return pd.DataFrame({
        "hylak_id": stats["hylak_id"].astype(int),
        "is_median_zero": is_median_zero,
        "is_flat": is_flat,
        "is_area_ratio": is_area_ratio,
        "is_outside_range": is_outside_range,
    })


def _load_flags_postgres(limit: int | None = None) -> pd.DataFrame:
    """Load area_anomalies and decode anomaly_flags into boolean columns."""
    from lakesource.postgres import series_db
    from lakeanalysis.quality import decode_anomaly_flags

    limit_sql = f"LIMIT {limit}" if limit else ""
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags "
                f"FROM area_anomalies ORDER BY hylak_id {limit_sql}"
            )
            rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        hid = int(row[0])
        flags = int(row[4]) if row[4] is not None else 0
        decoded = decode_anomaly_flags(flags)
        records.append({"hylak_id": hid, **decoded})

    return pd.DataFrame(records)


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    data_dir = Path(args.data_dir) if args.data_dir else Path(
        os.environ.get("PARQUET_DATA_DIR", os.environ.get("DATA_DIR", "data/parquet"))
    )
    df = _load_flags_parquet(
        data_dir,
        limit=args.limit,
        chunk_size=args.chunk_size,
        skip_flat=args.skip_flat,
    )

    if df.empty:
        log.warning("No anomaly data found")
        return

    n_flagged = df[
        df["is_median_zero"] | df["is_flat"] | df["is_area_ratio"] | df["is_outside_range"]
    ].shape[0]
    log.info(
        "Loaded %d records, %d flagged (%.1f%%)",
        len(df), n_flagged, n_flagged / len(df) * 100,
    )
    for col in ["is_median_zero", "is_flat", "is_area_ratio", "is_outside_range"]:
        n = int(df[col].sum())
        log.info("  %s: %d (%.1f%%)", col, n, n / len(df) * 100)

    fig = plot_anomaly_upset(df, min_size=args.min_size)
    save(fig, output_dir / "anomaly_upset.png")

    log.info("UpSet plot saved to %s", output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_anomaly_upset")
    run(args)


if __name__ == "__main__":
    main()