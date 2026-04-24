"""Run the Apportionment Entropy (AE) computation pipeline.

Steps:
  1. Ensure the entropy table exists in SERIES_DB.
  2. Split the full hylak_id space into chunks of --chunk-size (default 10 000).
  3. For each pending chunk: fetch lake_area from SERIES_DB, compute AE metrics,
     persist results to data/entropy/ as parquet, then upsert to SERIES_DB.
  4. Chunks already fully recorded in entropy are skipped automatically,
     enabling safe resume after an interrupted run.
  5. Optionally display matplotlib exploration plots (--plot).

Usage examples:
    # Full run (chunked, resumable):
    uv run python scripts/run_entropy.py

    # Test with only rows where id < 5000:
    uv run python scripts/run_entropy.py --limit-id 5000

    # Test + show plots:
    uv run python scripts/run_entropy.py --limit-id 5000 --plot

    # Plot only (load all parquet from data/entropy, no recompute):
    uv run python scripts/run_entropy.py --plot-only

    # Update amplitude only (re-fetch STL from lake_info; no AE recompute):
    uv run python scripts/run_entropy.py --update-amplitude-only
    uv run python scripts/run_entropy.py --update-amplitude-only --plot

    # Adjust chunk size:
    uv run python scripts/run_entropy.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
from pathlib import Path

import logging

import numpy as np
import pandas as pd

from lakeanalysis.logger import Logger
from lakesource.postgres import ChunkedLakeProcessor, ensure_entropy_table, fetch_lake_area_chunk, fetch_seasonal_amplitude_chunk, series_db, upsert_entropy
from lakeanalysis.entropy.compute import compute_annual_ae, compute_overall_ae, compute_trend

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "entropy"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Apportionment Entropy for lake_area data.")
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process rows with id < N (for testing).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values processed per chunk (default: 10000).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Show matplotlib exploration plots after computation.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Only load from data/entropy and plot; skip computation.",
    )
    parser.add_argument(
        "--update-amplitude-only",
        action="store_true",
        help="Only refresh mean_seasonal_amplitude (CV = annual_means_std/mean_area from lake_info); update parquet and DB.",
    )
    return parser.parse_args()


def _chunk_path(chunk_start: int, chunk_end: int) -> Path:
    return DATA_DIR / f"chunk_{chunk_start:08d}_{chunk_end:08d}.parquet"


def run(limit_id: int | None = None, chunk_size: int = 10_000, show_plot: bool = False) -> None:
    """Execute the AE pipeline in resumable chunks.

    Each chunk of ``chunk_size`` consecutive hylak_id values is processed
    independently.  Results are first persisted to ``data/entropy/`` as parquet
    files, then upserted to the ``entropy`` table in SERIES_DB.  Chunks already
    fully recorded in entropy are skipped automatically, enabling safe resume.

    Args:
        limit_id: If given, only lakes with hylak_id < limit_id are processed.
        chunk_size: Number of hylak_id values per processing chunk.
        show_plot: If True, display matplotlib figures after computation.
    """
    log.info("Starting entropy pipeline, limit_id=%s, chunk_size=%d", limit_id, chunk_size)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        ensure_entropy_table(conn)

    processor = ChunkedLakeProcessor(series_db, chunk_size=chunk_size, done_table="entropy")

    def process_chunk(chunk_start: int, chunk_end: int) -> list[dict]:
        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            amplitudes = fetch_seasonal_amplitude_chunk(conn, chunk_start, chunk_end)

        rows: list[dict] = []
        for hylak_id, df in lake_frames.items():
            ae_overall = compute_overall_ae(df)
            annual_df = compute_annual_ae(df)
            trend = compute_trend(annual_df)
            rows.append(
                {
                    "hylak_id": hylak_id,
                    "ae_overall": ae_overall,
                    "sens_slope": trend["sens_slope"],
                    "change_per_decade_pct": trend["change_per_decade_pct"],
                    "mk_trend": trend["mk_trend"],
                    "mk_p": trend["mk_p"],
                    "mk_z": trend["mk_z"],
                    "mk_significant": trend["mk_significant"],
                    "mean_seasonal_amplitude": amplitudes.get(hylak_id),
                }
            )

        out_path = _chunk_path(chunk_start, chunk_end)
        pd.DataFrame(rows).to_parquet(out_path, index=False)
        log.debug("Persisted %d rows to %s", len(rows), out_path.name)

        return rows

    def upsert_chunk(rows: list[dict]) -> None:
        with series_db.connection_context() as conn:
            upsert_entropy(conn, rows)

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk, limit_id=limit_id)

    if show_plot:
        _show_plots(limit_id=limit_id)


def run_update_amplitude_only(show_plot: bool = False) -> None:
    """Refresh mean_seasonal_amplitude (CV = annual_means_std/mean_area from lake_info) for existing entropy chunks.

    Does not recompute AE. For each chunk parquet in data/entropy, re-fetches
    amplitudes from lake_info, then updates the parquet file and upserts to
    the entropy table.

    Args:
        show_plot: If True, call _show_plots after updating.
    """
    log.info("Update amplitude only (no AE recompute)")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    paths = sorted(DATA_DIR.glob("chunk_*.parquet"))
    if not paths:
        log.warning("No chunk parquet files in %s", DATA_DIR)
        if show_plot:
            _show_plots(limit_id=None)
        return

    for path in paths:
        stem = path.stem  # chunk_00000000_00010000
        parts = stem.split("_", 2)
        chunk_start = int(parts[1])
        chunk_end = int(parts[2])

        with series_db.connection_context() as conn:
            amplitudes = fetch_seasonal_amplitude_chunk(conn, chunk_start, chunk_end)

        df = pd.read_parquet(path)
        df["mean_seasonal_amplitude"] = df["hylak_id"].map(amplitudes)
        df.to_parquet(path, index=False)
        log.debug("Updated amplitude in %s", path.name)

        rows = df.to_dict("records")
        with series_db.connection_context() as conn:
            upsert_entropy(conn, rows)

    log.info("Updated amplitude for %d chunk(s)", len(paths))
    if show_plot:
        _show_plots(limit_id=None)


def _load_summary(limit_id: int | None) -> pd.DataFrame:
    """Load entropy summary from persisted parquet files.

    Only files whose hylak_id range overlaps [0, limit_id) are included.
    If limit_id is None all available parquet files are loaded.
    """
    parts: list[pd.DataFrame] = []
    for path in sorted(DATA_DIR.glob("chunk_*.parquet")):
        stem = path.stem  # chunk_00000000_00010000
        _, start_str, end_str = stem.split("_", 2)
        chunk_start = int(start_str)
        if limit_id is not None and chunk_start >= limit_id:
            continue
        df = pd.read_parquet(path)
        if limit_id is not None:
            df = df[df["hylak_id"] < limit_id]
        parts.append(df)

    if not parts:
        log.warning("No parquet files found in %s", DATA_DIR)
        return pd.DataFrame()

    summary = pd.concat(parts, ignore_index=True)
    # Backward compatibility: old parquets used column annual_means_std
    if "mean_seasonal_amplitude" not in summary.columns and "annual_means_std" in summary.columns:
        summary["mean_seasonal_amplitude"] = summary["annual_means_std"]
    return summary


def _show_plots(limit_id: int | None) -> None:
    import matplotlib.pyplot as plt

    from lakeviz.entropy import (
        plot_ae_distribution,
        plot_amplitude_histogram,
        plot_amplitude_vs_entropy,
        plot_trend_summary,
        remove_amplitude_outliers,
    )
    from lakeviz.plot_config import setup_chinese_font

    summary_df = _load_summary(limit_id)
    if summary_df.empty:
        log.warning("No data available for plotting.")
        return

    log.info("Plotting summary for %d lakes.", len(summary_df))

    summary_no_amp_outliers = remove_amplitude_outliers(summary_df)
    _write_amplitude_entropy_csv(summary_no_amp_outliers)

    setup_chinese_font()
    plot_dir = DATA_DIR / "plot"
    plot_dir.mkdir(parents=True, exist_ok=True)

    fig = plot_ae_distribution(summary_df)
    fig.savefig(plot_dir / "ae_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_trend_summary(summary_df)
    fig.savefig(plot_dir / "trend_summary.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_amplitude_histogram(summary_no_amp_outliers)
    fig.savefig(plot_dir / "amplitude_histogram.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_amplitude_vs_entropy(summary_no_amp_outliers)
    fig.savefig(plot_dir / "amplitude_vs_entropy.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    log.info("Saved plots to %s", plot_dir)


def _write_amplitude_entropy_csv(summary_df: pd.DataFrame) -> None:
    """Write OLS and correlation test results to data/entropy/amplitude_entropy.csv."""
    from scipy.stats import pearsonr, spearmanr

    df = summary_df[["mean_seasonal_amplitude", "ae_overall"]].dropna()
    if len(df) < 3:
        log.warning("Insufficient data for amplitude–entropy statistics.")
        return

    x = np.abs(df["mean_seasonal_amplitude"].to_numpy(dtype=float))
    y = 1.0 - df["ae_overall"].to_numpy(dtype=float)

    r, p_r = pearsonr(x, y)
    rho, p_rho = spearmanr(x, y)
    slope, intercept = np.polyfit(x, y, 1)

    results = pd.DataFrame([{
        "n": len(df),
        "pearson_r": r,
        "pearson_p": p_r,
        "spearman_rho": rho,
        "spearman_p": p_rho,
        "ols_slope": slope,
        "ols_intercept": intercept,
        "x_transform": "CV (annual_means_std/mean_area)",
        "y": "1 - ae_overall",
    }])

    out_path = DATA_DIR / "amplitude_entropy.csv"
    results.to_csv(out_path, index=False)
    log.info("Wrote amplitude–entropy statistics to %s", out_path)


def main() -> None:
    args = parse_args()
    Logger("run_entropy")
    if args.plot_only:
        _show_plots(limit_id=None)  # load all parquet in data/entropy
    elif args.update_amplitude_only:
        run_update_amplitude_only(show_plot=args.plot)
    else:
        run(limit_id=args.limit_id, chunk_size=args.chunk_size, show_plot=args.plot)


if __name__ == "__main__":
    main()
