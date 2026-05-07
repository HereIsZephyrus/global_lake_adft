"""Application runner for the Apportionment Entropy pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider

from .compute import compute_annual_ae, compute_overall_ae, compute_trend

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntropyRunConfig:
    data_dir: Path
    limit_id: int | None = None
    chunk_size: int = 10_000
    show_plot: bool = False


def chunk_path(data_dir: Path, chunk_start: int, chunk_end: int) -> Path:
    return data_dir / f"chunk_{chunk_start:08d}_{chunk_end:08d}.parquet"


def run_entropy(config: EntropyRunConfig) -> None:
    """Execute the AE pipeline in resumable chunks."""
    log.info(
        "Starting entropy pipeline, limit_id=%s, chunk_size=%d",
        config.limit_id,
        config.chunk_size,
    )

    provider = create_provider(SourceConfig())
    config.data_dir.mkdir(parents=True, exist_ok=True)

    provider.ensure_table("entropy")

    max_id = provider.fetch_max_hylak_id()
    if config.limit_id is not None:
        max_id = min(max_id, config.limit_id - 1)

    chunk_ranges = [
        (start, min(start + config.chunk_size, max_id + 1))
        for start in range(0, max_id + 1, config.chunk_size)
    ]

    for chunk_start, chunk_end in chunk_ranges:
        done_ids = provider.fetch_done_ids("entropy", chunk_start, chunk_end)
        lake_frames = provider.fetch_lake_area_chunk(chunk_start, chunk_end)
        amplitudes = provider.fetch_seasonal_amplitude_chunk(chunk_start, chunk_end)

        rows: list[dict[str, int | float | str | bool | None]] = []
        for hylak_id, df in lake_frames.items():
            if hylak_id in done_ids:
                continue
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

        out_path = chunk_path(config.data_dir, chunk_start, chunk_end)
        pd.DataFrame(rows).to_parquet(out_path, index=False)
        log.debug("Persisted %d rows to %s", len(rows), out_path.name)
        provider.upsert_rows("entropy", rows)

    if config.show_plot:
        show_entropy_plots(config.data_dir, limit_id=config.limit_id)


def run_update_amplitude_only(data_dir: Path, show_plot: bool = False) -> None:
    """Refresh mean_seasonal_amplitude for existing entropy chunks."""
    log.info("Update amplitude only (no AE recompute)")
    data_dir.mkdir(parents=True, exist_ok=True)

    paths = sorted(data_dir.glob("chunk_*.parquet"))
    if not paths:
        log.warning("No chunk parquet files in %s", data_dir)
        if show_plot:
            show_entropy_plots(data_dir, limit_id=None)
        return

    provider = create_provider(SourceConfig())
    for path in paths:
        _, start_str, end_str = path.stem.split("_", 2)
        chunk_start = int(start_str)
        chunk_end = int(end_str)

        amplitudes = provider.fetch_seasonal_amplitude_chunk(chunk_start, chunk_end)

        df = pd.read_parquet(path)
        df["mean_seasonal_amplitude"] = df["hylak_id"].map(amplitudes)
        df.to_parquet(path, index=False)
        log.debug("Updated amplitude in %s", path.name)

        provider.upsert_rows("entropy", df.to_dict("records"))

    log.info("Updated amplitude for %d chunk(s)", len(paths))
    if show_plot:
        show_entropy_plots(data_dir, limit_id=None)


def load_entropy_summary(data_dir: Path, limit_id: int | None) -> pd.DataFrame:
    """Load entropy summary from persisted parquet files."""
    parts: list[pd.DataFrame] = []
    for path in sorted(data_dir.glob("chunk_*.parquet")):
        _, start_str, _ = path.stem.split("_", 2)
        chunk_start = int(start_str)
        if limit_id is not None and chunk_start >= limit_id:
            continue
        df = pd.read_parquet(path)
        if limit_id is not None:
            df = df[df["hylak_id"] < limit_id]
        parts.append(df)

    if not parts:
        log.warning("No parquet files found in %s", data_dir)
        return pd.DataFrame()

    summary = pd.concat(parts, ignore_index=True)
    if "mean_seasonal_amplitude" not in summary.columns and "annual_means_std" in summary.columns:
        summary["mean_seasonal_amplitude"] = summary["annual_means_std"]
    return summary


def show_entropy_plots(data_dir: Path, limit_id: int | None) -> None:
    import matplotlib.pyplot as plt

    from lakeviz.entropy import (
        plot_ae_distribution,
        plot_amplitude_histogram,
        plot_amplitude_vs_entropy,
        plot_trend_summary,
        remove_amplitude_outliers,
    )

    summary_df = load_entropy_summary(data_dir, limit_id)
    if summary_df.empty:
        log.warning("No data available for plotting.")
        return

    log.info("Plotting summary for %d lakes.", len(summary_df))

    summary_no_amp_outliers = remove_amplitude_outliers(summary_df)
    write_amplitude_entropy_csv(data_dir, summary_no_amp_outliers)

    plot_dir = data_dir / "plot"
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


def write_amplitude_entropy_csv(data_dir: Path, summary_df: pd.DataFrame) -> None:
    """Write OLS and correlation test results to data/entropy/amplitude_entropy.csv."""
    from scipy.stats import pearsonr, spearmanr

    df = summary_df[["mean_seasonal_amplitude", "ae_overall"]].dropna()
    if len(df) < 3:
        log.warning("Insufficient data for amplitude-entropy statistics.")
        return

    x = np.abs(df["mean_seasonal_amplitude"].to_numpy(dtype=float))
    y = 1.0 - df["ae_overall"].to_numpy(dtype=float)

    r, p_r = pearsonr(x, y)
    rho, p_rho = spearmanr(x, y)
    slope, intercept = np.polyfit(x, y, 1)

    results = pd.DataFrame([
        {
            "n": len(df),
            "pearson_r": r,
            "pearson_p": p_r,
            "spearman_rho": rho,
            "spearman_p": p_rho,
            "ols_slope": slope,
            "ols_intercept": intercept,
            "x_transform": "CV (annual_means_std/mean_area)",
            "y": "1 - ae_overall",
        }
    ])

    out_path = data_dir / "amplitude_entropy.csv"
    results.to_csv(out_path, index=False)
    log.info("Wrote amplitude-entropy statistics to %s", out_path)
