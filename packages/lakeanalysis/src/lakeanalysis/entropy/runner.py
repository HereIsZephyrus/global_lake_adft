"""Visualisation and display helpers for Apportionment Entropy results.

Separated from ``service.py`` (pipeline orchestration) and
``compute.py`` (pure AE math).
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .service import load_entropy_summary

log = logging.getLogger(__name__)


def show_entropy_plots(data_dir: Path, limit_id: int | None) -> None:
    """Generate and save AE distribution, trend, and amplitude-visualisation plots."""
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
