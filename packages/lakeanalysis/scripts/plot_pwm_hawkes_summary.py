"""Plot summary statistics for PWM-Hawkes diagnostic output.

Usage:
    uv run python scripts/plot_pwm_hawkes_summary.py
    uv run python scripts/plot_pwm_hawkes_summary.py --csv path/to/diagnosis.csv
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.style.presets import Theme
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "pwm_hawkes" / "diag"
PLOT_DIR = Path(__file__).resolve().parent.parent / "data" / "pwm_hawkes" / "figures"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PWM-Hawkes summary plots from diagnosis.csv."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DATA_DIR / "diagnosis.csv",
        help="Path to diagnosis.csv",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=PLOT_DIR,
    )
    return parser.parse_args()


def _save(fig: plt.Figure, name: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / name
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return path


def _plot_qc_pie(df: pd.DataFrame, output_dir: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    for ax, col, label in [
        (axes[0], "qc_pass", "QC"),
        (axes[1], "converged", "Fit Converged"),
    ]:
        if col in df.columns:
            series = df[col].fillna(False).astype(bool)
            counts = series.value_counts(dropna=False)
            labels = [f"Pass ({int(counts.get(True, 0))})",
                      f"Fail ({int(counts.get(False, 0))})"]
            vals = [int(counts.get(True, 0)), int(counts.get(False, 0))]
            if vals[0] + vals[1] == 0:
                vals = [1, 1]
                labels = ["N/A", "N/A"]
            ax.pie(vals, labels=labels, autopct="%1.1f%%",
                   colors=["#4CAF50", "#F44336"])
            ax.set_title(f"{label} ({df[col].notna().sum()})")
    fig.suptitle("PWM-Hawkes Quality Control", fontsize=14, fontweight="bold")
    _save(fig, "qc_pie.png", output_dir)


def _plot_event_histogram(df: pd.DataFrame, output_dir: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    if "n_events" in df.columns:
        vals = df["n_events"].dropna()
        if len(vals) > 0:
            axes[0].hist(vals, bins=30, color="steelblue", edgecolor="white", alpha=0.85)
            axes[0].set_xlabel("Declustered Events")
            axes[0].set_ylabel("Lake Count")
            axes[0].set_title(f"Events per Lake (n={len(vals)}, median={vals.median():.0f})")
    if "n_dry_events" in df.columns and "n_wet_events" in df.columns:
        dry = df["n_dry_events"].dropna()
        wet = df["n_wet_events"].dropna()
        if len(dry) > 0 and len(wet) > 0:
            axes[1].hist(dry, bins=30, color="#D2691E", edgecolor="white",
                         alpha=0.6, label="Dry")
            axes[1].hist(wet, bins=30, color="#4682B4", edgecolor="white",
                         alpha=0.6, label="Wet")
            axes[1].set_xlabel("Events")
            axes[1].set_ylabel("Lake Count")
            axes[1].set_title("Dry vs Wet Events")
            axes[1].legend()
    fig.suptitle("PWM-Hawkes Event Distribution", fontsize=14, fontweight="bold")
    _save(fig, "event_histogram.png", output_dir)


def _plot_mu_scatter(df: pd.DataFrame, output_dir: Path) -> None:
    if "mu_d" not in df.columns or "mu_w" not in df.columns:
        return
    vals = df[["mu_d", "mu_w"]].dropna()
    if len(vals) < 3:
        return
    fig, ax = plt.subplots(figsize=(6, 5.5))
    ax.scatter(vals["mu_d"], vals["mu_w"], alpha=0.5, s=20, color="steelblue")
    max_val = max(vals["mu_d"].max(), vals["mu_w"].max()) * 1.05
    ax.plot([0, max_val], [0, max_val], "k--", alpha=0.3)
    ax.set_xlabel("mu_D (baseline dry intensity)")
    ax.set_ylabel("mu_W (baseline wet intensity)")
    ax.set_title(f"Background Intensity (n={len(vals)})")
    _save(fig, "mu_scatter.png", output_dir)


def _plot_lrt_histogram(df: pd.DataFrame, output_dir: Path) -> None:
    if "lrt_p_d_to_w" not in df.columns:
        return
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    for ax, col, label in [
        (axes[0], "lrt_p_d_to_w", "D -> W"),
        (axes[1], "lrt_p_w_to_d", "W -> D"),
    ]:
        vals = df[col].dropna()
        if len(vals) > 0:
            sig = (vals < 0.05).sum()
            ax.hist(vals, bins=30, color="steelblue", edgecolor="white", alpha=0.85)
            ax.axvline(0.05, color="red", linestyle="--", alpha=0.6, label="p=0.05")
            ax.set_xlabel("p-value")
            ax.set_ylabel("Count")
            ax.set_title(f"{label} (sig={sig}/{len(vals)})")
            ax.legend()
    fig.suptitle("LRT p-value Distribution", fontsize=14, fontweight="bold")
    _save(fig, "lrt_histogram.png", output_dir)


def _plot_spectral_radius(df: pd.DataFrame, output_dir: Path) -> None:
    if "spectral_radius" not in df.columns:
        return
    vals = df["spectral_radius"].dropna()
    if len(vals) == 0:
        return
    fig, ax = plt.subplots(figsize=(6, 4.5))
    ax.hist(vals, bins=30, color="steelblue", edgecolor="white", alpha=0.85)
    ax.axvline(1.0, color="red", linestyle="--", alpha=0.6, label="stability=1")
    stable = (vals <= 1).sum()
    ax.set_xlabel("Spectral Radius")
    ax.set_ylabel("Lake Count")
    ax.set_title(f"Spectral Radius (stable={stable}/{len(vals)})")
    ax.legend()
    _save(fig, "spectral_radius.png", output_dir)


def _plot_alpha_heatmap(df: pd.DataFrame, output_dir: Path) -> None:
    alphas = ["alpha_dd", "alpha_dw", "alpha_wd", "alpha_ww"]
    present = [c for c in alphas if c in df.columns]
    if len(present) < 4:
        return
    sub = df[present].dropna()
    if len(sub) < 3:
        return
    median_vals = [sub[c].median() for c in alphas]
    matrix = np.array([[median_vals[0], median_vals[1]],
                        [median_vals[2], median_vals[3]]])
    fig, ax = plt.subplots(figsize=(4.5, 4))
    im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["D", "W"])
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["D", "W"])
    ax.set_xlabel("Source Type")
    ax.set_ylabel("Target Type")
    ax.set_title("Median Alpha (excitation)")
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{matrix[i, j]:.4f}", ha="center", va="center",
                    fontsize=12, fontweight="bold",
                    color="white" if matrix[i, j] > np.median(matrix) else "black")
    fig.colorbar(im, ax=ax)
    _save(fig, "alpha_heatmap.png", output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_pwm_hawkes_summary")

    if not args.csv.exists():
        log.error("diagnosis.csv not found at %s. Run run_pwm_hawkes_diag.py first.", args.csv)
        return

    df = pd.read_csv(args.csv)
    log.info("Loaded %d rows from %s", len(df), args.csv)

    Theme.apply()

    _plot_qc_pie(df, args.output_dir)
    _plot_event_histogram(df, args.output_dir)
    _plot_mu_scatter(df, args.output_dir)
    _plot_lrt_histogram(df, args.output_dir)
    _plot_spectral_radius(df, args.output_dir)
    _plot_alpha_heatmap(df, args.output_dir)

    saved = sorted(args.output_dir.glob("*.png"))
    log.info("Saved %d summary plots:", len(saved))
    for p in saved:
        log.info("  %s", p)


if __name__ == "__main__":
    main()
