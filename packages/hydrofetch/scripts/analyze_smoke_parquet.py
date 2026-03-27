"""Analyze smoke-test Parquet outputs and produce matplotlib figures.

Reads all ``*_sampled.parquet`` files from the input directory, computes
per-lake and per-date statistics, and writes a set of plots to the output
directory.

Usage::

    uv run --package hydrofetch python packages/hydrofetch/scripts/analyze_smoke_parquet.py
    uv run --package hydrofetch python packages/hydrofetch/scripts/analyze_smoke_parquet.py \\
        --input data/hydrofetch_smoke_out \\
        --output data/hydrofetch_smoke_analysis
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.rcParams["figure.dpi"] = 120
matplotlib.rcParams["axes.grid"] = True
matplotlib.rcParams["grid.alpha"] = 0.3

BAND_UNITS: dict[str, str] = {
    "temperature_2m": "K",
    "dewpoint_temperature_2m": "K",
    "total_precipitation_sum": "m day⁻¹",
    "potential_evaporation_sum": "m day⁻¹",
}


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def load_parquets(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*_sampled.parquet"))
    if not files:
        print(f"Error: no *_sampled.parquet files found in {input_dir}", file=sys.stderr)
        sys.exit(1)
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    return df


def band_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in ("hylak_id", "date")]


def display_name(col: str) -> str:
    unit = BAND_UNITS.get(col, "")
    label = col.replace("_", " ")
    return f"{label} [{unit}]" if unit else label


# ---------------------------------------------------------------------------
# Console statistics
# ---------------------------------------------------------------------------


def print_statistics(df: pd.DataFrame, bands: list[str]) -> None:
    print("=" * 64)
    print("Global summary statistics")
    print("=" * 64)
    print(df[["hylak_id", "date"] + bands].describe().to_string())

    print()
    print("=" * 64)
    print("Coverage per lake (non-NaN rows out of total dates)")
    print("=" * 64)
    n_dates = df["date"].nunique()
    for lid, grp in df.groupby("hylak_id"):
        valid = grp[bands].notna().all(axis=1).sum()
        print(f"  hylak_id {lid:>8}:  {valid}/{n_dates} dates have full coverage")

    print()
    print("=" * 64)
    print("Per-lake mean over all dates (bands)")
    print("=" * 64)
    print(df.groupby("hylak_id")[bands].mean().to_string())

    print()
    print("=" * 64)
    print("Per-date mean over all lakes (bands)")
    print("=" * 64)
    print(df.groupby("date")[bands].mean().to_string())


# ---------------------------------------------------------------------------
# Plot 1 – time series of daily mean per band
# ---------------------------------------------------------------------------


def plot_timeseries_mean(df: pd.DataFrame, bands: list[str], out_dir: Path) -> None:
    date_mean = df.groupby("date")[bands].mean()
    n = len(bands)
    fig, axes = plt.subplots(n, 1, figsize=(10, 3 * n), sharex=True)
    if n == 1:
        axes = [axes]

    for ax, col in zip(axes, bands):
        ax.plot(date_mean.index, date_mean[col], "o-", lw=1.5, ms=4)
        ax.set_ylabel(display_name(col), fontsize=9)

    axes[0].set_title("Daily mean across all lakes (valid pixels only)")
    axes[-1].set_xlabel("Date")
    fig.tight_layout()
    dest = out_dir / "timeseries_daily_mean.png"
    fig.savefig(dest)
    plt.close(fig)
    print(f"  → {dest}")


# ---------------------------------------------------------------------------
# Plot 2 – box plot distribution per band
# ---------------------------------------------------------------------------


def plot_boxplot_bands(df: pd.DataFrame, bands: list[str], out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    data = [df[col].dropna().values for col in bands]
    bp = ax.boxplot(data, tick_labels=[display_name(c) for c in bands], patch_artist=True)
    for patch in bp["boxes"]:
        patch.set_facecolor("#a8d8ea")
    ax.set_title("Value distribution per band (all lakes × dates)")
    ax.tick_params(axis="x", labelsize=8)
    fig.tight_layout()
    dest = out_dir / "boxplot_bands.png"
    fig.savefig(dest)
    plt.close(fig)
    print(f"  → {dest}")


# ---------------------------------------------------------------------------
# Plot 3 – heatmap: lakes × dates, temperature
# ---------------------------------------------------------------------------


def plot_heatmap_temperature(df: pd.DataFrame, out_dir: Path) -> None:
    col = "temperature_2m"
    if col not in df.columns:
        return

    # Convert to Celsius for readability.
    df = df.copy()
    df["temp_C"] = df[col] - 273.15

    pivot = df.pivot_table(index="hylak_id", columns="date", values="temp_C", aggfunc="mean")
    fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns) * 0.9), max(4, len(pivot) * 0.6)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdBu_r")

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(
        [d.strftime("%m-%d") for d in pivot.columns], rotation=45, ha="right", fontsize=8
    )
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=8)
    ax.set_xlabel("Date")
    ax.set_ylabel("hylak_id")
    ax.set_title("2 m air temperature (°C): lake × date")
    plt.colorbar(im, ax=ax, label="°C", shrink=0.8)
    fig.tight_layout()
    dest = out_dir / "heatmap_temperature.png"
    fig.savefig(dest)
    plt.close(fig)
    print(f"  → {dest}")


# ---------------------------------------------------------------------------
# Plot 4 – per-lake time series (temperature in °C and precipitation)
# ---------------------------------------------------------------------------


def plot_per_lake_timeseries(df: pd.DataFrame, out_dir: Path) -> None:
    df = df.copy()
    df["temp_C"] = df["temperature_2m"] - 273.15 if "temperature_2m" in df.columns else float("nan")

    lakes = sorted(df["hylak_id"].unique())
    n = len(lakes)
    n_cols = min(5, n)
    n_rows = (n + n_cols - 1) // n_cols

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(3 * n_cols, 3 * n_rows),
        sharex=True,
        sharey=False,
    )
    axes_flat = axes.flatten() if n > 1 else [axes]

    for i, lid in enumerate(lakes):
        ax = axes_flat[i]
        grp = df[df["hylak_id"] == lid].sort_values("date")
        if grp["temp_C"].notna().any():
            ax.plot(grp["date"], grp["temp_C"], "b-o", ms=3, lw=1, label="temp °C")
        else:
            ax.text(0.5, 0.5, "no coverage", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(f"lake {lid}", fontsize=9)
        ax.tick_params(axis="x", labelrotation=45, labelsize=7)
        ax.tick_params(axis="y", labelsize=7)

    for j in range(i + 1, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle("2 m temperature (°C) per lake", y=1.01)
    fig.tight_layout()
    dest = out_dir / "timeseries_per_lake.png"
    fig.savefig(dest, bbox_inches="tight")
    plt.close(fig)
    print(f"  → {dest}")


# ---------------------------------------------------------------------------
# Plot 5 – NaN coverage overview
# ---------------------------------------------------------------------------


def plot_coverage(df: pd.DataFrame, bands: list[str], out_dir: Path) -> None:
    pivot = df.pivot_table(
        index="hylak_id", columns="date", values=bands[0], aggfunc="count"
    ).notna()

    fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns) * 0.9), max(3, len(pivot) * 0.5)))
    im = ax.imshow(pivot.values.astype(float), aspect="auto", cmap="RdYlGn", vmin=0, vmax=1)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(
        [d.strftime("%m-%d") for d in pivot.columns], rotation=45, ha="right", fontsize=8
    )
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=8)
    ax.set_xlabel("Date")
    ax.set_ylabel("hylak_id")
    ax.set_title(f"Coverage (green = valid, red = NaN) — {bands[0]}")
    plt.colorbar(im, ax=ax, label="valid", shrink=0.8)
    fig.tight_layout()
    dest = out_dir / "coverage_map.png"
    fig.savefig(dest)
    plt.close(fig)
    print(f"  → {dest}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    default_input = repo_root / "data" / "hydrofetch_smoke_out"
    default_output = repo_root / "data" / "hydrofetch_smoke_analysis"

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=default_input,
        type=Path,
        metavar="DIR",
        help=f"Directory with *_sampled.parquet files (default: {default_input})",
    )
    parser.add_argument(
        "--output",
        default=default_output,
        type=Path,
        metavar="DIR",
        help=f"Directory for output plots (default: {default_output})",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    input_dir: Path = args.input
    out_dir: Path = args.output

    if not input_dir.is_dir():
        print(f"Error: input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading Parquet files from: {input_dir}")
    df = load_parquets(input_dir)
    bands = band_cols(df)
    n_files = df["date"].nunique()
    n_lakes = df["hylak_id"].nunique()

    print(f"Loaded {n_files} dates × {n_lakes} lakes = {len(df)} rows, bands: {bands}")
    print()

    print_statistics(df, bands)

    print()
    print(f"Writing plots to: {out_dir}")
    plot_timeseries_mean(df, bands, out_dir)
    plot_boxplot_bands(df, bands, out_dir)
    plot_heatmap_temperature(df, out_dir)
    plot_per_lake_timeseries(df, out_dir)
    plot_coverage(df, bands, out_dir)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
