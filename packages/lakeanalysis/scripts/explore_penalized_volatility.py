"""Explore flat-series detection metrics after filtering frozen months.

Key insight: frozen months are filled with the last valid observation, causing
72% of consecutive-month delta=0 BEFORE filtering. After removing frozen months,
delta=0 drops to ~17%, reflecting genuine physical stability (large lakes can
have unchanged area month-to-month).

Computes per-lake metrics from lake_area (frozen months excluded):
  - cv:               std(water_area) / mean(water_area)
  - std_pct_change:   std of month-over-month pct_change (across frozen gaps)
  - n_zero_delta:     count of consecutive non-frozen months with same area
  - penalized_volatility: std_pct_change / sqrt(n_zero_delta) (0 if n_zero_delta=0)
  - n_distinct:       count of distinct water_area values
  - n_obs:            count of non-frozen observations
  - n_frozen:         count of frozen months excluded
  - dominant_ratio:   frequency of most common value / n_obs

NOTE: pct_change is computed between consecutive non-frozen months, which may
span a frozen gap (e.g. Nov -> Jun). This measures inter-seasonal variability
rather than strict month-to-month change.

Produces:
  - 2x3 panel: CV, penalized_volatility, std_pct_change, n_zero_delta,
    dominant_ratio, n_distinct distributions
  - CDF of CV with threshold markers
  - Scatter: CV vs n_frozen (colored by dominant_ratio)

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
    uv run python scripts/explore_penalized_volatility.py
    uv run python scripts/explore_penalized_volatility.py --sample 50000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakesource.provider import create_provider
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Explore flat-series metrics after frozen filtering.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--sample", type=int, default=None, help="Random sample N lakes (None=all).")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures" / "quality")
    return parser.parse_args()


def _compute_metrics_sql(client, sample_ids=None) -> pd.DataFrame:
    sample_filter = ""
    if sample_ids is not None:
        id_list = ",".join(map(str, sample_ids))
        sample_filter = f"AND la.hylak_id IN ({id_list})"

    return client.query_df(f"""
        WITH frozen AS (
            SELECT hylak_id, year_month
            FROM anomaly
            WHERE anomaly_type = 'frozen'
        ),
        non_frozen AS (
            SELECT la.hylak_id, la.year_month, la.water_area
            FROM lake_area la
            JOIN area_quality aq ON aq.hylak_id = la.hylak_id
            LEFT JOIN frozen f ON f.hylak_id = la.hylak_id AND f.year_month = la.year_month
            WHERE f.hylak_id IS NULL {sample_filter}
        ),
        ordered AS (
            SELECT
                hylak_id,
                water_area,
                LAG(water_area) OVER (PARTITION BY hylak_id ORDER BY year_month) AS prev_area,
                ROW_NUMBER() OVER (PARTITION BY hylak_id ORDER BY year_month) AS rn
            FROM non_frozen
        ),
        with_change AS (
            SELECT
                hylak_id,
                water_area,
                prev_area,
                CASE
                    WHEN prev_area IS NULL OR prev_area = 0 THEN NULL
                    ELSE (water_area - prev_area) / prev_area
                END AS pct_change,
                CASE
                    WHEN prev_area IS NULL THEN FALSE
                    ELSE water_area = prev_area
                END AS is_zero_delta
            FROM ordered
            WHERE rn > 1
        ),
        change_metrics AS (
            SELECT
                hylak_id,
                COALESCE(STDDEV(pct_change), 0) AS std_pct_change,
                SUM(CASE WHEN is_zero_delta THEN 1 ELSE 0 END) AS n_zero_delta,
                COUNT(*) AS n_transitions
            FROM with_change
            GROUP BY hylak_id
        ),
        value_metrics AS (
            SELECT
                hylak_id,
                SUM(vc) AS n_obs,
                COUNT(*) AS n_distinct,
                AVG(w_area) AS mean_area,
                STDDEV(w_area) AS std_area,
                MAX(vc) AS dominant_count
            FROM (
                SELECT hylak_id, water_area AS w_area, COUNT(*) AS vc
                FROM non_frozen
                GROUP BY hylak_id, water_area
            )
            GROUP BY hylak_id
        ),
        frozen_counts AS (
            SELECT
                la.hylak_id,
                COUNT(*) AS n_frozen
            FROM lake_area la
            JOIN frozen f ON f.hylak_id = la.hylak_id AND f.year_month = la.year_month
            GROUP BY la.hylak_id
        )
        SELECT
            vm.hylak_id,
            vm.n_obs,
            vm.n_distinct,
            CASE
                WHEN vm.mean_area > 0 THEN vm.std_area / vm.mean_area
                ELSE NULL
            END AS cv,
            vm.dominant_count::double / vm.n_obs AS dominant_ratio,
            cm.std_pct_change,
            cm.n_zero_delta,
            cm.n_transitions,
            CASE
                WHEN cm.n_zero_delta > 0 THEN cm.std_pct_change / SQRT(cm.n_zero_delta)
                ELSE cm.std_pct_change
            END AS penalized_volatility,
            COALESCE(fc.n_frozen, 0) AS n_frozen
        FROM value_metrics vm
        JOIN change_metrics cm ON cm.hylak_id = vm.hylak_id
        LEFT JOIN frozen_counts fc ON fc.hylak_id = vm.hylak_id
        ORDER BY vm.hylak_id
    """)


def _plot_distributions(df: pd.DataFrame, output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    from lakeviz.style.presets import Theme

    Theme.apply()
    output_dir.mkdir(parents=True, exist_ok=True)

    cv = df["cv"].replace([np.inf, -np.inf], np.nan).dropna()
    pv = df["penalized_volatility"].replace([np.inf, -np.inf], np.nan).dropna()
    std = df["std_pct_change"].replace([np.inf, -np.inf], np.nan).dropna()
    nzd = df["n_zero_delta"]
    dr = df["dominant_ratio"].dropna()
    ndist = df["n_distinct"]

    fig, axes = plt.subplots(2, 3, figsize=(20, 10))

    for ax, data, label, color, title in [
        (axes[0, 0], cv, "CV", Theme.PRIMARY_ALT, "CV (去 frozen)"),
        (axes[0, 1], pv, "penalized_volatility", Theme.EXTREME_HIGH, "Penalized Volatility (去 frozen)"),
        (axes[0, 2], std, "std_pct_change", Theme.SECONDARY, "月际变化率标准差 (去 frozen)"),
        (axes[1, 0], nzd, "n_zero_delta", Theme.EXTREME_LOW, "Δarea=0 月份数 (去 frozen)"),
        (axes[1, 1], dr, "dominant_ratio", "#d8b365", "Dominant Ratio (去 frozen)"),
        (axes[1, 2], ndist, "n_distinct", "#5ab4ac", "不同面积值数 (去 frozen)"),
    ]:
        clean = data.replace([np.inf, -np.inf], np.nan).dropna()
        upper = clean.quantile(0.99)
        ax.hist(clean.clip(upper=upper), bins=100, color=color, alpha=0.8, edgecolor="white")
        ax.set_xlabel(label)
        ax.set_ylabel("湖泊数")
        ax.set_title(title)
        ax.grid(alpha=0.2)

    fig.suptitle("Flat-Series 检测指标分布 (排除 frozen 月)", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    path = output_dir / "flat_metrics_distribution.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)


def _plot_cdf(df: pd.DataFrame, output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    from lakeviz.style.presets import Theme

    Theme.apply()
    output_dir.mkdir(parents=True, exist_ok=True)

    cv = df["cv"].replace([np.inf, -np.inf], np.nan).dropna()
    cv_sorted = cv.sort_values().reset_index(drop=True)
    cdf = cv_sorted.rank() / len(cv_sorted)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(cv_sorted.clip(upper=cv_sorted.quantile(0.99)), cdf, color=Theme.PRIMARY_ALT, linewidth=2)

    for threshold in [0.001, 0.005, 0.01, 0.02, 0.05, 0.1]:
        frac = (cv < threshold).mean()
        ax.axvline(threshold, color="grey", linestyle="--", linewidth=0.8, alpha=0.6)
        ax.text(threshold, 0.5, f" {threshold}\n {frac:.1%}", fontsize=8, va="center")

    ax.set_xlabel("CV (去 frozen)")
    ax.set_ylabel("CDF")
    ax.set_title("CV 累积分布函数 (排除 frozen 月)")
    ax.grid(alpha=0.2)

    fig.tight_layout()
    path = output_dir / "cv_cdf.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)


def _plot_scatter(df: pd.DataFrame, output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    from lakeviz.style.presets import Theme

    Theme.apply()
    output_dir.mkdir(parents=True, exist_ok=True)

    cv = df["cv"].replace([np.inf, -np.inf], np.nan)
    nf = df["n_frozen"]
    valid = cv.notna()

    fig, ax = plt.subplots(figsize=(10, 7))

    sc = ax.scatter(
        nf[valid], cv[valid].clip(upper=cv[valid].quantile(0.99)),
        c=df.loc[valid, "dominant_ratio"],
        cmap="YlOrRd", s=2, alpha=0.3, rasterized=True, vmin=0, vmax=1,
    )
    cbar = fig.colorbar(sc, ax=ax, label="dominant_ratio (去 frozen)")
    cbar.ax.tick_params(labelsize=9)

    ax.set_xlabel("frozen 月份数")
    ax.set_ylabel("CV (去 frozen)")
    ax.set_title("CV vs n_frozen (颜色=dominant_ratio)")
    ax.grid(alpha=0.2)

    fig.tight_layout()
    path = output_dir / "cv_vs_nfrozen_scatter.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)


def _print_summary(df: pd.DataFrame) -> None:
    cv = df["cv"].replace([np.inf, -np.inf], np.nan).dropna()
    pv = df["penalized_volatility"].replace([np.inf, -np.inf], np.nan).dropna()
    std = df["std_pct_change"].replace([np.inf, -np.inf], np.nan).dropna()
    nzd = df["n_zero_delta"]
    dr = df["dominant_ratio"].dropna()
    ndist = df["n_distinct"]

    print("\n=== CV (去 frozen) Summary ===")
    print(cv.describe().to_string())
    print(f"\n=== Penalized Volatility (去 frozen) Summary ===")
    print(pv.describe().to_string())
    print(f"\n=== std_pct_change (去 frozen) Summary ===")
    print(std.describe().to_string())
    print(f"\n=== n_zero_delta (去 frozen) Summary ===")
    print(nzd.describe().to_string())
    print(f"\n=== dominant_ratio (去 frozen) Summary ===")
    print(dr.describe().to_string())
    print(f"\n=== n_distinct (去 frozen) Summary ===")
    print(ndist.describe().to_string())
    print(f"\n=== n_frozen Summary ===")
    print(df["n_frozen"].describe().to_string())

    print(f"\n=== CV Threshold Analysis ===")
    for threshold in [0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1]:
        n_below = (cv < threshold).sum()
        pct = n_below / len(cv) * 100
        print(f"  CV < {threshold:>8.4f}: {n_below:>8d} lakes ({pct:>5.1f}%)")

    print(f"\n=== Penalized Volatility Threshold Analysis (去 frozen) ===")
    for threshold in [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5]:
        n_below = (pv < threshold).sum()
        pct = n_below / len(pv) * 100
        print(f"  PV < {threshold:>6.3f}: {n_below:>8d} lakes ({pct:>5.1f}%)")

    print(f"\n=== Dominant Ratio Threshold Analysis (去 frozen) ===")
    for threshold in [0.5, 0.6, 0.7, 0.8, 0.9, 0.95]:
        n_above = (dr >= threshold).sum()
        pct = n_above / len(dr) * 100
        print(f"  DR >= {threshold:>5.2f}: {n_above:>8d} lakes ({pct:>5.1f}%)")


def main() -> None:
    Logger("explore_penalized_volatility")
    args = parse_args()
    load_env()

    source = SourceConfig()
    provider = create_provider(source)
    client = provider._client

    log.info("Computing flat-series metrics (frozen months excluded)...")

    sample_ids = None
    if args.sample is not None:
        log.info("Sampling %d lakes (seed=%d)...", args.sample, args.seed)
        aq_ids = client.query_df("SELECT hylak_id FROM area_quality")
        rng = np.random.default_rng(args.seed)
        sampled = rng.choice(aq_ids["hylak_id"].to_numpy(), size=min(args.sample, len(aq_ids)), replace=False)
        sample_ids = set(sampled.astype(int))

    df = _compute_metrics_sql(client, sample_ids=sample_ids)
    log.info("Computed metrics for %d lakes", len(df))

    if df.empty:
        log.warning("No data returned")
        return

    _print_summary(df)
    _plot_distributions(df, args.output_dir)
    _plot_cdf(df, args.output_dir)
    _plot_scatter(df, args.output_dir)

    cache_path = DATA_DIR / "cache" / "quality" / "flat_metrics_no_frozen.parquet"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Cached metrics to %s", cache_path)


if __name__ == "__main__":
    main()