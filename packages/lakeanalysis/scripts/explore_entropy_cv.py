"""Explore H*CV metric for flat-series detection (replacing penalized_volatility).

Full-scale computation: iterates all lakes via ChunkedLakeProcessor,
computes per-lake metrics in Python (frozen months excluded), writes results
to area_entropy_cv table, then generates exploration plots.

Metrics (frozen months excluded):
  - H:     discrete Shannon entropy of water_area values, H = -sum(p_i * log2(p_i))
  - CV:    std(water_area) / mean(water_area)
  - H*CV:  entropy-weighted coefficient of variation
  - n_distinct, n_obs, n_frozen, dominant_ratio

Produces:
  - DB table: area_entropy_cv
  - CDF of H*CV with threshold markers
  - 2x3 panel: H, CV, H*CV, n_distinct, dominant_ratio, n_frozen distributions
  - Scatter: H vs CV (colored by H*CV)

Usage:
    uv run python scripts/explore_entropy_cv.py
    uv run python scripts/explore_entropy_cv.py --limit-id 50000
    uv run python scripts/explore_entropy_cv.py --skip-compute
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.env import load_env
from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider
from lakesource.postgres import series_db
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"

_FETCH_CHUNK_SQL = """
SELECT hylak_id, water_area,
       (EXTRACT(YEAR FROM year_month)::int * 100
        + EXTRACT(MONTH FROM year_month)::int) AS ym
FROM lake_area
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
"""

_FETCH_FROZEN_CHUNK_SQL = """
SELECT hylak_id,
       (EXTRACT(YEAR FROM year_month)::int * 100
        + EXTRACT(MONTH FROM year_month)::int) AS ym
FROM anomaly
WHERE anomaly_type = 'frozen'
  AND hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
"""


def _compute_chunk(chunk_start: int, chunk_end: int) -> list[dict]:
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SET work_mem = '256MB'")
            cur.execute(_FETCH_CHUNK_SQL, {
                "chunk_start": chunk_start, "chunk_end": chunk_end,
            })
            area_rows = cur.fetchall()
            cur.execute(_FETCH_FROZEN_CHUNK_SQL, {
                "chunk_start": chunk_start, "chunk_end": chunk_end,
            })
            frozen_rows = cur.fetchall()

    if not area_rows:
        return []

    frozen_set = set()
    frozen_counter = Counter()
    for hid, ym in frozen_rows:
        hid_i, ym_i = int(hid), int(ym)
        frozen_set.add((hid_i, ym_i))
        frozen_counter[hid_i] += 1

    df = pd.DataFrame(area_rows, columns=["hylak_id", "water_area", "ym"])
    df["hylak_id"] = df["hylak_id"].astype(int)
    df["ym"] = df["ym"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    mask = np.array([
        (hid, ym) not in frozen_set
        for hid, ym in zip(df["hylak_id"].values, df["ym"].values)
    ])
    df_nf = df.loc[mask, ["hylak_id", "water_area"]]

    results = []
    for hid, grp in df_nf.groupby("hylak_id"):
        areas = grp["water_area"]
        n_obs = len(areas)
        if n_obs == 0:
            continue
        vc = areas.value_counts()
        n_distinct = len(vc)
        p = vc.values / n_obs
        H = float(-np.sum(p * np.log2(p)))
        mean_a = areas.mean()
        std_a = areas.std()
        cv = float(std_a / mean_a) if mean_a > 0 else None
        h_cv = float(H * cv) if cv is not None else None
        dominant_ratio = float(vc.values[0] / n_obs)
        n_frozen = frozen_counter.get(int(hid), 0)
        results.append({
            "hylak_id": int(hid),
            "n_obs": n_obs,
            "n_distinct": n_distinct,
            "dominant_ratio": dominant_ratio,
            "cv": cv,
            "H": H,
            "h_cv": h_cv,
            "n_frozen": n_frozen,
        })

    return results


def _plot_cdf(df: pd.DataFrame, output_dir: Path) -> None:
    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme
    Theme.apply()

    h_cv = df["h_cv"].replace([np.inf, -np.inf], np.nan).dropna()
    h_cv_sorted = h_cv.sort_values().reset_index(drop=True)
    cdf = h_cv_sorted.rank() / len(h_cv_sorted)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(h_cv_sorted.clip(upper=h_cv_sorted.quantile(0.99)), cdf, color="steelblue", linewidth=2)

    for thresh in [0.005, 0.01, 0.02, 0.03, 0.05, 0.1]:
        frac = (h_cv <= thresh).mean()
        ax.axvline(thresh, color="grey", linestyle="--", linewidth=0.8, alpha=0.6)
        ax.text(thresh, 0.5, f" {thresh}\n {frac:.1%}", fontsize=8, va="center")

    ax.set_xlabel("H×CV")
    ax.set_ylabel("CDF")
    ax.set_title("H×CV 累积分布函数 (排除 frozen 月)")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    path = output_dir / "h_cv_cdf.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)


def _plot_distributions(df: pd.DataFrame, output_dir: Path) -> None:
    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme
    Theme.apply()

    panels = [
        ("h", "H", Theme.PRIMARY_ALT, "信息熵 H"),
        ("cv", "CV", Theme.EXTREME_HIGH, "变异系数 CV"),
        ("h_cv", "H×CV", Theme.SECONDARY, "H×CV"),
        ("n_distinct", "n_distinct", "#5ab4ac", "不同面积值数"),
        ("dominant_ratio", "dominant_ratio", "#d8b365", "Dominant Ratio"),
        ("n_frozen", "n_frozen", Theme.EXTREME_LOW, "frozen 月份数"),
    ]

    fig, axes = plt.subplots(2, 3, figsize=(20, 10))
    for ax, (col, label, color, title) in zip(axes.flat, panels):
        clean = df[col].replace([np.inf, -np.inf], np.nan).dropna()
        upper = clean.quantile(0.99)
        ax.hist(clean.clip(upper=upper), bins=100, color=color, alpha=0.8, edgecolor="white")
        ax.set_xlabel(label)
        ax.set_ylabel("湖泊数")
        ax.set_title(title)
        ax.grid(alpha=0.2)

    fig.suptitle("H×CV 检测指标分布 (排除 frozen 月)", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    path = output_dir / "h_cv_distributions.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)


def _plot_scatter_h_cv(df: pd.DataFrame, output_dir: Path) -> None:
    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme
    Theme.apply()

    h = df["h"].replace([np.inf, -np.inf], np.nan)
    cv = df["cv"].replace([np.inf, -np.inf], np.nan)
    valid = h.notna() & cv.notna()

    fig, ax = plt.subplots(figsize=(10, 7))
    sc = ax.scatter(
        h[valid], cv[valid].clip(upper=cv[valid].quantile(0.99)),
        c=df.loc[valid, "h_cv"].clip(upper=df["h_cv"].quantile(0.95)),
        cmap="YlOrRd", s=2, alpha=0.3, rasterized=True,
    )
    cbar = fig.colorbar(sc, ax=ax, label="H×CV")
    cbar.ax.tick_params(labelsize=9)

    ax.set_xlabel("H (信息熵)")
    ax.set_ylabel("CV")
    ax.set_title("H vs CV (颜色=H×CV)")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    path = output_dir / "h_vs_cv_scatter.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)


def _print_summary(df: pd.DataFrame) -> None:
    for col in ["h", "cv", "h_cv", "n_distinct", "dominant_ratio", "n_frozen"]:
        clean = df[col].replace([np.inf, -np.inf], np.nan).dropna()
        print(f"\n=== {col} Summary ===")
        print(clean.describe().to_string())

    print("\n=== H×CV Threshold Analysis ===")
    h_cv = df["h_cv"].replace([np.inf, -np.inf], np.nan).dropna()
    for thresh in [0.001, 0.002, 0.005, 0.01, 0.02, 0.03, 0.05, 0.1]:
        n_below = (h_cv <= thresh).sum()
        pct = n_below / len(h_cv) * 100
        print(f"  H×CV <= {thresh:>8.4f}: {n_below:>8d} lakes ({pct:>5.1f}%)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explore H×CV metric for flat-series detection.")
    parser.add_argument(
        "--limit-id", type=int, default=None, metavar="N",
        help="Only process rows with hylak_id < N (for testing).",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=10_000, metavar="N",
        help="Number of hylak_id values per chunk (default: 10000).",
    )
    parser.add_argument(
        "--skip-compute", action="store_true",
        help="Skip computation, only plot from existing area_entropy_cv table.",
    )
    return parser.parse_args()


def main() -> None:
    Logger("explore_entropy_cv")
    args = parse_args()
    load_env()

    source = SourceConfig()
    provider = create_provider(source)

    if not args.skip_compute:
        provider.ensure_schema("area_entropy_cv")

        with series_db.connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(hylak_id) FROM lake_info")
                max_hylak_id = int(cur.fetchone()[0])
                cur.execute("SELECT MAX(hylak_id) FROM area_entropy_cv")
                row = cur.fetchone()
                max_done_id = int(row[0]) if row[0] is not None else -1

        if args.limit_id is not None:
            max_hylak_id = min(max_hylak_id, args.limit_id - 1)

        chunk_size = args.chunk_size
        all_chunks = list(range(0, max_hylak_id + 1, chunk_size))
        total = len(all_chunks)
        skipped = 0

        log.info(
            "Starting chunked run: %d chunk(s), chunk_size=%d, max_done_id=%s",
            total, chunk_size, max_done_id,
        )

        for idx, chunk_start in enumerate(all_chunks, 1):
            chunk_end = chunk_start + chunk_size
            if chunk_end <= max_done_id + 1:
                skipped += 1
                continue

            log.info("[%d/%d] chunk %d-%d: processing...", idx, total, chunk_start, chunk_end - 1)
            rows = _compute_chunk(chunk_start, chunk_end)
            if rows:
                provider.persist({"area_entropy_cv": rows})
            log.info(
                "[%d/%d] chunk %d-%d: done (%d item(s))",
                idx, total, chunk_start, chunk_end - 1, len(rows),
            )

        processed = total - skipped
        log.info("Done. %d chunk(s) processed, %d skipped.", processed, skipped)

    log.info("Reading area_entropy_cv for plotting...")
    with series_db.connection_context() as conn:
        df = pd.read_sql("SELECT * FROM area_entropy_cv", conn)
    log.info("Loaded %d rows from area_entropy_cv", len(df))

    if df.empty:
        log.warning("No data in area_entropy_cv")
        return

    output_dir = DATA_DIR / "figures" / "quality"
    output_dir.mkdir(parents=True, exist_ok=True)

    _print_summary(df)
    _plot_cdf(df, output_dir)
    _plot_distributions(df, output_dir)
    _plot_scatter_h_cv(df, output_dir)


if __name__ == "__main__":
    main()
