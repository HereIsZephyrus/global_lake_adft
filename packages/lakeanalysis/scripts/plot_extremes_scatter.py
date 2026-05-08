"""Scatter plot of extreme event counts per lake on a global Robinson projection.

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \\
        uv run python packages/lakeanalysis/scripts/plot_extremes_scatter.py

    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet_gt10 \\
        uv run python packages/lakeanalysis/scripts/plot_extremes_scatter.py --output-dir data/figures/gt10
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from lakesource.env import load_env
from lakeviz.style.base import AxKind, stamp_ax
from lakeviz.style.presets import Theme

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scatter-plot extreme-event lake counts on a global map."
    )
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures")
    parser.add_argument("--parquet-dir", type=Path, default=DATA_DIR / "parquet")
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--point-size", type=float, default=0.8)
    parser.add_argument("--alpha", type=float, default=0.4)
    parser.add_argument("--high-color", type=str, default="#e74c3c")
    parser.add_argument("--low-color", type=str, default="#3498db")
    parser.add_argument("--mixed-color", type=str, default="#8e44ad")
    return parser.parse_args()


def _fetch_event_counts(parquet_dir: Path) -> duckdb.DuckDBPyRelation:
    con = duckdb.connect(":memory:")

    extremes_glob = str(parquet_dir / "quantile_extremes.parquet")
    lake_info_glob = _resolve_parquet_path(parquet_dir, "lake_info")

    return con.execute(f"""
        WITH counts AS (
            SELECT
                hylak_id,
                SUM(CASE WHEN event_type = 'high' THEN 1 ELSE 0 END) AS high_count,
                SUM(CASE WHEN event_type = 'low'  THEN 1 ELSE 0 END) AS low_count
            FROM read_parquet('{extremes_glob}')
            GROUP BY hylak_id
        )
        SELECT li.hylak_id, li.lat, li.lon,
               COALESCE(c.high_count, 0) AS high_count,
               COALESCE(c.low_count,  0) AS low_count,
               li.lake_area
        FROM read_parquet('{lake_info_glob}') li
        LEFT JOIN counts c ON c.hylak_id = li.hylak_id
        ORDER BY li.hylak_id
    """).fetchdf()


def _resolve_parquet_path(base: Path, table: str) -> str:
    d = base / table
    if d.is_dir():
        return str(d / "*.parquet")
    single = base / f"{table}.parquet"
    if single.exists():
        return str(single)
    raise FileNotFoundError(f"Neither {d}/ nor {single} found for table {table}")


def _classify(row):
    if row.high_count > 0 and row.low_count > 0:
        return "both"
    if row.high_count > 0:
        return "high"
    if row.low_count > 0:
        return "low"
    return "none"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    load_env()
    Theme.apply()

    out = args.output_dir / "quantile"
    out.mkdir(parents=True, exist_ok=True)

    log.info("Loading extreme event counts from %s ...", args.parquet_dir)
    df = _fetch_event_counts(args.parquet_dir)
    df["category"] = df.apply(_classify, axis=1)

    log.info(
        "Lakes: total=%d  high=%d  low=%d  both=%d  none=%d",
        len(df),
        (df["category"] == "high").sum(),
        (df["category"] == "low").sum(),
        (df["category"] == "both").sum(),
        (df["category"] == "none").sum(),
    )

    color_map = {"high": args.high_color, "low": args.low_color, "both": args.mixed_color}

    fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={"projection": ccrs.Robinson()})
    stamp_ax(ax, AxKind.GEOGRAPHIC)

    ax.add_feature(cfeature.OCEAN, facecolor="#e8f4f8", edgecolor="none")
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", edgecolor="none")
    ax.add_feature(cfeature.LAKES, facecolor="#d4e6f1", edgecolor="#666666", linewidth=0.2)
    ax.set_global()

    for cat in ["both", "high", "low"]:
        subset = df[df["category"] == cat]
        if len(subset) == 0:
            continue
        ax.scatter(
            subset["lon"], subset["lat"],
            s=args.point_size,
            c=color_map[cat],
            alpha=args.alpha,
            edgecolors="none",
            transform=ccrs.PlateCarree(),
            label=f"{cat} ({len(subset)})",
        )

    ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")

    leg = ax.legend(loc="lower left", markerscale=3, fontsize=8, frameon=True)
    leg.get_frame().set_alpha(0.8)

    output_path = out / "extremes_scatter.png"
    fig.savefig(output_path, dpi=args.dpi, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", output_path)


if __name__ == "__main__":
    main()
