"""Inspect shift-filter results for a small set of lakes and save plots.

Usage:
    uv run python scripts/inspect_shift_lakes.py --hylak-id 170137 170009

The script prints the shift-filter classification first, then saves a plot for
each lake showing the monthly area series and the detected breakpoint.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.env import load_env
from lakesource.postgres import fetch_frozen_year_months_by_ids, fetch_lake_area_by_ids, series_db
from lakeanalysis.quality import ShiftConfig, filter_frozen_rows
from lakeanalysis.quality.filters.shift import ShiftFilter
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect shift-filter results for selected lakes.")
    parser.add_argument("--hylak-id", type=int, nargs="+", default=[170137, 170009])
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures" / "quality")
    parser.add_argument("--min-segment-months", type=int, default=24)
    parser.add_argument("--smooth-window", type=int, default=12)
    parser.add_argument("--p-value-thresh", type=float, default=0.05)
    return parser.parse_args()


def _plot_lake(ax, df: pd.DataFrame, detail: dict[str, object], hylak_id: int) -> None:
    dates = pd.to_datetime({"year": df["year"], "month": df["month"], "day": 1})
    area_km2 = df["water_area"].to_numpy(dtype=float) / 1e6
    ax.plot(dates, area_km2, color="#2b8cbe", linewidth=1.0, label="water_area")

    break_idx = detail.get("udmax_break_index")
    if isinstance(break_idx, int) and 0 < break_idx < len(df):
        break_date = dates.iloc[break_idx]
        ax.axvline(break_date, color="#d95f0e", linestyle="--", linewidth=1.2, label="break")
        pre_mean = detail.get("pre_break_mean")
        post_mean = detail.get("post_break_mean")
        if isinstance(pre_mean, (int, float)):
            ax.axhline(float(pre_mean) / 1e6, color="#636363", linestyle=":", linewidth=1.0, alpha=0.8)
        if isinstance(post_mean, (int, float)):
            ax.axhline(float(post_mean) / 1e6, color="#969696", linestyle=":", linewidth=1.0, alpha=0.8)

    ax.set_title(
        f"hylak_id={hylak_id}  label={detail.get('label')}  "
        f"UDmax={detail.get('udmax', 0):.2f}  WDmax={detail.get('wdmax', 0):.2f}",
        fontsize=10,
    )
    ax.set_ylabel("area (km²)")
    ax.grid(alpha=0.2)
    ax.legend(loc="upper right", fontsize=8)


def main() -> None:
    Logger("inspect_shift_lakes")
    args = parse_args()

    load_env()

    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme

    Theme.apply()

    shift_filter = ShiftFilter(
        ShiftConfig(
            min_segment_months=args.min_segment_months,
            smooth_window=args.smooth_window,
            p_value_thresh=args.p_value_thresh,
        )
    )

    with series_db.connection_context() as conn:
        lake_frames = fetch_lake_area_by_ids(conn, args.hylak_id)
        frozen_map = fetch_frozen_year_months_by_ids(conn, args.hylak_id)

    print("Shift filter results")
    print("=" * 72)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    for hylak_id in args.hylak_id:
        df = lake_frames.get(hylak_id)
        if df is None or df.empty:
            print(f"hylak_id={hylak_id}: no data")
            continue

        frozen_ym = frozen_map.get(hylak_id, set())
        df_no_frozen = filter_frozen_rows(df, frozen_ym)
        ctx = type("_Ctx", (), {})()
        ctx.df = df
        ctx.df_no_frozen = df_no_frozen
        ctx.rs_area_median = float(df_no_frozen["water_area"].median())
        ctx.rs_area_mean = float(df_no_frozen["water_area"].mean())
        ctx.rs_area_quantile = float(df_no_frozen["water_area"].quantile(0.8))
        ctx.atlas_area = 0.0

        result = shift_filter.classify(ctx)
        detail = result.detail
        print(f"hylak_id={hylak_id}")
        print(f"  is_anomaly={result.is_anomaly}")
        print(f"  label={detail.get('label')}")
        print(f"  seasonality_dominance_ratio={detail.get('seasonality_dominance_ratio')}")
        print(f"  used_deseasoned={detail.get('used_deseasoned')}")
        print(f"  udmax={detail.get('udmax')}  p={detail.get('udmax_p_value')}  break={detail.get('udmax_break_index')}")
        print(f"  wdmax={detail.get('wdmax')}  p={detail.get('wdmax_p_value')}  break={detail.get('wdmax_break_index')}")

        fig, ax = plt.subplots(figsize=(12, 4))
        _plot_lake(ax, df_no_frozen, detail, hylak_id)
        fig.tight_layout()
        path = args.output_dir / f"shift_lake_{hylak_id}.png"
        fig.savefig(path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  saved_plot={path}")
        print()


if __name__ == "__main__":
    main()
