"""Plot time series for offline shift-filter candidates.

Reads candidate ids from a CSV or Parquet file, fetches the corresponding lake
series, reruns the shift filter for annotations, and saves one PNG per lake.
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

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot offline shift-filter candidates.")
    parser.add_argument("--candidate-file", type=Path, required=True)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-segment-months", type=int, default=24)
    parser.add_argument("--smooth-window", type=int, default=12)
    parser.add_argument("--p-value-thresh", type=float, default=0.05)
    return parser.parse_args()


def _read_candidates(path: Path, top_n: int) -> list[int]:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    if "hylak_id" not in df.columns:
        raise ValueError(f"candidate file missing hylak_id column: {path}")
    ids = [int(v) for v in df["hylak_id"].dropna().tolist()]
    return ids[:top_n]


def _plot_lake(ax, df: pd.DataFrame, detail: dict[str, object], hylak_id: int) -> None:
    dates = pd.to_datetime({"year": df["year"], "month": df["month"], "day": 1})
    area_km2 = df["water_area"].to_numpy(dtype=float) / 1e6
    ax.plot(dates, area_km2, color="#2b8cbe", linewidth=1.0)

    break_idx = detail.get("udmax_break_index")
    if isinstance(break_idx, int) and 0 < break_idx < len(df):
        ax.axvline(dates.iloc[break_idx], color="#d95f0e", linestyle="--", linewidth=1.2)

    ax.set_title(
        f"hylak_id={hylak_id}  label={detail.get('label')}  "
        f"UDmax={detail.get('udmax', 0):.2f}  p={detail.get('udmax_p_value', 1):.3g}",
        fontsize=10,
    )
    ax.set_ylabel("area (km²)")
    ax.grid(alpha=0.2)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    load_env()

    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme

    Theme.apply()

    id_list = _read_candidates(args.candidate_file, args.top_n)
    if not id_list:
        log.warning("No candidate ids found in %s", args.candidate_file)
        return

    shift_filter = ShiftFilter(
        ShiftConfig(
            p_value_thresh=args.p_value_thresh,
            smooth_window=args.smooth_window,
            min_segment_months=args.min_segment_months,
        )
    )

    with series_db.connection_context() as conn:
        lake_frames = fetch_lake_area_by_ids(conn, id_list)
        frozen_map = fetch_frozen_year_months_by_ids(conn, id_list)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    for hylak_id in id_list:
        df = lake_frames.get(hylak_id)
        if df is None or df.empty:
            log.warning("No lake_area rows for hylak_id=%d", hylak_id)
            continue

        df_no_frozen = filter_frozen_rows(df, frozen_map.get(hylak_id, set()))
        if df_no_frozen.empty:
            log.warning("All rows filtered as frozen for hylak_id=%d", hylak_id)
            continue

        ctx = type("_Ctx", (), {})()
        ctx.df = df
        ctx.df_no_frozen = df_no_frozen
        ctx.rs_area_median = float(df_no_frozen["water_area"].median()) / 1_000_000
        ctx.rs_area_mean = float(df_no_frozen["water_area"].mean()) / 1_000_000
        ctx.rs_area_quantile = float(df_no_frozen["water_area"].quantile(0.8)) / 1_000_000
        ctx.atlas_area = 0.0
        result = shift_filter.classify(ctx)

        fig, ax = plt.subplots(figsize=(12, 4))
        _plot_lake(ax, df_no_frozen, result.detail, hylak_id)
        fig.tight_layout()
        out_path = args.output_dir / f"shift_candidate_{hylak_id}.png"
        fig.savefig(out_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        log.info("Saved %s", out_path)


if __name__ == "__main__":
    main()
