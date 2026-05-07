"""Scan lake_area in chunks and export structure-shift candidates.

This script is read-only: it reads monthly lake area and frozen-month masks,
runs the breakpoint-based shift filter offline, and writes candidate lakes to
local files for later manual review.

Usage:
    uv run python scripts/sample_shift_degraded_candidates.py
    uv run python scripts/sample_shift_degraded_candidates.py --limit-id 50000
    uv run python scripts/sample_shift_degraded_candidates.py --chunk-size 5000 --top-n 200
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.env import load_env
from lakesource.postgres import fetch_frozen_year_months_chunk, fetch_lake_area_chunk, series_db
from lakeanalysis.logger import Logger
from lakeanalysis.quality import LakeContext, ShiftConfig, filter_frozen_rows
from lakeanalysis.quality.filters.shift import ShiftFilter

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export offline shift-filter candidates.")
    parser.add_argument("--limit-id", type=int, default=None, metavar="N")
    parser.add_argument("--chunk-size", type=int, default=10_000, metavar="N")
    parser.add_argument("--top-n", type=int, default=100, metavar="N")
    parser.add_argument("--min-segment-months", type=int, default=24, metavar="N")
    parser.add_argument("--smooth-window", type=int, default=12, metavar="N")
    parser.add_argument("--p-value-thresh", type=float, default=0.05, metavar="P")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DATA_DIR / "comparison" / "shift_candidates",
    )
    return parser.parse_args()


def _fetch_max_hylak_id() -> int:
    query = "SELECT MAX(hylak_id) FROM lake_info"
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            value = cur.fetchone()[0]
    return 0 if value is None else int(value)


def _classify_lake(hylak_id: int, df: pd.DataFrame, frozen_map: dict[int, set[int]], shift_filter: ShiftFilter) -> dict[str, object] | None:
    frozen_ym = frozen_map.get(hylak_id)
    df_no_frozen = filter_frozen_rows(df, frozen_ym)
    if df_no_frozen.empty:
        return None

    ctx = LakeContext(
        df=df,
        df_no_frozen=df_no_frozen,
        rs_area_median=float(df_no_frozen["water_area"].median()) / 1_000_000,
        rs_area_mean=float(df_no_frozen["water_area"].mean()) / 1_000_000,
        rs_area_quantile=float(df_no_frozen["water_area"].quantile(0.8)) / 1_000_000,
        atlas_area=0.0,
    )
    result = shift_filter.classify(ctx)
    detail = result.detail
    return {
        "hylak_id": hylak_id,
        "label": detail.get("label"),
        "is_anomaly": bool(result.is_anomaly),
        "n_obs": int(len(df)),
        "n_obs_no_frozen": int(len(df_no_frozen)),
        "n_frozen": int(len(df) - len(df_no_frozen)),
        "udmax": float(detail.get("udmax", 0.0)),
        "udmax_p_value": float(detail.get("udmax_p_value", 1.0)),
        "udmax_break_index": detail.get("udmax_break_index"),
        "wdmax": float(detail.get("wdmax", 0.0)),
        "wdmax_p_value": float(detail.get("wdmax_p_value", 1.0)),
        "wdmax_break_index": detail.get("wdmax_break_index"),
        "used_deseasoned": bool(detail.get("used_deseasoned", False)),
        "seasonality_dominance_ratio": float(detail.get("seasonality_dominance_ratio", 0.0)),
    }


def run(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    shift_filter = ShiftFilter(
        ShiftConfig(
            p_value_thresh=args.p_value_thresh,
            smooth_window=args.smooth_window,
            min_segment_months=args.min_segment_months,
        )
    )
    max_hylak_id = _fetch_max_hylak_id()
    upper_bound = min(args.limit_id, max_hylak_id + 1) if args.limit_id is not None else max_hylak_id + 1

    log.info(
        "Scanning shift candidates, upper_bound=%s, chunk_size=%d, top_n=%d",
        upper_bound,
        args.chunk_size,
        args.top_n,
    )

    all_rows: list[dict[str, object]] = []
    for chunk_start in range(1, upper_bound, args.chunk_size):
        chunk_end = min(chunk_start + args.chunk_size, upper_bound)
        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

        if not lake_frames:
            continue

        for hylak_id, df in lake_frames.items():
            row = _classify_lake(hylak_id, df, frozen_map, shift_filter)
            if row is not None:
                all_rows.append(row)

        log.info("Scanned chunk [%d, %d): %d lakes", chunk_start, chunk_end, len(lake_frames))

    all_df = pd.DataFrame(all_rows)
    if all_df.empty:
        return all_df, all_df

    degraded = all_df[
        (all_df["label"] == "degraded")
        & (all_df["udmax_p_value"] <= args.p_value_thresh)
        & (all_df["wdmax_p_value"] > args.p_value_thresh)
    ].copy()
    degraded = degraded.sort_values(["udmax", "n_obs_no_frozen"], ascending=[False, False])
    top_df = degraded.head(args.top_n).reset_index(drop=True)
    return all_df, top_df


def main() -> None:
    args = parse_args()
    Logger("sample_shift_degraded_candidates")
    load_env()

    all_df, top_df = run(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_path = args.output_dir / "shift_candidates_all.parquet"
    top_path = args.output_dir / "shift_candidates_top.parquet"
    top_csv_path = args.output_dir / "shift_candidates_top.csv"

    all_df.to_parquet(all_path, index=False)
    top_df.to_parquet(top_path, index=False)
    top_df.to_csv(top_csv_path, index=False)

    print("Shift candidate summary")
    print("=" * 72)
    print(f"all_rows={len(all_df)}")
    print(f"degraded_top_rows={len(top_df)}")
    print(f"all_path={all_path}")
    print(f"top_path={top_path}")
    print(f"top_csv_path={top_csv_path}")

    if not top_df.empty:
        preview = top_df[["hylak_id", "label", "udmax", "udmax_p_value", "udmax_break_index", "wdmax_p_value", "n_obs_no_frozen"]].head(20)
        print()
        print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
