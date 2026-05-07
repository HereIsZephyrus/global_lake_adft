"""Run the area quality assessment pipeline.

Steps:
  1. Ensure area_quality, area_anomalies tables and area_processed view exist
     in SERIES_DB.
  2. Split the full hylak_id space into chunks of --chunk-size (default 10 000).
  3. For each pending chunk: fetch lake_area, atlas_area, and frozen months from
     SERIES_DB, compute rs_area_mean and rs_area_median per lake (converted from
     m² to km²) from defrozen data, run anomaly filters, then upsert to SERIES_DB:
       - non-anomalous  →  area_quality
       - anomalous      →  area_anomalies (with anomaly_flags bitmask)
  4. Chunks already fully recorded in area_processed (UNION of both tables) are
     skipped automatically, enabling safe resume after an interrupted run.

Usage examples:
    # Full run (chunked, resumable):
    uv run python scripts/run_quality.py

    # Test with only rows where hylak_id < 5000:
    uv run python scripts/run_quality.py --limit-id 5000

    # Adjust chunk size:
    uv run python scripts/run_quality.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
import logging

from lakesource.postgres import (
    ChunkedLakeProcessor,  # deprecated: use Engine + LakeProvider
    ensure_area_anomalies_table,
    ensure_area_quality_table,
    fetch_atlas_area_chunk,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_area_anomalies,
    upsert_area_quality,
)
from lakeanalysis.logger import Logger
from lakeanalysis.quality import (
    FlatnessFilterConfig,
    ZeroQuantileConfig,
    AreaRatioConfig,
    PenalizedVolatilityConfig,
    OutsideRangeConfig,
    ShiftConfig,
    LakeContext,
    classify_area_anomaly,
    compute_mean_area,
    compute_median_area,
    compute_quantile_area,
    default_filters,
    filter_frozen_rows,
)

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assess lake area data quality.")
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process rows with hylak_id < N (for testing).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values processed per chunk (default: 10000).",
    )
    parser.add_argument(
        "--zero-quantile",
        type=float,
        default=0.80,
        metavar="Q",
        help="Zero-quantile filter: quantile position (0-1) at which zero area is flagged (default: 0.75).",
    )
    parser.add_argument(
        "--flat-dominant-ratio-threshold",
        type=float,
        default=0.8,
        metavar="R",
        help="Flatness filter: dominant value frequency ratio threshold.",
    )
    parser.add_argument(
        "--flat-round-digits",
        type=int,
        default=None,
        metavar="N",
        help="Optional rounding digits before flatness statistics.",
    )
    parser.add_argument(
        "--area-ratio-min",
        type=float,
        default=0.1,
        metavar="R",
        help="Area ratio filter: minimum acceptable ratio (default: 0.1).",
    )
    parser.add_argument(
        "--area-ratio-max",
        type=float,
        default=10.0,
        metavar="R",
        help="Area ratio filter: maximum acceptable ratio (default: 10.0).",
    )
    parser.add_argument(
        "--pv-threshold",
        type=float,
        default=0.001,
        metavar="R",
        help="H×CV filter: penalized_volatility <= threshold flags anomaly (default: 0.001).",
    )
    parser.add_argument(
        "--outside-range-tolerance",
        type=float,
        default=0.5,
        metavar="R",
        help="Outside range filter: fractional tolerance beyond observed range (default: 0.5).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate area_quality and area_anomalies before running (reprocess all chunks).",
    )
    parser.add_argument(
        "--shift-p-value",
        type=float,
        default=0.05,
        metavar="P",
        help="Shift filter: Pettitt significance threshold (default: 0.05).",
    )
    parser.add_argument(
        "--shift-smooth-window",
        type=int,
        default=12,
        metavar="N",
        help="Shift filter: rolling smooth window in months (default: 12).",
    )
    return parser.parse_args()


def run(
    limit_id: int | None = None,
    chunk_size: int = 10_000,
    zero_quantile: float = 0.80,
    flat_config: FlatnessFilterConfig | None = None,
    ratio_config: AreaRatioConfig | None = None,
    pv_config: PenalizedVolatilityConfig | None = None,
    outside_range_config: OutsideRangeConfig | None = None,
    shift_config: ShiftConfig | None = None,
    reset: bool = False,
) -> None:
    """Execute the area quality pipeline in resumable chunks."""
    zero_quantile_config = ZeroQuantileConfig(quantile=zero_quantile)
    if flat_config is None:
        flat_config = FlatnessFilterConfig()
    if ratio_config is None:
        ratio_config = AreaRatioConfig()
    if pv_config is None:
        pv_config = PenalizedVolatilityConfig()
    if outside_range_config is None:
        outside_range_config = OutsideRangeConfig()
    if shift_config is None:
        shift_config = ShiftConfig()

    log.info(
        "Starting area quality pipeline, limit_id=%s, chunk_size=%d, reset=%s, "
        "zero_quantile=%.2f, "
        "flat_dominant_ratio_threshold=%.3f, flat_round_digits=%s, "
        "area_ratio_min=%.3f, area_ratio_max=%.1f, "
        "pv_threshold=%.4f, pv_dominant_ratio_max=%.2f, "
        "outside_range_tolerance=%.2f, "
        "shift_p_value=%.3f, shift_smooth_window=%d",
        limit_id,
        chunk_size,
        reset,
        zero_quantile,
        flat_config.dominant_ratio_threshold,
        flat_config.round_digits,
        ratio_config.min_ratio,
        ratio_config.max_ratio,
        pv_config.pv_threshold,
        pv_config.dominant_ratio_max,
        outside_range_config.tolerance,
        shift_config.p_value_thresh,
        shift_config.smooth_window,
    )

    with series_db.connection_context() as conn:
        ensure_area_quality_table(conn)
        ensure_area_anomalies_table(conn)

    if reset:
        with series_db.connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE area_quality, area_anomalies")
            conn.commit()
        log.info("Truncated area_quality and area_anomalies (reset mode)")

    processor = ChunkedLakeProcessor(series_db, chunk_size=chunk_size, done_table="area_processed")
    filters = default_filters(zero_quantile_config=zero_quantile_config, flat_config=flat_config, ratio_config=ratio_config, pv_config=pv_config, outside_range_config=outside_range_config, shift_config=shift_config)

    def process_chunk(chunk_start: int, chunk_end: int) -> dict[str, list[dict]]:
        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            atlas_areas = fetch_atlas_area_chunk(conn, chunk_start, chunk_end)
            frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

        normal: list[dict] = []
        anomalies: list[dict] = []
        n_zero_quantile = 0
        n_flat = 0
        n_area_ratio = 0
        n_outside_range = 0
        n_pv = 0
        n_shift = 0

        for hylak_id, df in lake_frames.items():
            frozen_ym = frozen_map.get(hylak_id, None)
            df_no_frozen = filter_frozen_rows(df, frozen_ym)

            rs_area_median = compute_median_area(df_no_frozen) / 1_000_000
            rs_area_mean = compute_mean_area(df_no_frozen) / 1_000_000
            rs_area_quantile = compute_quantile_area(df_no_frozen, quantile=zero_quantile) / 1_000_000
            atlas_area = atlas_areas.get(hylak_id, 0.0)

            ctx = LakeContext(
                df=df,
                df_no_frozen=df_no_frozen,
                rs_area_median=rs_area_median,
                rs_area_mean=rs_area_mean,
                rs_area_quantile=rs_area_quantile,
                atlas_area=atlas_area,
            )
            decision = classify_area_anomaly(ctx, filters)
            row = {
                "hylak_id": hylak_id,
                "rs_area_mean": rs_area_mean,
                "rs_area_median": rs_area_median,
                "atlas_area": atlas_area,
                "anomaly_flags": decision["anomaly_flags"],
            }
            if bool(decision["is_anomalous"]):
                anomalies.append(row)
                if bool(decision["is_zero_quantile"]):
                    n_zero_quantile += 1
                if bool(decision["is_flat"]):
                    n_flat += 1
                if bool(decision["is_area_ratio"]):
                    n_area_ratio += 1
                if bool(decision["is_outside_range"]):
                    n_outside_range += 1
                if bool(decision["is_pv"]):
                    n_pv += 1
                if bool(decision["is_shift"]):
                    n_shift += 1
            else:
                normal.append(row)

        log.debug(
            "chunk [%d, %d): %d normal, %d anomalous "
            "(zero_quantile=%d, flat=%d, area_ratio=%d, outside_range=%d, pv=%d, shift=%d)",
            chunk_start,
            chunk_end,
            len(normal),
            len(anomalies),
            n_zero_quantile,
            n_flat,
            n_area_ratio,
            n_outside_range,
            n_pv,
            n_shift,
        )
        return {"normal": normal, "anomalies": anomalies}

    def upsert_chunk(result: dict[str, list[dict]]) -> None:
        with series_db.connection_context() as conn:
            if result["normal"]:
                upsert_area_quality(conn, result["normal"])
            if result["anomalies"]:
                upsert_area_anomalies(conn, result["anomalies"])

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk, limit_id=limit_id)


def main() -> None:
    args = parse_args()
    Logger("run_quality")
    flat_config = FlatnessFilterConfig(
        dominant_ratio_threshold=args.flat_dominant_ratio_threshold,
        round_digits=args.flat_round_digits,
    )
    ratio_config = AreaRatioConfig(
        min_ratio=args.area_ratio_min,
        max_ratio=args.area_ratio_max,
    )
    pv_config = PenalizedVolatilityConfig(
        pv_threshold=args.pv_threshold,
    )
    outside_range_config = OutsideRangeConfig(
        tolerance=args.outside_range_tolerance,
    )
    shift_config = ShiftConfig(
        p_value_thresh=args.shift_p_value,
        smooth_window=args.shift_smooth_window,
    )
    run(
        limit_id=args.limit_id,
        chunk_size=args.chunk_size,
        zero_quantile=args.zero_quantile,
        flat_config=flat_config,
        ratio_config=ratio_config,
        pv_config=pv_config,
        outside_range_config=outside_range_config,
        shift_config=shift_config,
        reset=args.reset,
    )


if __name__ == "__main__":
    main()
