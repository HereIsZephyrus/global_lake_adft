"""Application runner for the area quality pipeline."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from lakesource.postgres import (
    ChunkedLakeProcessor,
    ensure_area_anomalies_table,
    ensure_area_quality_table,
    fetch_atlas_area_chunk,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_area_anomalies,
    upsert_area_quality,
)

from . import (
    AreaRatioConfig,
    FlatnessFilterConfig,
    LakeContext,
    OutsideRangeConfig,
    PenalizedVolatilityConfig,
    ShiftConfig,
    ZeroQuantileConfig,
    classify_area_anomaly,
    compute_mean_area,
    compute_median_area,
    compute_quantile_area,
    default_filters,
    filter_frozen_rows,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class QualityRunConfig:
    limit_id: int | None = None
    chunk_size: int = 10_000
    zero_quantile: float = 0.80
    flat_config: FlatnessFilterConfig = FlatnessFilterConfig()
    ratio_config: AreaRatioConfig = AreaRatioConfig()
    pv_config: PenalizedVolatilityConfig = PenalizedVolatilityConfig()
    outside_range_config: OutsideRangeConfig = OutsideRangeConfig()
    shift_config: ShiftConfig = ShiftConfig()
    reset: bool = False


def _log_run_config(config: QualityRunConfig) -> None:
    log.info(
        "Starting area quality pipeline, limit_id=%s, chunk_size=%d, reset=%s, "
        "zero_quantile=%.2f, "
        "flat_dominant_ratio_threshold=%.3f, flat_round_digits=%s, "
        "area_ratio_min=%.3f, area_ratio_max=%.1f, "
        "pv_threshold=%.4f, pv_dominant_ratio_max=%.2f, "
        "outside_range_tolerance=%.2f, "
        "shift_p_value=%.3f, shift_smooth_window=%d",
        config.limit_id,
        config.chunk_size,
        config.reset,
        config.zero_quantile,
        config.flat_config.dominant_ratio_threshold,
        config.flat_config.round_digits,
        config.ratio_config.min_ratio,
        config.ratio_config.max_ratio,
        config.pv_config.pv_threshold,
        config.pv_config.dominant_ratio_max,
        config.outside_range_config.tolerance,
        config.shift_config.p_value_thresh,
        config.shift_config.smooth_window,
    )


def build_quality_filters(config: QualityRunConfig) -> list:
    return default_filters(
        zero_quantile_config=ZeroQuantileConfig(quantile=config.zero_quantile),
        flat_config=config.flat_config,
        ratio_config=config.ratio_config,
        pv_config=config.pv_config,
        outside_range_config=config.outside_range_config,
        shift_config=config.shift_config,
    )


def _ensure_area_quality_tables() -> None:
    with series_db.connection_context() as conn:
        ensure_area_quality_table(conn)
        ensure_area_anomalies_table(conn)


def _reset_area_quality_tables() -> None:
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE area_quality, area_anomalies")
        conn.commit()
    log.info("Truncated area_quality and area_anomalies (reset mode)")


def _format_chunk_stats(counts: dict[str, int]) -> str:
    return (
        "(zero_quantile=%d, flat=%d, area_ratio=%d, outside_range=%d, pv=%d, shift=%d)"
        % (
            counts["zero_quantile"],
            counts["flat"],
            counts["area_ratio"],
            counts["outside_range"],
            counts["pv"],
            counts["shift"],
        )
    )


def _empty_chunk_counts() -> dict[str, int]:
    return {
        "zero_quantile": 0,
        "flat": 0,
        "area_ratio": 0,
        "outside_range": 0,
        "pv": 0,
        "shift": 0,
    }


def build_quality_context(
    df: object,
    atlas_area: float,
    frozen_ym: int | None,
    zero_quantile: float,
) -> tuple[LakeContext, dict[str, int | float]]:
    df_no_frozen = filter_frozen_rows(df, frozen_ym)

    rs_area_median = compute_median_area(df_no_frozen) / 1_000_000
    rs_area_mean = compute_mean_area(df_no_frozen) / 1_000_000
    rs_area_quantile = compute_quantile_area(df_no_frozen, quantile=zero_quantile) / 1_000_000

    ctx = LakeContext(
        df=df,
        df_no_frozen=df_no_frozen,
        rs_area_median=rs_area_median,
        rs_area_mean=rs_area_mean,
        rs_area_quantile=rs_area_quantile,
        atlas_area=atlas_area,
    )
    metrics = {
        "rs_area_mean": rs_area_mean,
        "rs_area_median": rs_area_median,
        "atlas_area": atlas_area,
    }
    return ctx, metrics


def classify_quality_lake(
    hylak_id: int,
    df: object,
    atlas_area: float,
    frozen_ym: int | None,
    zero_quantile: float,
    filters: Sequence,
) -> tuple[dict[str, int | float], bool, dict[str, int]]:
    ctx, metrics = build_quality_context(
        df=df,
        atlas_area=atlas_area,
        frozen_ym=frozen_ym,
        zero_quantile=zero_quantile,
    )
    decision = classify_area_anomaly(ctx, list(filters))
    row = {
        "hylak_id": hylak_id,
        **metrics,
        "anomaly_flags": decision["anomaly_flags"],
    }

    counts = _empty_chunk_counts()
    if bool(decision["is_anomalous"]):
        for name in counts:
            if bool(decision[f"is_{name}"]):
                counts[name] += 1
    return row, bool(decision["is_anomalous"]), counts


def run_quality(config: QualityRunConfig) -> None:
    """Execute the area quality pipeline in resumable chunks."""
    _log_run_config(config)
    _ensure_area_quality_tables()
    if config.reset:
        _reset_area_quality_tables()

    processor = ChunkedLakeProcessor(series_db, chunk_size=config.chunk_size, done_table="area_processed")
    filters = build_quality_filters(config)

    def process_chunk(chunk_start: int, chunk_end: int) -> dict[str, list[dict[str, int | float]]]:
        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            atlas_areas = fetch_atlas_area_chunk(conn, chunk_start, chunk_end)
            frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

        normal: list[dict[str, int | float]] = []
        anomalies: list[dict[str, int | float]] = []
        counts = _empty_chunk_counts()

        for hylak_id, df in lake_frames.items():
            row, is_anomalous, lake_counts = classify_quality_lake(
                hylak_id=hylak_id,
                df=df,
                atlas_area=atlas_areas.get(hylak_id, 0.0),
                frozen_ym=frozen_map.get(hylak_id),
                zero_quantile=config.zero_quantile,
                filters=filters,
            )
            if is_anomalous:
                anomalies.append(row)
                for name, value in lake_counts.items():
                    counts[name] += value
            else:
                normal.append(row)

        log.debug(
            "chunk [%d, %d): %d normal, %d anomalous %s",
            chunk_start,
            chunk_end,
            len(normal),
            len(anomalies),
            _format_chunk_stats(counts),
        )
        return {"normal": normal, "anomalies": anomalies}

    def upsert_chunk(result: dict[str, list[dict[str, int | float]]]) -> None:
        with series_db.connection_context() as conn:
            if result["normal"]:
                upsert_area_quality(conn, result["normal"])
            if result["anomalies"]:
                upsert_area_anomalies(conn, result["anomalies"])

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk, limit_id=config.limit_id)
