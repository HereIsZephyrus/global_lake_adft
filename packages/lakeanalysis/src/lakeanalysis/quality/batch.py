"""Batch adapters for the quality pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lakeanalysis.batch.engine import Calculator, LakeTask
from lakeanalysis.batch.io import BatchReader, BatchWriter
from lakesource.provider.base import LakeProvider

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


@dataclass(frozen=True)
class QualityTask:
    hylak_id: int
    series_df: object
    atlas_area: float
    frozen_year_months: frozenset[int]


class QualityBatchReader(BatchReader):
    def __init__(self, provider: LakeProvider) -> None:
        self._provider = provider

    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        return self._provider.fetch_lake_area_chunk(chunk_start, chunk_end)

    def fetch_lake_area_by_ids(self, id_list: list[int]):
        return self._provider.fetch_lake_area_by_ids(id_list)

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        return self._provider.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        return self._provider.fetch_frozen_year_months_by_ids(id_list)

    def fetch_max_hylak_id(self) -> int:
        return self._provider.fetch_max_hylak_id()

    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
        del algorithm
        quality_done = self._provider.fetch_done_ids("area_quality", chunk_start, chunk_end)
        anomaly_done = self._provider.fetch_done_ids("area_anomalies", chunk_start, chunk_end)
        return quality_done | anomaly_done

    def fetch_atlas_area_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, float]:
        return self._provider.fetch_atlas_area_chunk(chunk_start, chunk_end)

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        return self._provider.fetch_atlas_area_by_ids(id_list)

    def build_task(self, hylak_id: int, series_df: object, frozen_year_months: set[int]) -> LakeTask:
        atlas_area = self._provider.fetch_atlas_area_by_ids([hylak_id]).get(hylak_id, 0.0)
        return LakeTask(
            hylak_id=hylak_id,
            series_df=series_df,
            frozen_year_months=frozenset(frozen_year_months),
            extra={
                "quality_task": QualityTask(
                    hylak_id=hylak_id,
                    series_df=series_df,
                    atlas_area=atlas_area,
                    frozen_year_months=frozenset(frozen_year_months),
                )
            },
        )


class QualityBatchWriter(BatchWriter):
    def __init__(self, provider: LakeProvider, *, reset: bool = False) -> None:
        self._provider = provider
        self._reset = reset
        self._initialized = False

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        for table_name in ("area_quality", "area_anomalies"):
            rows = rows_by_table.get(table_name, [])
            if rows:
                self._provider.upsert_rows(table_name, rows)

    def ensure_schema(self, algorithm: str) -> None:
        del algorithm
        if self._initialized:
            return
        self._provider.ensure_table("area_quality")
        self._provider.ensure_table("area_anomalies")
        if self._reset:
            self._provider.truncate_table("area_quality")
            self._provider.truncate_table("area_anomalies")
        self._initialized = True


class QualityCalculator(Calculator):
    def __init__(self, config: QualityRunConfig) -> None:
        self._config = config
        self._filters = default_filters(
            zero_quantile_config=ZeroQuantileConfig(quantile=config.zero_quantile),
            flat_config=config.flat_config,
            ratio_config=config.ratio_config,
            pv_config=config.pv_config,
            outside_range_config=config.outside_range_config,
            shift_config=config.shift_config,
        )

    def run(self, task: LakeTask) -> dict[str, Any]:
        quality_task = task.extra["quality_task"]
        ctx, metrics = build_quality_context(
            df=quality_task.series_df,
            atlas_area=quality_task.atlas_area,
            frozen_year_months=quality_task.frozen_year_months,
            zero_quantile=self._config.zero_quantile,
        )
        decision = classify_area_anomaly(ctx, list(self._filters))
        row = {
            "hylak_id": quality_task.hylak_id,
            **metrics,
            "anomaly_flags": decision["anomaly_flags"],
        }
        return {
            "row": row,
            "is_anomalous": bool(decision["is_anomalous"]),
        }

    def result_to_rows(self, result: dict[str, Any]) -> dict[str, list[dict]]:
        table_name = "area_anomalies" if result["is_anomalous"] else "area_quality"
        return {table_name: [result["row"]]}

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        raise RuntimeError(
            f"quality calculation failed for hylak_id={hylak_id} in [{chunk_start}, {chunk_end}): {error}"
        ) from error


def build_quality_context(
    df: object,
    atlas_area: float,
    frozen_year_months: frozenset[int],
    zero_quantile: float,
) -> tuple[LakeContext, dict[str, int | float]]:
    df_no_frozen = filter_frozen_rows(df, set(frozen_year_months) or None)

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
