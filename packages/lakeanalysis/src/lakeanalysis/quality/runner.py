"""Application runner for the area quality pipeline."""

from __future__ import annotations

import logging

from lakeanalysis.batch import Engine, RangeFilter
from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider

from . import default_filters, ZeroQuantileConfig

from .batch import (  # pylint: disable=unused-import
    QualityBatchReader,
    QualityBatchWriter,
    QualityCalculator,
    QualityRunConfig,
    build_quality_context,
)

log = logging.getLogger(__name__)
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
def run_quality(config: QualityRunConfig, source_config: SourceConfig | None = None) -> None:
    """Execute the area quality pipeline in resumable chunks."""
    _log_run_config(config)
    provider = create_provider(source_config or SourceConfig())
    reader = QualityBatchReader(provider)
    writer = QualityBatchWriter(provider, reset=config.reset)
    calculator = QualityCalculator(config)
    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        algorithm="quality",
        lake_filter=RangeFilter(end=config.limit_id) if config.limit_id is not None else None,
        chunk_size=config.chunk_size,
    )
    engine.run()
