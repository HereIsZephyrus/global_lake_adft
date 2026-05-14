"""ShiftLabelsCalculator: compute structural shift labels for lakes."""

from __future__ import annotations

import logging
from typing import Any

from lakeanalysis.batch.domain import Calculator, LakeTask
from lakeanalysis.quality.batch import build_quality_context
from .filters.shift import ShiftConfig, ShiftFilter

log = logging.getLogger(__name__)


class ShiftLabelsCalculator(Calculator):
    def __init__(self, config: ShiftConfig | None = None) -> None:
        self._config = config or ShiftConfig()
        self._filter = ShiftFilter(self._config)

    def run(self, task: LakeTask) -> dict[str, Any]:
        ctx, _ = build_quality_context(
            df=task.series_df,
            atlas_area=0.0,
            frozen_year_months=task.frozen_year_months,
            zero_quantile=0.8,
        )
        result = self._filter.classify(ctx)
        return {
            "hylak_id": task.hylak_id,
            "detail": result.detail,
        }

    def result_to_rows(self, result: dict[str, Any]) -> dict[str, list[dict]]:
        detail = result["detail"]
        return {
            "area_shift_labels": [
                {
                    "hylak_id": result["hylak_id"],
                    "shift_label": detail.get("label", "stable"),
                    "udmax": detail.get("udmax"),
                    "udmax_p_value": detail.get("udmax_p_value"),
                    "udmax_break_index": detail.get("udmax_break_index"),
                    "wdmax": detail.get("wdmax"),
                    "wdmax_p_value": detail.get("wdmax_p_value"),
                    "wdmax_break_index": detail.get("wdmax_break_index"),
                    "used_deseasoned": detail.get("used_deseasoned"),
                    "seasonality_dominance_ratio": detail.get("seasonality_dominance_ratio"),
                }
            ]
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        return {
            "area_shift_labels": [
                {
                    "hylak_id": hylak_id,
                    "shift_label": "stable",
                    "udmax": None,
                    "udmax_p_value": None,
                    "udmax_break_index": None,
                    "wdmax": None,
                    "wdmax_p_value": None,
                    "wdmax_break_index": None,
                    "used_deseasoned": None,
                    "seasonality_dominance_ratio": None,
                }
            ]
        }
