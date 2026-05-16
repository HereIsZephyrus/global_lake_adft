"""EOTHawkesCalculator: EOT event extraction -> Hawkes fit.

Uses the EOT (Extremes Over Threshold) pathway to extract events, then fits
a bivariate Hawkes process via the shared ``run_hawkes_pipeline`` orchestrator.
Defaults to RunsDeclustering to decluster consecutive exceedances;
NoDeclustering available for comparison.
"""

from __future__ import annotations

from decimal import Decimal
import logging

from lakeanalysis.eot import (
    EOTEstimator,
    NoDeclustering,
    ReturnLevelEstimator,
    RunsDeclustering,
)
from lakeanalysis.hawkes import (
    HawkesCoreResult,
    build_error_summary,
    build_events_from_eot,
    run_hawkes_pipeline,
)
from lakesource.eot import return_levels_to_rows

from .. import LakeTask
from .hawkes_base import HawkesCalculator, HawkesResult

log = logging.getLogger(__name__)


def _q_str(value: float) -> str:
    return str(Decimal(str(value)))


class EOTHawkesCalculator(HawkesCalculator):
    """Batch calculator: EOT event extraction -> Hawkes process fitting.

    Pipeline:
        1. Build dual-type event series from EOT (high/low tail quantile threshold)
        2. QC check + fit + LRT + decomposition via ``run_hawkes_pipeline``
    """

    def __init__(
        self,
        *,
        threshold_quantile: float | None = None,
        threshold_quantiles: list[float] | None = None,
        hawkes_window_months: float = 4.0,
        monthly_significance_quantile: float = 0.95,
        decluster_run_length: int | None = 1,
    ) -> None:
        super().__init__(
            hawkes_window_months=hawkes_window_months,
            monthly_significance_quantile=monthly_significance_quantile,
        )
        self._table_prefix = "eot_hawkes"
        self._return_levels_table = "eot_return_levels"
        if threshold_quantiles is not None:
            self._threshold_quantiles = threshold_quantiles
        elif threshold_quantile is not None:
            self._threshold_quantiles = [threshold_quantile]
        else:
            self._threshold_quantiles = [0.95, 0.99]
        self._decluster_run_length = decluster_run_length

    def compute(self, task: LakeTask) -> HawkesResult:
        hylak_id = task.hylak_id
        series_df = task.series_df
        frozen = set(task.frozen_year_months) if task.frozen_year_months else None

        try:
            if self._decluster_run_length is None or self._decluster_run_length <= 0:
                decluster_strategy = NoDeclustering()
            else:
                decluster_strategy = RunsDeclustering(
                    run_length=self._decluster_run_length
                )
            result_rows: list[dict] = []
            lrt_rows: list[dict] = []
            transition_rows: list[dict] = []
            return_level_rows: list[dict] = []
            errors: list[str] = []

            for threshold_quantile in self._threshold_quantiles:
                try:
                    event_series = build_events_from_eot(
                        series_df,
                        threshold_quantile=threshold_quantile,
                        frozen_year_months=frozen,
                        declustering_strategy=decluster_strategy,
                    )
                    events_table = event_series.events_table
                    return_level_rows.extend(
                        self._build_return_level_rows(
                            series_df,
                            frozen,
                            decluster_strategy,
                            hylak_id=hylak_id,
                            threshold_quantile=threshold_quantile,
                        )
                    )
                    core = run_hawkes_pipeline(
                        event_series,
                        events_table,
                        series_df,
                        hylak_id=hylak_id,
                        threshold_quantile=threshold_quantile,
                        hawkes_window_months=self._hawkes_window_months,
                        monthly_significance_quantile=self._monthly_significance_quantile,
                    )
                    result_rows.append(core.summary)
                    lrt_rows.extend(core.lrt_rows)
                    transition_rows.extend(core.transition_monthly_rows)
                except Exception as exc:
                    log.debug(
                        "EOT-Hawkes failed for hylak_id=%d q=%.4f: %s",
                        task.hylak_id,
                        threshold_quantile,
                        exc,
                    )
                    result_rows.append(
                        build_error_summary(
                            hylak_id,
                            str(exc),
                            threshold_quantile,
                        )
                    )
                    errors.append(f"q={_q_str(threshold_quantile)}: {exc}")

            first_summary = result_rows[0] if result_rows else build_error_summary(hylak_id, "No threshold_quantiles configured")
            aggregate_summary = dict(first_summary)
            aggregate_summary["error_message"] = None if not errors else "; ".join(errors)[:500]
            return self._make_result(
                HawkesCoreResult(
                    summary=aggregate_summary,
                    lrt_rows=lrt_rows,
                    transition_monthly_rows=transition_rows,
                ),
                return_level_rows=return_level_rows,
                extra_rows_by_table={"eot_hawkes_results": result_rows},
            )
        except Exception as exc:
            log.debug("EOT-Hawkes failed for hylak_id=%d: %s", task.hylak_id, exc)
            error_summary = build_error_summary(
                hylak_id, str(exc), self._threshold_quantiles[0]
            )
            return self._make_result(
                HawkesCoreResult(
                    summary=error_summary, lrt_rows=[], transition_monthly_rows=[]
                )
            )

    def _build_return_level_rows(
        self,
        series_df,
        frozen_year_months: set[int] | None,
        decluster_strategy,
        *,
        hylak_id: int,
        threshold_quantile: float,
    ) -> list[dict]:
        estimator = EOTEstimator(declustering_strategy=decluster_strategy)
        fit_high, fit_low = estimator.fit_both_tails(
            series_df,
            threshold_quantile=threshold_quantile,
            frozen_year_months=frozen_year_months,
        )
        rows_by_tail = {
            "high": ReturnLevelEstimator(fit_high).estimate().to_dict("records"),
            "low": ReturnLevelEstimator(fit_low).estimate().to_dict("records"),
        }
        return return_levels_to_rows(
            hylak_id,
            threshold_quantile,
            rows_by_tail,
        )

    def result_to_rows(self, result: HawkesResult) -> dict[str, list[dict]]:
        rows = super().result_to_rows(result)
        extra_results = result.extra_rows_by_table.get("eot_hawkes_results")
        if extra_results is not None:
            rows["eot_hawkes_results"] = [
                {
                    **row,
                    "threshold_quantile": _q_str(float(row["threshold_quantile"])),
                }
                for row in extra_results
            ]
        return rows
