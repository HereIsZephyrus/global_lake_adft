"""EOTCalculator: batch wrapper for excess-over-threshold NHPP fitting."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from lakeanalysis.eot import EOTEstimator

from ..engine import Calculator, LakeTask

log = logging.getLogger(__name__)

CURRENT_EOT_WORKFLOW_VERSION = "eot-nhpp-v1"


@dataclass(frozen=True)
class EOTResult:
    hylak_id: int
    fits: list[tuple[str, float, Any]]


class EOTCalculator(Calculator):
    def __init__(
        self,
        *,
        tails: list[str] | None = None,
        quantiles: list[float] | None = None,
        workflow_version: str = CURRENT_EOT_WORKFLOW_VERSION,
    ) -> None:
        self._tails = tails or ["high", "low"]
        self._quantiles = quantiles or [0.95, 0.98]
        self._workflow_version = workflow_version

    def run(self, task: LakeTask) -> EOTResult:
        estimator = EOTEstimator()
        frozen = set(task.frozen_year_months)
        fits: list[tuple[str, float, Any]] = []
        for tail in self._tails:
            for q in self._quantiles:
                try:
                    fit = estimator.fit(
                        task.series_df,
                        tail=tail,
                        threshold_quantile=q,
                        frozen_year_months=frozen,
                    )
                    fits.append((tail, q, fit))
                except Exception as exc:
                    log.debug(
                        "hylak_id=%d tail=%s q=%.2f error: %s",
                        task.hylak_id, tail, q, exc,
                    )
                    fits.append((tail, q, exc))
        return EOTResult(hylak_id=task.hylak_id, fits=fits)

    def result_to_rows(self, result: EOTResult) -> dict[str, list[dict]]:
        result_rows: list[dict] = []
        extreme_rows: list[dict] = []
        has_success = False

        for tail, q, fit_or_exc in result.fits:
            q_decimal = str(Decimal(str(q)))
            if isinstance(fit_or_exc, Exception):
                result_rows.append(self._error_result_row(result.hylak_id, tail, q_decimal, fit_or_exc, set()))
                continue

            fit = fit_or_exc
            has_success = True
            p = fit.params
            ll = fit.log_likelihood
            result_rows.append({
                "hylak_id": result.hylak_id,
                "tail": tail,
                "threshold_quantile": q_decimal,
                "converged": bool(fit.converged),
                "log_likelihood": float(ll) if math.isfinite(ll) else None,
                "threshold": float(fit.threshold),
                "n_extremes": int(len(fit.extremes)),
                "n_observations": int(fit.series.n_obs),
                "n_frozen_months": int(len(fit.frozen_year_months)),
                "beta0": p.get("beta0"),
                "beta1": p.get("beta1"),
                "sin_1": p.get("sin_1"),
                "cos_1": p.get("cos_1"),
                "sigma": p.get("sigma"),
                "xi": p.get("xi"),
                "error_message": None,
            })
            extreme_rows.extend([
                {
                    "hylak_id": result.hylak_id,
                    "tail": tail,
                    "threshold_quantile": q_decimal,
                    "cluster_id": int(row["cluster_id"]),
                    "cluster_size": int(row["cluster_size"]),
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "water_area": float(row["original_value"]),
                    "threshold_at_event": float(row["threshold"]),
                }
                for _, row in fit.extremes.iterrows()
            ])

        status_rows = [self._make_run_status_row(result.hylak_id, 0, 0, has_success)]
        return {
            "eot_results": result_rows,
            "eot_extremes": extreme_rows,
            "eot_run_status": status_rows,
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        result_rows: list[dict] = []
        for tail in self._tails:
            for q in self._quantiles:
                q_decimal = str(Decimal(str(q)))
                result_rows.append(self._error_result_row(hylak_id, tail, q_decimal, error, set()))

        return {
            "eot_results": result_rows,
            "eot_run_status": [self._make_run_status_row(hylak_id, chunk_start, chunk_end, False, error)],
        }

    def _error_result_row(
        self, hylak_id: int, tail: str, q_decimal: str, exc: Exception, frozen: set[int]
    ) -> dict:
        return {
            "hylak_id": hylak_id,
            "tail": tail,
            "threshold_quantile": q_decimal,
            "converged": False,
            "log_likelihood": None,
            "threshold": None,
            "n_extremes": None,
            "n_observations": None,
            "n_frozen_months": int(len(frozen)),
            "beta0": None,
            "beta1": None,
            "sin_1": None,
            "cos_1": None,
            "sigma": None,
            "xi": None,
            "error_message": str(exc)[:500],
        }

    def _make_run_status_row(
        self,
        hylak_id: int,
        chunk_start: int,
        chunk_end: int,
        success: bool,
        error: Exception | None = None,
    ) -> dict:
        return {
            "hylak_id": hylak_id,
            "chunk_start": chunk_start,
            "chunk_end": chunk_end,
            "workflow_version": self._workflow_version,
            "status": "done" if success else "error",
            "error_message": str(error)[:500] if error else None,
        }