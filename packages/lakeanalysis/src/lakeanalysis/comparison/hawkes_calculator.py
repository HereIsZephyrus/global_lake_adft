"""HawkesComparisonCalculator: PWM-Hawkes vs EOT-Hawkes side-by-side comparison."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lakeanalysis.batch import Calculator, LakeTask
from lakeanalysis.batch.calculator.eot_hawkes import EOTHawkesCalculator
from lakeanalysis.batch.calculator.hawkes_base import HawkesResult
from lakeanalysis.batch.calculator.pwm_hawkes import PWMHawkesCalculator

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class HawkesComparisonResult:
    """Hawkes Comparison Result for one lake."""
    hylak_id: int
    pwm_result: HawkesResult | Exception | None
    eot_result: HawkesResult | Exception | None


class HawkesComparisonCalculator(Calculator):
    """Batch calculator running PWM-Hawkes and EOT-Hawkes side-by-side."""

    def __init__(
        self,
        *,
        pwm_config: dict | None = None,
        eot_config: dict | None = None,
    ) -> None:
        self._pwm_calc = PWMHawkesCalculator(**(pwm_config or {}))
        self._eot_calc = EOTHawkesCalculator(**(eot_config or {}))

    def compute(self, task: LakeTask) -> HawkesComparisonResult:
        """Compute both Hawkes pipelines for one lake."""
        pwm_result: HawkesResult | Exception | None = None
        try:
            pwm_result = self._pwm_calc.compute(task)
        except Exception as exc:
            log.debug("hylak_id=%d pwm_hawkes error: %s", task.hylak_id, exc)
            pwm_result = exc

        eot_result: HawkesResult | Exception | None = None
        try:
            eot_result = self._eot_calc.compute(task)
        except Exception as exc:
            log.debug("hylak_id=%d eot_hawkes error: %s", task.hylak_id, exc)
            eot_result = exc

        return HawkesComparisonResult(
            hylak_id=task.hylak_id,
            pwm_result=pwm_result,
            eot_result=eot_result,
        )

    def result_to_rows(self, result: HawkesComparisonResult) -> dict[str, list[dict]]:
        """Convert comparison result to table rows."""
        rows: dict[str, list[dict]] = {}

        # Include individual algorithm rows
        if isinstance(result.pwm_result, HawkesResult):
            pwm_rows = self._pwm_calc.result_to_rows(result.pwm_result)
            for table, r in pwm_rows.items():
                rows.setdefault(table, []).extend(r)
        if isinstance(result.eot_result, HawkesResult):
            eot_rows = self._eot_calc.result_to_rows(result.eot_result)
            for table, r in eot_rows.items():
                rows.setdefault(table, []).extend(r)

        # Build comparison row
        cmp_row: dict = {"hylak_id": int(result.hylak_id)}

        if isinstance(result.pwm_result, HawkesResult):
            pwm_summary = result.pwm_result.core.summary
            cmp_row["pwm_hawkes_qc_pass"] = int(pwm_summary.get("hawkes_qc_pass", False))
            cmp_row["pwm_hawkes_converged"] = int(pwm_summary.get("converged", False))
            cmp_row["pwm_lrt_p_d_to_w"] = pwm_summary.get("lrt_p_d_to_w")
            cmp_row["pwm_lrt_p_w_to_d"] = pwm_summary.get("lrt_p_w_to_d")
            cmp_row["pwm_n_events"] = pwm_summary.get("n_events")
            cmp_row["pwm_n_dry"] = pwm_summary.get("n_dry")
            cmp_row["pwm_n_wet"] = pwm_summary.get("n_wet")
        else:
            cmp_row["pwm_hawkes_qc_pass"] = 0
            cmp_row["pwm_hawkes_converged"] = 0

        if isinstance(result.eot_result, HawkesResult):
            eot_summary = result.eot_result.core.summary
            cmp_row["eot_hawkes_qc_pass"] = int(eot_summary.get("hawkes_qc_pass", False))
            cmp_row["eot_hawkes_converged"] = int(eot_summary.get("converged", False))
            cmp_row["eot_lrt_p_d_to_w"] = eot_summary.get("lrt_p_d_to_w")
            cmp_row["eot_lrt_p_w_to_d"] = eot_summary.get("lrt_p_w_to_d")
            cmp_row["eot_n_events"] = eot_summary.get("n_events")
            cmp_row["eot_n_dry"] = eot_summary.get("n_dry")
            cmp_row["eot_n_wet"] = eot_summary.get("n_wet")
        else:
            cmp_row["eot_hawkes_qc_pass"] = 0
            cmp_row["eot_hawkes_converged"] = 0

        rows["hawkes_comparison"] = [cmp_row]
        return rows

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        """Error rows."""
        return {
            "hawkes_comparison": [
                {
                    "hylak_id": hylak_id,
                    "pwm_hawkes_qc_pass": 0,
                    "pwm_hawkes_converged": 0,
                    "eot_hawkes_qc_pass": 0,
                    "eot_hawkes_converged": 0,
                }
            ],
        }
