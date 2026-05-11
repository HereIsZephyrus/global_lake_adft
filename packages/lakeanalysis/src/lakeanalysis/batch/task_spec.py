"""Task-specific batch IO specs kept outside the protocol layer."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatchTaskSpec:
    done_table: str | None = None
    done_requires_status: bool = False
    ensure_tables: tuple[str, ...] = ()


_TASK_SPECS: dict[str, BatchTaskSpec] = {
    "quantile": BatchTaskSpec(
        done_table="quantile_run_status",
        done_requires_status=True,
        ensure_tables=(
            "quantile_labels",
            "quantile_extremes",
            "quantile_abrupt_transitions",
            "quantile_run_status",
        ),
    ),
    "pwm_extreme": BatchTaskSpec(
        done_table="pwm_extreme_run_status",
        done_requires_status=True,
        ensure_tables=(
            "pwm_extreme_thresholds",
            "pwm_extreme_run_status",
        ),
    ),
    "eot": BatchTaskSpec(
        done_table="eot_run_status",
        done_requires_status=True,
        ensure_tables=(
            "eot_results",
            "eot_extremes",
            "eot_run_status",
        ),
    ),
    "comparison": BatchTaskSpec(
        done_table="comparison_run_status",
        done_requires_status=True,
        ensure_tables=(
            "comparison_run_status",
            "quantile_labels",
            "quantile_extremes",
            "quantile_abrupt_transitions",
            "quantile_run_status",
            "pwm_extreme_thresholds",
            "pwm_extreme_run_status",
        ),
    ),
    "pwm_hawkes": BatchTaskSpec(
        done_table="pwm_hawkes_run_status",
        done_requires_status=True,
        ensure_tables=(
            "pwm_extreme_thresholds",
            "pwm_extreme_run_status",
            "pwm_hawkes_run_status",
        ),
    ),
    "shift_labels": BatchTaskSpec(
        done_table="area_shift_labels",
        done_requires_status=False,
        ensure_tables=(),
    ),
}


def get_batch_task_spec(algorithm: str) -> BatchTaskSpec:
    return _TASK_SPECS.get(algorithm, BatchTaskSpec())
