"""PWM extreme quantile data layer."""

from .schema import (
    PWMExtremeBatchConfig,
    PWMExtremeConfig,
    PWMExtremeMonthResult,
    PWMExtremeResult,
    PWMExtremeServiceConfig,
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from .store import (
    ensure_pwm_extreme_tables,
    make_run_status_row,
    return_levels_to_rows,
    result_to_threshold_rows,
    upsert_pwm_extreme_run_status,
    upsert_pwm_extreme_return_levels,
    upsert_pwm_extreme_thresholds,
)

__all__ = [

    "PWMExtremeBatchConfig",
    "PWMExtremeConfig",
    "PWMExtremeMonthResult",
    "PWMExtremeResult",
    "PWMExtremeServiceConfig",
    "RUN_STATUS_DONE",
    "RUN_STATUS_ERROR",
    "ensure_pwm_extreme_tables",
    "fetch_pwm_convergence_grid_agg",
    "fetch_pwm_converged_grid_agg",
    "make_run_status_row",
    "return_levels_to_rows",
    "result_to_threshold_rows",
    "upsert_pwm_extreme_run_status",
    "upsert_pwm_extreme_return_levels",
    "upsert_pwm_extreme_thresholds",
]


def __getattr__(name: str):
    if name in {"fetch_pwm_convergence_grid_agg", "fetch_pwm_converged_grid_agg"}:
        from .reader import (
            fetch_pwm_convergence_grid_agg,
            fetch_pwm_converged_grid_agg,
        )

        return {
            "fetch_pwm_convergence_grid_agg": fetch_pwm_convergence_grid_agg,
            "fetch_pwm_converged_grid_agg": fetch_pwm_converged_grid_agg,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
