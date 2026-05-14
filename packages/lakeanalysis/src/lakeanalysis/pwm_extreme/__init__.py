"""PWM extreme quantile estimation module.

Provides PWM + minimum cross-entropy extreme quantile estimation for
small-sample monthly lake area data, producing per-month anomaly thresholds
that can extrapolate beyond the empirical distribution.
"""

from .compute import (
    PWMExtremeConfig,
    assign_pwm_extreme_labels,
    compute_monthly_thresholds,
    compute_one_month_thresholds,
    compute_pooled_pwm_thresholds,
    compute_pwm_beta,
    crossent_quantile,
    shifted_exponential_prior,
    solve_lagrange_multipliers,
)
from .config import (
    PWMExtremeBatchConfig,
    PWMExtremeServiceConfig,
)
from .diagnostics import (
    pwm_constraint_residuals,
    quantile_function_curve,
)
from .evt_index import compute_evt_index_strengths
from .phi import map_strength_df_to_phi, map_strength_to_phi
from .plot_adapter import (
    plot_quantile_functions,
    plot_threshold_summary,
)
from .service import run_single_lake_service
from .store import (
    ensure_pwm_extreme_tables,
    make_run_status_row,
    result_to_threshold_rows,
    upsert_pwm_extreme_run_status,
    upsert_pwm_extreme_thresholds,
)
from lakesource.pwm_extreme.schema import (
    PWMExtremeMonthResult,
    PWMExtremeResult,
)


def __getattr__(name: str):
    if name in ("plot_pwm_extreme_quantile_functions", "plot_pwm_extreme_threshold_summary"):
        from lakeviz.pwm_extreme import (
            plot_pwm_extreme_quantile_functions,
            plot_pwm_extreme_threshold_summary,
        )
        return {
            "plot_pwm_extreme_quantile_functions": plot_pwm_extreme_quantile_functions,
            "plot_pwm_extreme_threshold_summary": plot_pwm_extreme_threshold_summary,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# pylint: disable=undefined-all-variable
# E0603: plot_* names are resolved lazily via __getattr__ above
__all__ = [
    "PWMExtremeBatchConfig",
    "PWMExtremeConfig",
    "PWMExtremeMonthResult",
    "PWMExtremeResult",
    "PWMExtremeServiceConfig",
    "assign_pwm_extreme_labels",
    "compute_monthly_thresholds",
    "compute_one_month_thresholds",
    "compute_pooled_pwm_thresholds",
    "compute_pwm_beta",
    "crossent_quantile",
    "compute_evt_index_strengths",
    "ensure_pwm_extreme_tables",
    "map_strength_df_to_phi",
    "map_strength_to_phi",
    "make_run_status_row",
    "plot_pwm_extreme_quantile_functions",
    "plot_pwm_extreme_threshold_summary",
    "plot_quantile_functions",
    "plot_threshold_summary",
    "pwm_constraint_residuals",
    "quantile_function_curve",
    "result_to_threshold_rows",
    "run_single_lake_service",
    "shifted_exponential_prior",
    "solve_lagrange_multipliers",
    "upsert_pwm_extreme_run_status",
    "upsert_pwm_extreme_thresholds",
]
