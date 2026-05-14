"""DB adapter wrappers for PWM extreme outputs.

Re-exported from lakesource.pwm.store to avoid duplication.
"""

from __future__ import annotations

from lakesource.pwm.store import (  # noqa: F401
    ensure_pwm_extreme_tables,
    make_run_status_row,
    return_levels_to_rows,
    result_to_threshold_rows,
    upsert_pwm_extreme_run_status,
    upsert_pwm_extreme_return_levels,
    upsert_pwm_extreme_thresholds,
)
