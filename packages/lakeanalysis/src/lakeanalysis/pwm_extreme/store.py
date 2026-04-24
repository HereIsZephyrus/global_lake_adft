"""DB adapter wrappers for PWM extreme outputs.

Re-exported from lakesource.pwm_extreme.store to avoid duplication.
"""

from __future__ import annotations

from lakesource.pwm_extreme.store import (  # noqa: F401
    ensure_pwm_extreme_tables,
    make_run_status_row,
    result_to_threshold_rows,
    upsert_pwm_extreme_run_status,
    upsert_pwm_extreme_thresholds,
)