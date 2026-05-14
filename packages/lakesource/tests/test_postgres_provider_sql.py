"""SQL safety tests for PostgresLakeProvider table-name handling (P0).

Validates that table names are validated and properly quoted via
psycopg.sql.Identifier, replacing the old f-string injection pattern.
"""

from __future__ import annotations

import pytest
from psycopg import sql as psql

from lakesource.provider.postgres_provider import _table_ident, _SAFE_TABLE_NAME


def test_table_ident_valid_plain_name() -> None:
    ident = _table_ident("lake_area")
    sql = psql.SQL("SELECT * FROM {}").format(ident)
    query = sql.as_string()
    assert 'SELECT * FROM "lake_area"' in query


def test_table_ident_valid_with_underscores() -> None:
    ident = _table_ident("quantile_run_status")
    sql = psql.SQL("DELETE FROM {}").format(ident)
    query = sql.as_string()
    assert '"quantile_run_status"' in query


def test_table_ident_rejects_semicolon_injection() -> None:
    with pytest.raises(ValueError, match="Invalid table name"):
        _table_ident("lake_area; DROP TABLE quantile_labels;")


def test_table_ident_rejects_dash() -> None:
    with pytest.raises(ValueError, match="Invalid table name"):
        _table_ident("lake-area")


def test_table_ident_rejects_space() -> None:
    with pytest.raises(ValueError, match="Invalid table name"):
        _table_ident("lake area")


def test_table_ident_rejects_quotes() -> None:
    with pytest.raises(ValueError, match="Invalid table name"):
        _table_ident('lake_area" DROP TABLE x--')


def test_table_ident_rejects_empty_string() -> None:
    with pytest.raises(ValueError, match="Invalid table name"):
        _table_ident("")


def test_safe_table_name_regex_accepts_common_names() -> None:
    valid = [
        "lake_area",
        "area_quality",
        "quantile_labels",
        "eot_results",
        "eot_hawkes_lrt",
        "comparison_run_status",
        "pwm_extreme_thresholds",
        "interpolation_detect",
        "area_shift_labels",
        "quality_run_status",
        "lake_info",
        "frozen_anomaly",
        "area_anomalies",
        "pwm_hawkes_results",
        "pwm_hawkes_transition_monthly",
        "eot_hawkes_results",
        "eot_hawkes_transition_monthly",
        "quantile_extremes",
        "quantile_abrupt_transitions",
        "entropy",
        "area_entropy_cv",
        "lake_pfaf",
        "af_nearest",
    ]
    for name in valid:
        assert _SAFE_TABLE_NAME.match(name), f"{name!r} should be valid"
