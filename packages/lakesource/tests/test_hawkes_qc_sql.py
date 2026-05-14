"""Hawkes QC SQL builder tests (P0).

Covers read-only SELECT query builders in postgres/hawkes_qc.py that
were previously untested.  Follows the same pattern as test_grid_agg_sql.py.
"""

from __future__ import annotations

import pytest
from psycopg import sql as psql

from lakesource.table_config import TableConfig
from lakesource.postgres.hawkes_qc import (
    _hawkes_qc_summary_by_quantile_sql,
    _hawkes_error_message_counts_sql,
    _hawkes_results_select,
    _hawkes_lrt_select,
    _hawkes_lrt_summary_by_test_sql,
    _eot_hawkes_coverage_sql,
    _hawkes_transition_monthly_select,
    _append_quantile_filter,
)


@pytest.fixture
def tc() -> TableConfig:
    return TableConfig.default()


class TestHawkesQCSummaryByQuantile:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _hawkes_qc_summary_by_quantile_sql(tc)
        query = sql.as_string()
        assert "threshold_quantile" in query
        assert "qc_pass_rate" in query
        assert "converged_rate" in query
        assert "no_error_message_rate" in query
        assert "GROUP BY threshold_quantile" in query
        assert "ORDER BY threshold_quantile" in query

    def test_no_fstring_injection(self, tc: TableConfig) -> None:
        sql = _hawkes_qc_summary_by_quantile_sql(tc)
        query = sql.as_string()
        assert query  # no error building


class TestHawkesErrorCounts:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _hawkes_error_message_counts_sql(tc)
        query = sql.as_string()
        assert "LEFT(COALESCE(error_message" in query
        assert "LIMIT %(limit)s" in query
        assert "error_prefix" in query

    def test_param_placeholder(self, tc: TableConfig) -> None:
        sql = _hawkes_error_message_counts_sql(tc)
        query = sql.as_string()
        assert "%(limit)s" in query


class TestHawkesResultsSelect:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _hawkes_results_select(tc)
        query = sql.as_string()
        assert "mu_d" in query
        assert "mu_w" in query
        assert "alpha_dd" in query
        assert "alpha_dw" in query
        assert "alpha_wd" in query
        assert "alpha_ww" in query
        assert "beta_dd" in query
        assert "spectral_radius" in query
        assert "lrt_p_d_to_w" in query
        assert "lrt_p_w_to_d" in query
        assert "qc_pass" in query
        assert "qc_exceedance_rate" in query
        assert "qc_relative_amplitude" in query
        assert "qc_median_excess" in query
        assert "computed_at" in query


class TestHawkesLRTSelect:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _hawkes_lrt_select(tc)
        query = sql.as_string()
        assert "lr_statistic" in query
        assert "p_value" in query
        assert "significance_level" in query
        assert "reject_null" in query
        assert "restricted_log_likelihood" in query
        assert "full_log_likelihood" in query


class TestHawkesLRTSummary:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _hawkes_lrt_summary_by_test_sql(tc)
        query = sql.as_string()
        assert "test_name" in query
        assert "reject_null_rate" in query
        assert "mean_p_value" in query
        assert "AVG(p_value)" in query
        assert "MAX(p_value)" in query
        assert "WHERE 1=1" in query


class TestEOTHawkesCoverage:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _eot_hawkes_coverage_sql(tc)
        query = sql.as_string()
        assert "WITH eot_counts AS" in query
        assert "COUNT(DISTINCT tail)" in query
        assert "hawkes_without_eot_row" in query
        assert "hawkes_with_any_eot" in query
        assert "hawkes_with_both_eot_tails" in query
        assert "frac_both_eot_tails" in query
        assert "LEFT JOIN eot_counts ec" in query


class TestHawkesTransitionMonthlySelect:
    def test_builds(self, tc: TableConfig) -> None:
        sql = _hawkes_transition_monthly_select(tc)
        query = sql.as_string()
        assert "threshold_quantile" in query
        assert "score_raw" in query
        assert "score_norm" in query
        assert "significance_quantile" in query
        assert "significance_threshold" in query
        assert "significant" in query
        assert "direction" in query


class TestAppendQuantileFilter:
    def test_appends_filter(self) -> None:
        base = psql.SQL("WHERE 1=1")
        params: dict = {}
        result = _append_quantile_filter(base, params, 0.95)
        r = result.as_string()
        assert "AND threshold_quantile = %(threshold_quantile)s" in r
        assert params["threshold_quantile"] == 0.95

    def test_none_returns_unchanged(self) -> None:
        base = psql.SQL("WHERE 1=1")
        params: dict = {}
        result = _append_quantile_filter(base, params, None)
        assert result is base
        assert not params

    def test_composes_with_other_filters(self) -> None:
        base = psql.SQL("WHERE 1=1")
        params: dict = {}
        result = _append_quantile_filter(base, params, 0.95)
        result = _append_quantile_filter(result, params, None)
        r = result.as_string()
        assert "AND threshold_quantile = %(threshold_quantile)s" in r
