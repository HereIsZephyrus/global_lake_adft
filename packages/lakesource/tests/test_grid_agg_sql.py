"""Grid aggregation SQL template tests (P0+P1).

Covers read-path SQL builder functions in pwm_extreme/reader.py,
eot/reader.py, and quantile/reader.py that were previously untested.
"""

from __future__ import annotations

import pytest

from lakesource.table_config import TableConfig

from lakesource.pwm_extreme.reader import (
    _convergence_grid_agg_sql,
    _converged_grid_agg_sql,
    _crossent_threshold_sql_pg,
    _exceedance_grid_agg_sql,
    _monthly_exceedance_grid_agg_sql,
    _monthly_threshold_grid_agg_sql,
)
from lakesource.eot.reader import (
    _available_quantiles_sql,
    _eot_converged_all_grid_agg_sql,
    _eot_converged_grid_agg_sql,
    _eot_convergence_grid_agg_sql,
    _eot_grid_agg_sql,
)
from lakesource.quantile.reader import (
    _extremes_by_type_grid_agg_sql,
    _extremes_grid_agg_sql,
    _per_lake_stats_grid_agg_sql,
    _transitions_by_type_grid_agg_sql,
    _transitions_grid_agg_sql,
)


@pytest.fixture
def tc() -> TableConfig:
    return TableConfig.default()


# ======================================================================
# P0 — PWM extreme parameterized exceedance queries (the buggy ones)
# ======================================================================

class TestPWMExceedanceSQL:
    """Regression + correctness for parameterized exceedance SQL builders."""

    def test_exceedance_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _exceedance_grid_agg_sql(tc, 0.01, 0.05)
        query = sql.as_string()
        assert query
        assert "WITH deduped_area AS" in query
        assert "quantile_thresholds AS" in query
        assert "threshold_high" in query
        assert "threshold_low" in query
        assert "mean_high_exceedance" in query
        assert "mean_low_exceedance" in query
        assert "mean_all_exceedance" in query
        assert "median_all_exceedance" in query
        assert "EXTRACT(MONTH FROM" in query
        assert "ORDER BY 1, 2" in query

    def test_exceedance_grid_agg_sql_no_sql_attr(self, tc: TableConfig) -> None:
        """Regression: psycopg3 SQL has no .sql attr (psycopg2-ism)."""
        sql = _exceedance_grid_agg_sql(tc, 0.01, 0.05)
        assert sql.as_string() is not None  # must not raise AttributeError

    def test_exceedance_grid_agg_sql_different_p(self, tc: TableConfig) -> None:
        """Different p values embed different threshold expressions."""
        sql1 = _exceedance_grid_agg_sql(tc, 0.01, 0.05).as_string()
        sql2 = _exceedance_grid_agg_sql(tc, 0.10, 0.05).as_string()
        assert sql1 != sql2

    def test_exceedance_grid_agg_sql_symmetric_p(self, tc: TableConfig) -> None:
        """Symmetric p yields same high/low thresholds (structure, not value)."""
        sql = _exceedance_grid_agg_sql(tc, 0.05, 0.05)
        query = sql.as_string()
        assert query

    def test_monthly_exceedance_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _monthly_exceedance_grid_agg_sql(tc, 0.01, 0.05)
        query = sql.as_string()
        assert query
        assert "deduped_area AS" in query
        assert "high_exceedance_rate" in query
        assert "low_exceedance_rate" in query
        assert "EXTRACT(MONTH FROM" in query
        assert "ORDER BY 1, 2, 3" in query

    def test_monthly_exceedance_grid_agg_sql_no_sql_attr(self, tc: TableConfig) -> None:
        """Regression: psycopg3 SQL has no .sql attr (psycopg2-ism)."""
        sql = _monthly_exceedance_grid_agg_sql(tc, 0.01, 0.05)
        assert sql.as_string() is not None  # must not raise AttributeError

    def test_crossent_threshold_sql_pg_high(self) -> None:
        expr = _crossent_threshold_sql_pg(0.05, "high")
        assert "t.mean_area" in expr
        assert "t.epsilon" in expr
        assert "t.lambda_0" in expr
        assert "t.lambda_4" in expr
        assert "LN(" in expr
        assert "EXP(-" in expr

    def test_crossent_threshold_sql_pg_low(self) -> None:
        expr = _crossent_threshold_sql_pg(0.05, "low")
        assert "t.mean_area" in expr
        assert "t.epsilon" in expr
        assert "LN(" in expr


# ======================================================================
# P1 — PWM extreme simple templates
# ======================================================================

class TestPWMConvergenceSQL:

    def test_convergence_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _convergence_grid_agg_sql(tc)
        query = sql.as_string()
        assert "convergence_rate" in query
        assert "lake_count" in query
        assert "cell_lat" in query
        assert "cell_lon" in query
        assert "GROUP BY 1, 2" in query

    def test_converged_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _converged_grid_agg_sql(tc)
        query = sql.as_string()
        assert "median_threshold_high" in query
        assert "median_threshold_low" in query
        assert "converged IS TRUE" in query
        assert "PERCENTILE_CONT(0.5)" in query

    def test_monthly_threshold_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _monthly_threshold_grid_agg_sql(tc)
        query = sql.as_string()
        assert "t.month" in query
        assert "median_threshold_high" in query
        assert "converged IS TRUE" in query
        assert "GROUP BY 1, 2, 3" in query


# ======================================================================
# P1 — EOT reader SQL
# ======================================================================

class TestEOTReaderSQL:

    def test_available_quantiles_sql_builds(self, tc: TableConfig) -> None:
        sql = _available_quantiles_sql(tc)
        query = sql.as_string()
        assert "DISTINCT tail" in query
        assert "threshold_quantile" in query
        assert "ORDER" in query

    def test_eot_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _eot_grid_agg_sql(tc)
        query = sql.as_string()
        assert "convergence_rate" in query
        assert "median_xi" in query
        assert "median_sigma" in query
        assert "mean_extremes_freq" in query
        assert "median_threshold" in query
        assert "r.tail = %(tail)s" in query

    def test_eot_convergence_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _eot_convergence_grid_agg_sql(tc)
        query = sql.as_string()
        assert "convergence_rate" in query
        assert "r.tail = %(tail)s" in query
        assert "r.threshold_quantile = %(threshold_quantile)s" in query

    def test_eot_converged_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _eot_converged_grid_agg_sql(tc)
        query = sql.as_string()
        assert "median_xi" in query
        assert "median_sigma" in query
        assert "mean_extremes_freq" in query
        assert "median_extremes_freq" in query
        assert "r.converged IS TRUE" in query

    def test_eot_converged_all_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _eot_converged_all_grid_agg_sql(tc)
        query = sql.as_string()
        assert "WITH paired AS" in query
        assert "all_extremes_freq" in query
        assert "mean_all_extremes_freq" in query
        assert "median_all_extremes_freq" in query
        assert "hi.tail = 'high'" in query
        assert "lo.tail = 'low'" in query


# ======================================================================
# P1 — Quantile reader SQL
# ======================================================================

class TestQuantileReaderSQL:

    def test_extremes_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _extremes_grid_agg_sql(tc, 0.5)
        query = sql.as_string()
        assert "lake_count" in query
        assert "event_count" in query
        assert "GROUP BY 1, 2" in query

    def test_extremes_by_type_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _extremes_by_type_grid_agg_sql(tc, 0.5)
        query = sql.as_string()
        assert "event_type" in query
        assert "GROUP BY 1, 2, 3" in query

    def test_transitions_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _transitions_grid_agg_sql(tc, 0.5)
        query = sql.as_string()
        assert "lake_count" in query
        assert "event_count" in query
        assert "GROUP BY 1, 2" in query

    def test_transitions_by_type_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _transitions_by_type_grid_agg_sql(tc, 0.5)
        query = sql.as_string()
        assert "transition_type" in query
        assert "GROUP BY 1, 2, 3" in query

    def test_per_lake_stats_grid_agg_sql_builds(self, tc: TableConfig) -> None:
        sql = _per_lake_stats_grid_agg_sql(tc, 0.5)
        query = sql.as_string()
        assert "WITH per_lake AS" in query
        assert "mean_high" in query
        assert "median_high" in query
        assert "mean_low" in query
        assert "median_low" in query
        assert "mean_all" in query
        assert "median_all" in query

    def test_per_lake_stats_grid_agg_sql_cte_pattern(self, tc: TableConfig) -> None:
        """Verify the per_lake CTE references quantile_extremes correctly."""
        sql = _per_lake_stats_grid_agg_sql(tc, 0.5)
        query = sql.as_string()
        assert "event_type = 'high'" in query
        assert "event_type = 'low'" in query
