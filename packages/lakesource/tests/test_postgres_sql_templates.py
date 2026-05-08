"""Tests for SQL templates - validates SQL syntax without database connection."""

from __future__ import annotations

import pytest
from lakesource.postgres.area_quality import (
    _upsert_area_quality_sql,
    _upsert_area_anomalies_sql,
    _upsert_quality_run_status_sql,
)
from lakesource.postgres.lake_eot import _upsert_eot_results_sql
from lakesource.postgres.lake_quantile import _upsert_quantile_labels_sql
from lakesource.postgres.lake_pwm import _upsert_pwm_extreme_thresholds_sql
from lakesource.postgres.lake_hawkes import _upsert_hawkes_results_sql
from lakesource.postgres.lake_entropy import _upsert_entropy_sql
from lakesource.postgres.lake_misc import _upsert_comparison_run_status_sql
from lakesource.table_config import TableConfig


@pytest.fixture
def table_config() -> TableConfig:
    return TableConfig.default()


class TestUpsertSQLTemplates:
    def test_upsert_area_quality_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_area_quality_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query

    def test_upsert_area_anomalies_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_area_anomalies_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "anomaly_flags" in query

    def test_upsert_quality_run_status_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_quality_run_status_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "chunk_start" in query
        assert "chunk_end" in query
        assert "status" in query

    def test_upsert_eot_results_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_eot_results_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "beta0" in query
        assert "xi" in query

    def test_upsert_quantile_labels_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_quantile_labels_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query

    def test_upsert_pwm_thresholds_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_pwm_extreme_thresholds_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "lambda_" in query

    def test_upsert_hawkes_results_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_hawkes_results_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "mu_d" in query
        assert "mu_w" in query

    def test_upsert_entropy_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_entropy_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "ae_overall" in query
        assert "sens_slope" in query

    def test_upsert_comparison_run_status_sql_builds(self, table_config: TableConfig) -> None:
        sql = _upsert_comparison_run_status_sql(table_config)
        query = sql.as_string()
        assert "INSERT INTO" in query
        assert "VALUES" in query
        assert "ON CONFLICT" in query
        assert "now()" in query
        assert "EXCLUDED." in query
        assert "status" in query
