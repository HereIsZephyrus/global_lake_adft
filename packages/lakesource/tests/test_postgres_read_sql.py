"""Postgres read-path SQL builder tests (P1).

Covers SQL builders in lake_info_read.py and frozen_read.py that were
previously untested.
"""

from __future__ import annotations

import pytest

from lakesource.table_config import TableConfig
from lakesource.postgres.lake_info_read import (
    _fetch_seasonal_amplitude_chunk_sql,
    _fetch_linear_trend_by_ids_sql,
    _fetch_max_lake_info_hylak_id_sql,
    _count_source_hylak_ids_in_range_sql,
    _fetch_source_hylak_ids_in_range_sql,
)
from lakesource.postgres.frozen_read import (
    _fetch_frozen_year_months_by_ids_sql,
    _fetch_frozen_year_months_chunk_sql,
)


@pytest.fixture
def tc() -> TableConfig:
    return TableConfig.default()


class TestLakeInfoReadSQL:
    def test_fetch_max_hylak_id(self, tc: TableConfig) -> None:
        sql = _fetch_max_lake_info_hylak_id_sql(tc)
        query = sql.as_string()
        assert "SELECT MAX(hylak_id)" in query

    def test_fetch_seasonal_amplitude_chunk(self, tc: TableConfig) -> None:
        sql = _fetch_seasonal_amplitude_chunk_sql(tc)
        query = sql.as_string()
        assert "annual_means_std" in query
        assert "mean_area" in query
        assert "hylak_id" in query

    def test_fetch_linear_trend_by_ids(self, tc: TableConfig) -> None:
        sql = _fetch_linear_trend_by_ids_sql(tc)
        query = sql.as_string()
        assert "linear_trend_of_stl_trend_per_period" in query
        assert "ANY(%(id_list)s)" in query

    def test_count_source_in_range(self, tc: TableConfig) -> None:
        sql = _count_source_hylak_ids_in_range_sql(tc)
        query = sql.as_string()
        assert "SELECT COUNT(*)" in query

    def test_fetch_source_in_range(self, tc: TableConfig) -> None:
        sql = _fetch_source_hylak_ids_in_range_sql(tc)
        query = sql.as_string()
        assert "SELECT hylak_id" in query


class TestFrozenReadSQL:
    def test_fetch_frozen_by_ids(self, tc: TableConfig) -> None:
        sql = _fetch_frozen_year_months_by_ids_sql(tc)
        query = sql.as_string()
        assert "anomaly_type = 'frozen'" in query
        assert "ANY(%(id_list)s)" in query

    def test_fetch_frozen_chunk(self, tc: TableConfig) -> None:
        sql = _fetch_frozen_year_months_chunk_sql(tc)
        query = sql.as_string()
        assert "anomaly_type = 'frozen'" in query
        assert "hylak_id" in query
