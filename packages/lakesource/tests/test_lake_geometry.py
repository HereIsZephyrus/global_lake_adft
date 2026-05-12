"""Lake geometry SQL builder tests (P1).

Tests environment-variable resolution, table name validation, and
psycopg.sql.Identifier composition.
"""

from __future__ import annotations

import pytest
from psycopg import sql

from lakesource.table_config import TableConfig
from lakesource.postgres.lake_geometry import (
    _lake_geometry_table_sql_ident,
    _validate_sql_identifier,
)


@pytest.fixture
def tc() -> TableConfig:
    return TableConfig.default()


class TestValidateSQLIdentifier:
    def test_valid_simple(self) -> None:
        assert _validate_sql_identifier("my_table", "test") == "my_table"

    def test_valid_leading_underscore(self) -> None:
        assert _validate_sql_identifier("_private_table", "test") == "_private_table"

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="must match"):
            _validate_sql_identifier("bad;DROP", "test")

    def test_rejects_space(self) -> None:
        with pytest.raises(ValueError, match="must match"):
            _validate_sql_identifier("bad table", "test")

    def test_rejects_dash(self) -> None:
        with pytest.raises(ValueError, match="must match"):
            _validate_sql_identifier("bad-table", "test")

    def test_rejects_leading_digit(self) -> None:
        with pytest.raises(ValueError, match="must match"):
            _validate_sql_identifier("123bad", "test")

    def test_rejects_dot(self) -> None:
        with pytest.raises(ValueError, match="must match"):
            _validate_sql_identifier("schema.table", "test")


class TestLakeGeometryTableSQLIdent:
    def test_fallback_to_config(self, tc: TableConfig) -> None:
        ident = _lake_geometry_table_sql_ident(tc)
        q = sql.SQL("SELECT * FROM {}").format(ident).as_string()
        expected = tc.atlas_table("lake_geometry")
        assert f'"{expected}"' in q

    def test_env_var_simple_table(self, monkeypatch, tc: TableConfig) -> None:
        monkeypatch.setenv("LAKE_GEOMETRY_TABLE", "my_geometry")
        ident = _lake_geometry_table_sql_ident(tc)
        q = sql.SQL("SELECT * FROM {}").format(ident).as_string()
        assert '"my_geometry"' in q

    def test_env_var_schema_table(self, monkeypatch, tc: TableConfig) -> None:
        monkeypatch.setenv("LAKE_GEOMETRY_TABLE", "myschema.mytable")
        ident = _lake_geometry_table_sql_ident(tc)
        q = sql.SQL("SELECT * FROM {}").format(ident).as_string()
        assert '"myschema"."mytable"' in q

    def test_env_var_rejects_invalid_table(self, monkeypatch, tc: TableConfig) -> None:
        monkeypatch.setenv("LAKE_GEOMETRY_TABLE", "bad;DROP")
        with pytest.raises(ValueError, match="must match"):
            _lake_geometry_table_sql_ident(tc)

    def test_env_var_rejects_too_many_parts(self, monkeypatch, tc: TableConfig) -> None:
        monkeypatch.setenv("LAKE_GEOMETRY_TABLE", "a.b.c")
        with pytest.raises(ValueError, match="must be 'table' or 'schema.table'"):
            _lake_geometry_table_sql_ident(tc)

    def test_env_var_empty_strips_and_falls_back(self, monkeypatch, tc: TableConfig) -> None:
        monkeypatch.setenv("LAKE_GEOMETRY_TABLE", "   ")
        ident = _lake_geometry_table_sql_ident(tc)
        q = sql.SQL("SELECT * FROM {}").format(ident).as_string()
        expected = tc.atlas_table("lake_geometry")
        assert f'"{expected}"' in q
