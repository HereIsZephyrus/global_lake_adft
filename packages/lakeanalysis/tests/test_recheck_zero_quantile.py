"""Tests for zero-quantile maintenance workflows."""

from __future__ import annotations

from lakeanalysis.quality.filters import FLAG_ZERO_QUANTILE
from lakeanalysis.quality.maintenance_runner import _clear_zero_quantile_flag


class _Cursor:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount
        self.executed: list[tuple[str, list[object]]] = []

    def execute(self, sql: str, params: list[object]) -> None:
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _Conn:
    def __init__(self, rowcount: int = 0) -> None:
        self.cursor_obj = _Cursor(rowcount=rowcount)
        self.commits = 0

    def cursor(self) -> _Cursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


def test_clear_zero_quantile_flag_updates_rows_in_place() -> None:
    conn = _Conn(rowcount=2)

    updated = _clear_zero_quantile_flag(conn, [101, 202])

    assert updated == 2
    assert conn.commits == 1
    assert conn.cursor_obj.executed == [
        (
            "UPDATE area_anomalies SET anomaly_flags = anomaly_flags & ~%s WHERE hylak_id = ANY(%s)",
            [FLAG_ZERO_QUANTILE, [101, 202]],
        )
    ]


def test_clear_zero_quantile_flag_skips_empty_input() -> None:
    conn = _Conn(rowcount=99)

    updated = _clear_zero_quantile_flag(conn, [])

    assert updated == 0
    assert conn.commits == 0
    assert conn.cursor_obj.executed == []
