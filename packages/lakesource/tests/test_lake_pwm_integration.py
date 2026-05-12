"""Integration tests for lake_pwm schema DDL and UPSERT (P0).

Validates the full code path: Provider dispatch → Backend → Repository → SQL.
"""

from __future__ import annotations

import psycopg
import pytest

WF = "pwm-extreme-v1"


def _threshold_row(
    hylak_id: int = 1,
    month: int = 1,
    wf: str = WF,
    mean_area: float = 100.0,
    epsilon: float = 0.0,
) -> dict:
    return {
        "hylak_id": hylak_id,
        "month": month,
        "mean_area": mean_area,
        "epsilon": epsilon,
        "lambda_0": 1.0, "lambda_1": 2.0, "lambda_2": 3.0, "lambda_3": 4.0, "lambda_4": 5.0,
        "b_0": 0.1, "b_1": 0.2, "b_2": 0.3, "b_3": 0.4, "b_4": 0.5,
        "threshold_high": 200.0,
        "threshold_low": 50.0,
        "converged": True,
        "objective_value": 0.123,
    }


def _label_row(
    hylak_id: int = 1,
    year: int = 2020,
    month: int = 1,
    wf: str = WF,
) -> dict:
    return {
        "hylak_id": hylak_id,
        "year": year,
        "month": month,
        "water_area": 95.0,
        "threshold_low": 50.0,
        "threshold_high": 200.0,
        "extreme_label": "none",
    }


def _extreme_row(
    hylak_id: int = 1,
    year: int = 2020,
    month: int = 1,
    wf: str = WF,
) -> dict:
    return {
        "hylak_id": hylak_id,
        "year": year,
        "month": month,
        "event_type": "high",
        "water_area": 210.0,
        "threshold": 200.0,
        "severity": 10.0,
        "extreme_label": "high",
    }


def _transition_row(
    hylak_id: int = 1,
    from_year: int = 2020,
    from_month: int = 1,
    to_year: int = 2020,
    to_month: int = 2,
    wf: str = WF,
    **kwargs,
) -> dict:
    row = {
        "hylak_id": hylak_id,
        "from_year": from_year,
        "from_month": from_month,
        "to_year": to_year,
        "to_month": to_month,
        "transition_type": "none->high",
        "from_water_area": 95.0,
        "to_water_area": 210.0,
        "from_label": "none",
        "to_label": "high",
    }
    row.update(kwargs)
    return row


def _run_status_row(
    hylak_id: int = 1,
    status: str = "done",
    wf: str = WF,
    **kwargs,
) -> dict:
    row = {
        "hylak_id": hylak_id,
        "chunk_start": 0,
        "chunk_end": 1000,
        "status": status,
        "error_message": None,
    }
    row.update(kwargs)
    return row


def _hawkes_run_status_row(
    hylak_id: int = 1,
    status: str = "done",
    wf: str = WF,
) -> dict:
    return {
        "hylak_id": hylak_id,
        "chunk_start": 0,
        "chunk_end": 1000,
        "status": status,
        "error_message": None,
    }


class TestPWMTablesCreate:
    def test_ensure_pwm_tables_creates_all_six(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        from lakesource.table_config import TableConfig
        tc = TableConfig.default()
        with provider._conn() as conn:
            with conn.cursor() as cur:
                for table_key in [
                    "pwm_extreme_thresholds",
                    "pwm_extreme_labels",
                    "pwm_extreme_extremes",
                    "pwm_extreme_abrupt_transitions",
                    "pwm_extreme_run_status",
                    "pwm_hawkes_run_status",
                ]:
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                        [tc.series_table(table_key)],
                    )
                    assert cur.fetchone()[0] is True, f"{table_key} not created"

    def test_ensure_pwm_tables_idempotent(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.ensure_table("pwm_extreme")  # no error


class TestPWMThresholdsUpsert:
    def test_insert_new_row(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_thresholds", [_threshold_row()])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT hylak_id, month, lambda_0, lambda_4, b_0, b_4, converged "
                    "FROM pwm_extreme_thresholds "
                    "WHERE hylak_id = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == 1
        assert row[2] == 1.0
        assert row[3] == 5.0
        assert row[4] == 0.1
        assert row[5] == 0.5
        assert row[6] is True

    def test_on_conflict_updates_row(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_thresholds", [_threshold_row(mean_area=100.0)])
        provider.upsert_rows("pwm_extreme_thresholds", [_threshold_row(mean_area=999.0)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT mean_area FROM pwm_extreme_thresholds "
                    "WHERE hylak_id = 1 AND month = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert float(row[0]) == 999.0

    def test_multiple_rows_bulk_insert(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        rows = [_threshold_row(month=m) for m in range(1, 13)]
        provider.upsert_rows("pwm_extreme_thresholds", rows)
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM pwm_extreme_thresholds "
                    "WHERE hylak_id = 1",
                )
                assert cur.fetchone()[0] == 12

    def test_empty_rows_noop(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_thresholds", [])  # no error


class TestPWMLabelsUpsert:
    def test_insert_and_readback(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_labels", [_label_row()])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT extreme_label FROM pwm_extreme_labels "
                    "WHERE hylak_id = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == "none"


class TestPWMExtremesUpsert:
    def test_insert_and_readback(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_extremes", [_extreme_row()])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT event_type, severity FROM pwm_extreme_extremes "
                    "WHERE hylak_id = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == "high"
        assert float(row[1]) == 10.0


class TestPWMTransitionsUpsert:
    def test_insert_with_6col_pk(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_abrupt_transitions", [_transition_row()])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT transition_type FROM pwm_extreme_abrupt_transitions "
                    "WHERE hylak_id = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == "none->high"

    def test_on_conflict_update(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_abrupt_transitions", [_transition_row(to_water_area=100.0)])
        provider.upsert_rows("pwm_extreme_abrupt_transitions", [_transition_row(to_water_area=999.0)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_water_area FROM pwm_extreme_abrupt_transitions "
                    "WHERE hylak_id = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert float(row[0]) == 999.0


class TestPWMRunStatus:
    def test_upsert_and_count(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_run_status", [_run_status_row()])
        # count via raw query (not through Provider's count method)
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM pwm_extreme_run_status "
                    "WHERE status = 'done'",
                )
                assert cur.fetchone()[0] == 1

    def test_upsert_error_status(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_extreme_run_status", [
            _run_status_row(status="error", error_message="timeout")
        ])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, error_message FROM pwm_extreme_run_status "
                    "WHERE hylak_id = 1",
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == "error"
        assert row[1] == "timeout"


class TestPWMHawkesRunStatus:
    def test_upsert(self, provider) -> None:
        provider.ensure_table("pwm_extreme")
        provider.upsert_rows("pwm_hawkes_run_status", [_hawkes_run_status_row()])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM pwm_hawkes_run_status "
                    "WHERE hylak_id = 1",
                )
                assert cur.fetchone()[0] == 1


class TestPWMDispatchMapping:
    """Verify that the string dispatch tables map correctly."""

    def test_ensure_dispatch_pwm_keys_exist(self, provider) -> None:
        from lakesource.provider.postgres_provider import _ENSURE_DISPATCH
        assert "pwm_extreme" in _ENSURE_DISPATCH
        assert "pwm_extreme_thresholds" in _ENSURE_DISPATCH
        assert "pwm_extreme_labels" in _ENSURE_DISPATCH
        assert "pwm_extreme_extremes" in _ENSURE_DISPATCH
        assert "pwm_extreme_abrupt_transitions" in _ENSURE_DISPATCH
        assert "pwm_extreme_run_status" in _ENSURE_DISPATCH
        assert "pwm_hawkes_run_status" in _ENSURE_DISPATCH

    def test_upsert_dispatch_pwm_keys_exist(self, provider) -> None:
        from lakesource.provider.postgres_provider import _UPSERT_DISPATCH
        assert "pwm_extreme_thresholds" in _UPSERT_DISPATCH
        assert "pwm_extreme_labels" in _UPSERT_DISPATCH
        assert "pwm_extreme_extremes" in _UPSERT_DISPATCH
        assert "pwm_extreme_abrupt_transitions" in _UPSERT_DISPATCH
        assert "pwm_extreme_run_status" in _UPSERT_DISPATCH
        assert "pwm_hawkes_run_status" in _UPSERT_DISPATCH
