"""Integration tests for lake_eot schema DDL, UPSERT, and Decimal type handling (P0)."""

from __future__ import annotations

import pytest

EOT_WF = "monthly-transition-v1"


def _eot_result_row(hylak_id=1, tail="high", quantile=0.95, wf=EOT_WF, **kwargs) -> dict:
    row = {
        "hylak_id": hylak_id, "tail": tail,
        "threshold_quantile": quantile,
        "converged": True, "log_likelihood": -123.45, "threshold": 150.0,
        "n_extremes": 12, "n_observations": 240, "n_frozen_months": 5,
        "beta0": 10.0, "beta1": 0.5, "sin_1": 0.1, "cos_1": 0.2,
        "sigma": 1.5, "xi": 0.1, "error_message": None,
    }
    row.update(kwargs)
    return row


def _eot_extreme_row(
    hylak_id=1, tail="high", quantile=0.95, cluster_id=1, wf=EOT_WF,
) -> dict:
    return {
        "hylak_id": hylak_id, "tail": tail,
        "threshold_quantile": quantile, "cluster_id": cluster_id,
        "cluster_size": 3, "year": 2020, "month": 1,
        "water_area": 160.0, "threshold_at_event": 150.0,
    }


def _eot_status_row(hylak_id=1, status="done", wf=EOT_WF) -> dict:
    return {
        "hylak_id": hylak_id,
        "chunk_start": 0, "chunk_end": 1000,
        "status": status, "error_message": None,
    }


class TestEOTTablesCreate:
    def test_ensure_creates_three_tables(self, provider) -> None:
        provider.ensure_table("eot")
        from lakesource.table_config import TableConfig
        tc = TableConfig.default()
        with provider._conn() as conn:
            with conn.cursor() as cur:
                for key in ["eot_results", "eot_extremes", "eot_run_status"]:
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                        [tc.series_table(key)],
                    )
                    assert cur.fetchone()[0] is True, f"{key} not created"

    def test_ensure_idempotent(self, provider) -> None:
        provider.ensure_table("eot")
        provider.ensure_table("eot")


class TestEOTResultsUpsert:
    HID = 501

    def test_insert_3col_pk(self, provider) -> None:
        provider.ensure_table("eot")
        provider.upsert_rows("eot_results", [_eot_result_row(hylak_id=self.HID)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT converged, xi FROM eot_results "
                    "WHERE hylak_id = %s AND tail = 'high'",
                    [self.HID],
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] is True
        assert float(row[1]) == 0.1

    def test_two_tails_same_lake(self, provider) -> None:
        provider.ensure_table("eot")
        provider.upsert_rows("eot_results", [
            _eot_result_row(hylak_id=self.HID, tail="high"),
            _eot_result_row(hylak_id=self.HID, tail="low"),
        ])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM eot_results WHERE hylak_id = %s",
                    [self.HID],
                )
                assert cur.fetchone()[0] == 2

    def test_on_conflict_update(self, provider) -> None:
        provider.ensure_table("eot")
        provider.upsert_rows("eot_results", [_eot_result_row(hylak_id=self.HID, xi=0.1)])
        provider.upsert_rows("eot_results", [_eot_result_row(hylak_id=self.HID, xi=0.9)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT xi FROM eot_results WHERE hylak_id = %s AND tail = 'high' "
                    "AND threshold_quantile = 0.95",
                    [self.HID],
                )
                row = cur.fetchone()
        assert row is not None
        assert float(row[0]) == 0.9


class TestEOTDecimalType:
    HID = 601

    def test_threshold_quantile_roundtrip(self, provider) -> None:
        """NUMERIC(5,4) preserves 4 decimal places."""
        provider.ensure_table("eot")
        provider.upsert_rows("eot_results", [_eot_result_row(hylak_id=self.HID, quantile=0.9500)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT threshold_quantile FROM eot_results "
                    "WHERE hylak_id = %s AND tail = 'high'",
                    [self.HID],
                )
                row = cur.fetchone()
        assert row is not None
        assert str(row[0]) in ("0.9500", "0.95")

    def test_threshold_quantile_high_precision(self, provider) -> None:
        """Value with full precision up to 4 decimals."""
        provider.ensure_table("eot")
        provider.upsert_rows("eot_results", [_eot_result_row(hylak_id=self.HID, quantile=0.9876)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT threshold_quantile FROM eot_results "
                    "WHERE hylak_id = %s AND tail = 'high' "
                    "AND threshold_quantile = 0.9876",
                    [self.HID],
                )
                row = cur.fetchone()
        assert row is not None
        val = float(row[0])
        assert abs(val - 0.9876) < 1e-4


class TestEOTExtremesUpsert:
    HID = 701

    def test_insert_4col_pk(self, provider) -> None:
        provider.ensure_table("eot")
        provider.upsert_rows("eot_extremes", [_eot_extreme_row(hylak_id=self.HID)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cluster_size FROM eot_extremes WHERE hylak_id = %s",
                    [self.HID],
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == 3


class TestEOTRunStatus:
    HID = 801

    def test_upsert(self, provider) -> None:
        provider.ensure_table("eot")
        provider.upsert_rows("eot_run_status", [_eot_status_row(hylak_id=self.HID)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM eot_run_status "
                    "WHERE hylak_id = %s AND status = 'done'",
                    [self.HID],
                )
                assert cur.fetchone()[0] == 1
