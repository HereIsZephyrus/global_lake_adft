"""Integration tests for lake_quantile schema DDL and UPSERT (P0)."""

from __future__ import annotations

Q_WF = "monthly-transition-v1"


def _q_label_row(hylak_id=1, year=2020, month=1, wf=Q_WF, **kwargs) -> dict:
    row = {
        "hylak_id": hylak_id, "workflow_version": wf,
        "year": year, "month": month,
        "water_area": 100.0, "monthly_climatology": 90.0, "anomaly": 10.0,
        "q_low": 50.0, "q_high": 150.0, "extreme_label": "none",
    }
    row.update(kwargs)
    return row


def _q_transition_row(hylak_id=1, wf=Q_WF, **kwargs) -> dict:
    row = {
        "hylak_id": hylak_id, "workflow_version": wf,
        "from_year": 2020, "from_month": 1,
        "to_year": 2020, "to_month": 2,
        "transition_type": "none->high",
        "from_anomaly": 0.0, "to_anomaly": 70.0,
        "from_label": "none", "to_label": "high",
    }
    row.update(kwargs)
    return row


def _q_extreme_row(hylak_id=1, year=2020, month=1, event_type="high", wf=Q_WF) -> dict:
    return {
        "hylak_id": hylak_id, "workflow_version": wf,
        "year": year, "month": month, "event_type": event_type,
        "water_area": 160.0, "monthly_climatology": 90.0, "anomaly": 70.0,
        "threshold": 150.0,
    }


def _q_status_row(hylak_id=1, status="done", wf=Q_WF) -> dict:
    return {
        "hylak_id": hylak_id, "workflow_version": wf,
        "chunk_start": 0, "chunk_end": 1000,
        "status": status, "error_message": None,
    }


class TestQuantileTablesCreate:
    def test_ensure_creates_four_tables(self, provider) -> None:
        provider.ensure_table("quantile")
        from lakesource.table_config import TableConfig
        tc = TableConfig.default()
        with provider._conn() as conn:
            with conn.cursor() as cur:
                for key in [
                    "quantile_labels", "quantile_extremes",
                    "quantile_abrupt_transitions", "quantile_run_status",
                ]:
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                        [tc.series_table(key)],
                    )
                    assert cur.fetchone()[0] is True, f"{key} not created"

    def test_ensure_idempotent(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.ensure_table("quantile")  # no error


class TestQuantileLabelsUpsert:
    HID = 201

    def test_insert_4col_pk(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_labels", [_q_label_row(hylak_id=self.HID)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT extreme_label FROM quantile_labels "
                    "WHERE hylak_id = %s AND workflow_version = %s",
                    [self.HID, Q_WF],
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == "none"

    def test_on_conflict_4col_pk_update(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_labels", [_q_label_row(hylak_id=self.HID, water_area=100.0)])
        provider.upsert_rows("quantile_labels", [_q_label_row(hylak_id=self.HID, water_area=999.0)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT water_area FROM quantile_labels "
                    "WHERE hylak_id = %s AND workflow_version = %s",
                    [self.HID, Q_WF],
                )
                row = cur.fetchone()
        assert row is not None
        assert float(row[0]) == 999.0


class TestQuantileExtremesUpsert:
    HID = 301

    def test_insert_5col_pk(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_extremes", [_q_extreme_row(hylak_id=self.HID)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM quantile_extremes "
                    "WHERE hylak_id = %s AND workflow_version = %s",
                    [self.HID, Q_WF],
                )
                assert cur.fetchone()[0] == 1

    def test_two_event_types_same_month(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_extremes", [
            _q_extreme_row(hylak_id=self.HID, event_type="high"),
            _q_extreme_row(hylak_id=self.HID, event_type="low"),
        ])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM quantile_extremes "
                    "WHERE hylak_id = %s AND workflow_version = %s",
                    [self.HID, Q_WF],
                )
                assert cur.fetchone()[0] == 2


class TestQuantileTransitionsUpsert:
    HID = 401

    def test_insert_7col_pk(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_abrupt_transitions", [_q_transition_row(hylak_id=self.HID)])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT transition_type FROM quantile_abrupt_transitions "
                    "WHERE hylak_id = %s AND workflow_version = %s",
                    [self.HID, Q_WF],
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == "none->high"

    def test_on_conflict_7col_pk_update(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_abrupt_transitions", [
            _q_transition_row(hylak_id=self.HID, to_anomaly=10.0)
        ])
        provider.upsert_rows("quantile_abrupt_transitions", [
            _q_transition_row(hylak_id=self.HID, to_anomaly=999.0)
        ])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_anomaly FROM quantile_abrupt_transitions "
                    "WHERE hylak_id = %s AND workflow_version = %s",
                    [self.HID, Q_WF],
                )
                row = cur.fetchone()
        assert row is not None
        assert float(row[0]) == 999.0


class TestQuantileRunStatus:
    def test_upsert(self, provider) -> None:
        provider.ensure_table("quantile")
        provider.upsert_rows("quantile_run_status", [_q_status_row()])
        with provider._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM quantile_run_status WHERE status = 'done'"
                )
                assert cur.fetchone()[0] == 1
