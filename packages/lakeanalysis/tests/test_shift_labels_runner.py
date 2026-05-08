"""Tests for lakeanalysis.quality.shift_labels_runner."""

from __future__ import annotations


import pandas as pd

from lakeanalysis.quality import FLAG_SHIFT
from lakeanalysis.quality.shift_labels_runner import sync_shift_to_anomalies


class MockProvider:
    def __init__(self, labels_df: pd.DataFrame, statuses: dict[int, tuple[str, int]]) -> None:
        self._labels_df = labels_df
        self._statuses = statuses
        self._area_quality: list[dict] = []
        self._area_anomalies: list[dict] = []
        self._upsert_calls: list[tuple[str, list[dict]]] = []
        self._delete_calls: list[tuple[str, list[int]]] = []
        self._flag_updates: list[tuple[int, int]] = []

    def _read_table_df(self, table_name: str) -> pd.DataFrame:
        if table_name == "area_shift_labels":
            return self._labels_df
        return pd.DataFrame()

    def fetch_area_statuses(self) -> dict[int, tuple[str, int]]:
        return self._statuses.copy()

    def upsert_rows(self, table_name: str, rows: list[dict]) -> None:
        self._upsert_calls.append((table_name, rows))
        if table_name == "area_quality":
            for r in rows:
                self._area_quality.append(r)
        elif table_name == "area_anomalies":
            for r in rows:
                self._area_anomalies.append(r)

    def delete_ids(self, table_name: str, hylak_ids: list[int]) -> None:
        self._delete_calls.append((table_name, hylak_ids))
        if table_name == "area_quality":
            self._area_quality = [r for r in self._area_quality if r["hylak_id"] not in hylak_ids]
        elif table_name == "area_anomalies":
            self._area_anomalies = [r for r in self._area_anomalies if r["hylak_id"] not in hylak_ids]

    def update_area_anomaly_flags(self, updates: list[tuple[int, int]]) -> None:
        self._flag_updates.extend(updates)

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict]:
        if table_name == "area_quality":
            return [r for r in self._area_quality if chunk_start <= r["hylak_id"] < chunk_end]
        if table_name == "area_anomalies":
            return [r for r in self._area_anomalies if chunk_start <= r["hylak_id"] < chunk_end]
        return []


class TestSyncShiftToAnomalies:
    def test_degraded_in_quality_moves_to_anomalies(self) -> None:
        labels_df = pd.DataFrame([
            {"hylak_id": 1, "shift_label": "degraded"},
        ])
        statuses = {1: ("quality", 0)}
        provider = MockProvider(labels_df, statuses)
        provider._area_quality = [
            {"hylak_id": 1, "rs_area_mean": 100.0, "rs_area_median": 90.0, "atlas_area": 200.0},
        ]

        sync_shift_to_anomalies(provider)

        delete_calls = [(t, ids) for t, ids in provider._delete_calls if t == "area_quality"]
        assert delete_calls == [("area_quality", [1])]

        upsert_calls = [(t, rows) for t, rows in provider._upsert_calls if t == "area_anomalies"]
        assert len(upsert_calls) == 1
        _, anomaly_rows = upsert_calls[0]
        assert len(anomaly_rows) == 1
        assert anomaly_rows[0]["hylak_id"] == 1
        assert anomaly_rows[0]["anomaly_flags"] == FLAG_SHIFT

    def test_degraded_in_anomalies_without_flag_adds_flag(self) -> None:
        labels_df = pd.DataFrame([
            {"hylak_id": 2, "shift_label": "degraded"},
        ])
        statuses = {2: ("anomalies", 0)}
        provider = MockProvider(labels_df, statuses)
        provider._area_anomalies = [
            {"hylak_id": 2, "rs_area_mean": 100.0, "rs_area_median": 90.0, "atlas_area": 200.0, "anomaly_flags": 0},
        ]

        sync_shift_to_anomalies(provider)

        assert ("area_quality", [2]) not in provider._delete_calls
        assert provider._flag_updates == [(2, FLAG_SHIFT)]

    def test_degraded_in_anomalies_with_flag_skipped(self) -> None:
        labels_df = pd.DataFrame([
            {"hylak_id": 3, "shift_label": "degraded"},
        ])
        statuses = {3: ("anomalies", FLAG_SHIFT)}
        provider = MockProvider(labels_df, statuses)

        sync_shift_to_anomalies(provider)

        assert ("area_anomalies", [3]) not in provider._delete_calls
        assert (3, FLAG_SHIFT) not in provider._flag_updates

    def test_stable_intermittent_with_only_flag_moves_to_quality(self) -> None:
        labels_df = pd.DataFrame([
            {"hylak_id": 4, "shift_label": "stable"},
        ])
        statuses = {4: ("anomalies", FLAG_SHIFT)}
        provider = MockProvider(labels_df, statuses)
        provider._area_anomalies = [
            {"hylak_id": 4, "rs_area_mean": 100.0, "rs_area_median": 90.0, "atlas_area": 200.0, "anomaly_flags": FLAG_SHIFT},
        ]

        sync_shift_to_anomalies(provider)

        delete_calls = [(t, ids) for t, ids in provider._delete_calls if t == "area_anomalies"]
        assert delete_calls == [("area_anomalies", [4])]

        upsert_calls = [(t, rows) for t, rows in provider._upsert_calls if t == "area_quality"]
        assert len(upsert_calls) == 1
        _, quality_rows = upsert_calls[0]
        assert len(quality_rows) == 1
        assert quality_rows[0]["hylak_id"] == 4
        assert "anomaly_flags" not in quality_rows[0]

    def test_stable_intermittent_with_other_flags_not_moved(self) -> None:
        labels_df = pd.DataFrame([
            {"hylak_id": 5, "shift_label": "intermittent"},
        ])
        other_flag = FLAG_SHIFT | 1
        statuses = {5: ("anomalies", other_flag)}
        provider = MockProvider(labels_df, statuses)

        sync_shift_to_anomalies(provider)

        assert ("area_anomalies", [5]) not in provider._delete_calls
        assert (5, other_flag) not in provider._flag_updates

    def test_dry_run_does_not_modify(self) -> None:
        labels_df = pd.DataFrame([
            {"hylak_id": 6, "shift_label": "degraded"},
        ])
        statuses = {6: ("quality", 0)}
        provider = MockProvider(labels_df, statuses)
        provider._area_quality = [
            {"hylak_id": 6, "rs_area_mean": 100.0, "rs_area_median": 90.0, "atlas_area": 200.0},
        ]

        sync_shift_to_anomalies(provider, dry_run=True)

        assert len(provider._upsert_calls) == 0
        assert len(provider._delete_calls) == 0
        assert len(provider._flag_updates) == 0

    def test_empty_labels_logs_warning(self) -> None:
        labels_df = pd.DataFrame()
        statuses = {}
        provider = MockProvider(labels_df, statuses)

        sync_shift_to_anomalies(provider)

        assert len(provider._upsert_calls) == 0
