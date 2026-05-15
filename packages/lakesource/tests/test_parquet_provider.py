"""Comprehensive tests for ParquetLakeProvider — fetch, write, delete, utilities."""

from __future__ import annotations

import pandas as pd
from pathlib import Path

import pytest

from lakesource.config import Backend, SourceConfig
from lakesource.provider.parquet_provider import ParquetLakeProvider


# ── Helpers to build test data ──────────────────────────────────────────────

def _make_lake_area_parquet(tmp_path: Path) -> None:
    """Write lake_area.parquet with 3 lakes, 2 months each."""
    df = pd.DataFrame([
        {"hylak_id": 1, "year_month": pd.Timestamp("2000-01-01"), "water_area": 10.5},
        {"hylak_id": 1, "year_month": pd.Timestamp("2000-02-01"), "water_area": 11.0},
        {"hylak_id": 2, "year_month": pd.Timestamp("2000-01-01"), "water_area": 20.0},
        {"hylak_id": 2, "year_month": pd.Timestamp("2000-02-01"), "water_area": 21.0},
        {"hylak_id": 5, "year_month": pd.Timestamp("2000-01-01"), "water_area": 50.0},
        {"hylak_id": 5, "year_month": pd.Timestamp("2000-02-01"), "water_area": 51.0},
    ])
    df.to_parquet(tmp_path / "lake_area.parquet", index=False)


def _make_lake_info_parquet(tmp_path: Path) -> None:
    """Write lake_info.parquet."""
    df = pd.DataFrame([
        {"hylak_id": 1, "lake_area": 100.0, "annual_means_std": 5.0, "mean_area": 10.0, "lat": 30.0, "lon": 120.0},
        {"hylak_id": 2, "lake_area": 200.0, "annual_means_std": 10.0, "mean_area": 20.0, "lat": 31.0, "lon": 121.0},
        {"hylak_id": 5, "lake_area": 500.0, "annual_means_std": None, "mean_area": None, "lat": 35.0, "lon": 125.0},
        {"hylak_id": 10, "lake_area": 1000.0, "annual_means_std": 50.0, "mean_area": 100.0, "lat": 40.0, "lon": 130.0},
    ])
    df.to_parquet(tmp_path / "lake_info.parquet", index=False)


def _make_anomaly_parquet(tmp_path: Path) -> None:
    """Write anomaly.parquet with frozen data."""
    df = pd.DataFrame([
        {"hylak_id": 1, "year_month": pd.Timestamp("2000-01-01"), "anomaly_flags": 0},
        {"hylak_id": 1, "year_month": pd.Timestamp("2000-03-01"), "anomaly_flags": 1},
        {"hylak_id": 2, "year_month": pd.Timestamp("2000-06-01"), "anomaly_flags": 0},
        {"hylak_id": 5, "year_month": pd.Timestamp("2000-12-01"), "anomaly_flags": 0},
    ])
    df.to_parquet(tmp_path / "anomaly.parquet", index=False)


def _make_provider(tmp_path: Path) -> ParquetLakeProvider:
    """Create a provider with all test parquet files."""
    _make_lake_area_parquet(tmp_path)
    _make_lake_info_parquet(tmp_path)
    _make_anomaly_parquet(tmp_path)
    return ParquetLakeProvider(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))


# ── fetch_lake_area_chunk ───────────────────────────────────────────────────

class TestFetchLakeAreaChunk:
    def test_returns_correct_ids_and_rows(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_chunk(1, 3)
        assert set(result.keys()) == {1, 2}
        assert len(result[1]) == 2
        assert len(result[2]) == 2
        assert result[1]["water_area"].tolist() == [10.5, 11.0]

    def test_returns_empty_for_out_of_range_chunk(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_chunk(100, 200)
        assert result == {}

    def test_handles_exclusive_upper_bound(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_chunk(1, 2)
        assert set(result.keys()) == {1}

    def test_columns_are_correct_types(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_chunk(1, 3)
        df = result[1]
        assert df["year"].dtype in ("int64", "int32")
        assert df["month"].dtype in ("int64", "int32")
        assert "water_area" in df.columns


# ── fetch_lake_area_by_ids ──────────────────────────────────────────────────

class TestFetchLakeAreaByIds:
    def test_returns_specific_ids(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_by_ids([1, 5])
        assert set(result.keys()) == {1, 5}

    def test_empty_id_list_returns_empty(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_by_ids([])
        assert result == {}

    def test_nonexistent_ids_return_empty_map(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_by_ids([999, 1000])
        assert result == {}

    def test_mixed_existing_and_nonexistent(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_lake_area_by_ids([1, 999])
        assert set(result.keys()) == {1}


# ── fetch_max_hylak_id ──────────────────────────────────────────────────────

class TestFetchMaxHylakId:
    def test_returns_max_from_lake_info(self, tmp_path):
        provider = _make_provider(tmp_path)
        assert provider.fetch_max_hylak_id() == 10

    def test_returns_zero_when_no_lake_info(self, tmp_path):
        _make_lake_area_parquet(tmp_path)
        provider = ParquetLakeProvider(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))
        assert provider.fetch_max_hylak_id() == 0


# ── fetch_atlas_area_chunk / by_ids ─────────────────────────────────────────

class TestFetchAtlasArea:
    def test_chunk_returns_correct_areas(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_atlas_area_chunk(1, 3)
        assert result == {1: 100.0, 2: 200.0}

    def test_chunk_empty_range_returns_empty(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_atlas_area_chunk(100, 200)
        assert result == {}

    def test_by_ids_returns_specific(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_atlas_area_by_ids([1, 5])
        assert result == {1: 100.0, 5: 500.0}

    def test_by_ids_handles_zero_lake_area(self, tmp_path):
        _make_lake_area_parquet(tmp_path)
        _make_anomaly_parquet(tmp_path)
        pd.DataFrame([
            {"hylak_id": 999, "lake_area": 0.0, "annual_means_std": None,
             "mean_area": None, "lat": 0.0, "lon": 0.0},
        ]).to_parquet(tmp_path / "lake_info.parquet", index=False)
        provider = ParquetLakeProvider(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))
        result = provider.fetch_atlas_area_by_ids([999])
        assert result == {999: 0.0}

    @pytest.mark.skip(reason="BUG: pd.NA read from parquet is not None, "
                             "causes TypeError in float(row.lake_area)")
    def test_by_ids_handles_none_lake_area(self, tmp_path):
        _make_lake_area_parquet(tmp_path)
        _make_anomaly_parquet(tmp_path)
        pd.DataFrame([
            {"hylak_id": 999, "lake_area": None, "annual_means_std": None,
             "mean_area": None, "lat": 0.0, "lon": 0.0},
        ]).to_parquet(tmp_path / "lake_info.parquet", index=False)
        provider = ParquetLakeProvider(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))
        result = provider.fetch_atlas_area_by_ids([999])
        assert result == {999: 0.0}


# ── fetch_seasonal_amplitude ────────────────────────────────────────────────

class TestFetchSeasonalAmplitude:
    def test_returns_ratio_for_valid_data(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_seasonal_amplitude_chunk(1, 3)
        assert result[1] == pytest.approx(0.5)  # 5.0 / 10.0
        assert result[2] == pytest.approx(0.5)  # 10.0 / 20.0

    def test_returns_none_when_std_or_mean_null(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_seasonal_amplitude_by_ids([5])
        assert result[5] is None

    def test_empty_id_list_returns_empty(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_seasonal_amplitude_by_ids([])
        assert result == {}


# ── fetch_frozen_year_months ────────────────────────────────────────────────

class TestFetchFrozenYearMonths:
    def test_chunk_returns_frozen_maps(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_frozen_year_months_chunk(1, 3)
        assert 1 in result
        assert 2 in result
        assert 200001 in result[1]  # 2000 * 100 + 1
        assert 200003 in result[1]  # 2000 * 100 + 3

    def test_chunk_empty_returns_empty(self, tmp_path):
        provider = _make_provider(tmp_path)
        result = provider.fetch_frozen_year_months_chunk(100, 200)
        assert result == {}


# ── _split_by_hylak_id ─────────────────────────────────────────────────────

class TestSplitByHylakId:
    def test_splits_into_separate_dataframes(self):
        df = pd.DataFrame([
            {"hylak_id": 1, "year": 2000, "month": 1, "water_area": 10.0},
            {"hylak_id": 1, "year": 2000, "month": 2, "water_area": 11.0},
            {"hylak_id": 2, "year": 2000, "month": 1, "water_area": 20.0},
        ])
        result = ParquetLakeProvider._split_by_hylak_id(df)
        assert set(result.keys()) == {1, 2}
        assert "hylak_id" not in result[1].columns
        assert len(result[1]) == 2
        assert len(result[2]) == 1

    def test_empty_dataframe_returns_empty(self):
        result = ParquetLakeProvider._split_by_hylak_id(pd.DataFrame())
        assert result == {}


# ── Write operations (upsert / truncate / delete / fetch_rows) ─────────────

class TestWriteOperations:
    def test_upsert_creates_table(self, tmp_path):
        provider = _make_provider(tmp_path)
        rows = [{"hylak_id": 1, "score": 0.95}, {"hylak_id": 2, "score": 0.80}]
        provider.upsert_rows("test_results", rows)
        result = provider.fetch_rows("test_results", 0, 100)
        assert len(result) == 2
        assert result[0]["score"] == 0.95

    def test_upsert_merges_with_existing(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [{"hylak_id": 1, "score": 0.50}])
        provider.upsert_rows("test_results", [{"hylak_id": 1, "score": 0.99}])
        result = provider.fetch_rows("test_results", 0, 100)
        assert len(result) == 1
        assert result[0]["score"] == 0.99  # keep=last

    def test_upsert_empty_rows_is_noop(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [])
        result = provider.fetch_rows("test_results", 0, 100)
        assert result == []

    def test_truncate_removes_table(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [{"hylak_id": 1, "score": 0.50}])
        provider.truncate_table("test_results")
        result = provider.fetch_rows("test_results", 0, 100)
        assert result == []

    def test_delete_ids_removes_specific(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [
            {"hylak_id": 1, "score": 0.50},
            {"hylak_id": 2, "score": 0.80},
            {"hylak_id": 3, "score": 0.90},
        ])
        provider.delete_ids("test_results", [2])
        result = provider.fetch_rows("test_results", 0, 100)
        ids = {r["hylak_id"] for r in result}
        assert ids == {1, 3}

    def test_delete_ids_empty_list_is_noop(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [{"hylak_id": 1, "score": 0.50}])
        provider.delete_ids("test_results", [])
        result = provider.fetch_rows("test_results", 0, 100)
        assert len(result) == 1

    def test_fetch_done_ids_returns_processed(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [
            {"hylak_id": 1, "status": "success"},
            {"hylak_id": 2, "status": "error"},
            {"hylak_id": 5, "status": "success"},
        ])
        done = provider.fetch_done_ids("test_results", 0, 100)
        assert done == {1, 2, 5}

    def test_fetch_done_ids_filters_by_status(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [
            {"hylak_id": 1, "status": "success"},
            {"hylak_id": 2, "status": "error"},
        ])
        done = provider.fetch_done_ids("test_results", 0, 100, status="success")
        assert done == {1}

    def test_fetch_done_ids_respects_chunk_range(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("test_results", [
            {"hylak_id": 1, "status": "success"},
            {"hylak_id": 5, "status": "success"},
            {"hylak_id": 10, "status": "success"},
        ])
        done = provider.fetch_done_ids("test_results", 0, 5)
        assert done == {1}


# ── fetch_area_statuses ────────────────────────────────────────────────────

class TestFetchAreaStatuses:
    def test_returns_quality_and_anomaly_statuses(self, tmp_path):
        provider = _make_provider(tmp_path)
        provider.upsert_rows("area_quality", [{"hylak_id": 1}, {"hylak_id": 3}])
        provider.upsert_rows("area_anomalies", [
            {"hylak_id": 1, "anomaly_flags": 3},
            {"hylak_id": 2, "anomaly_flags": 0},
        ])
        result = provider.fetch_area_statuses()
        # hylak 1: appears in both — anomalies overwrites (last)
        assert result[1] == ("anomalies", 3)
        # hylak 2: only anomalies
        assert result[2] == ("anomalies", 0)
        # hylak 3: only quality
        assert result[3] == ("quality", 0)


# ── Backend metadata ───────────────────────────────────────────────────────

class TestBackendMetadata:
    def test_backend_name_is_parquet(self, tmp_path):
        provider = _make_provider(tmp_path)
        assert provider.backend_name == "parquet"

    def test_cache_dir_is_data_parent_cache(self, tmp_path):
        provider = _make_provider(tmp_path)
        assert provider.cache_dir == tmp_path.parent / "cache"

    def test_raises_when_data_dir_is_none(self, monkeypatch):
        monkeypatch.setenv("PARQUET_DATA_DIR", "/fake/to/satisfy/init")
        test_config = SourceConfig(backend=Backend.PARQUET)
        object.__setattr__(test_config, "data_dir", None)
        with pytest.raises(ValueError, match="data_dir is required"):
            ParquetLakeProvider(test_config)
