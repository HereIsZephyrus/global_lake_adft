"""Tests for the unified batch framework (quantile calculator + engine)."""

from __future__ import annotations

import pandas as pd

from lakesource.quantile.schema import (
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.quantile.store import make_run_status_row
from lakesource.provider.base import LakeProvider
from lakeanalysis.batch import Engine, LakeTask, RangeFilter, RunReport
from lakeanalysis.batch.calculator.quantile import QuantileCalculator


def _make_series_df() -> pd.DataFrame:
    rows = []
    for year, offset in ((2000, -10.0), (2001, 0.0), (2002, 10.0)):
        for month in range(1, 13):
            rows.append({"year": year, "month": month, "water_area": 100.0 + month + offset})
    return pd.DataFrame(rows)


class FakeProvider(LakeProvider):
    def __init__(self, lake_map=None, done_ids=None):
        self._lake_map = lake_map or {}
        self._done_ids = done_ids or set()

    def fetch_lake_area_chunk(self, cs, ce):
        return {i: _make_series_df() for i in range(cs, min(ce, 10)) if i not in self._done_ids}

    def fetch_lake_area_by_ids(self, id_list):
        return {i: _make_series_df() for i in id_list}

    def fetch_frozen_year_months_chunk(self, cs, ce):
        return {}

    def fetch_frozen_year_months_by_ids(self, id_list):
        return {}

    def fetch_max_hylak_id(self):
        return 9

    def fetch_lake_geometry_wkt_by_ids(self, hylak_ids, **kw):
        raise NotImplementedError

    def fetch_done_ids(self, algorithm, cs, ce):
        return {0, 1}

    def count_done_ids(self, algorithm, cs, ce):
        return len(self._done_ids)

    def fetch_extremes_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_extremes_by_type_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_transitions_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_transitions_by_type_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_eot_convergence_grid_agg(self, tail, q, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_eot_converged_grid_agg(self, tail, q, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_pwm_convergence_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_pwm_converged_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def persist(self, rows_by_table):
        pass

    def ensure_schema(self, algorithm):
        pass

    @property
    def backend_name(self):
        return "fake"

    @property
    def cache_dir(self):
        return None


def test_quantile_calculator_run() -> None:
    calc = QuantileCalculator(
        min_valid_per_month=3,
        min_valid_observations=36,
    )
    task = LakeTask(hylak_id=42, series_df=_make_series_df(), frozen_year_months=frozenset())
    result = calc.run(task)
    assert result.hylak_id == 42


def test_quantile_calculator_result_to_rows() -> None:
    calc = QuantileCalculator(
        min_valid_per_month=3,
        min_valid_observations=36,
    )
    task = LakeTask(hylak_id=42, series_df=_make_series_df(), frozen_year_months=frozenset())
    result = calc.run(task)
    rows = calc.result_to_rows(result)

    assert "quantile_labels" in rows
    assert "quantile_extremes" in rows
    assert "quantile_abrupt_transitions" in rows
    assert "quantile_run_status" in rows
    assert len(rows["quantile_run_status"]) == 1
    assert rows["quantile_run_status"][0]["status"] == RUN_STATUS_DONE
    assert rows["quantile_run_status"][0]["hylak_id"] == 42


def test_quantile_calculator_error_to_rows() -> None:
    calc = QuantileCalculator()
    rows = calc.error_to_rows(99, ValueError("boom"), 0, 1000)

    assert "quantile_run_status" in rows
    assert len(rows["quantile_run_status"]) == 1
    assert rows["quantile_run_status"][0]["status"] == RUN_STATUS_ERROR
    assert rows["quantile_run_status"][0]["hylak_id"] == 99
    assert "boom" in rows["quantile_run_status"][0]["error_message"]


def test_range_filter() -> None:
    rf = RangeFilter(start=10, end=20)
    assert rf(range(0, 30)) == set(range(10, 20))

    rf_no_end = RangeFilter(start=5)
    assert rf_no_end(range(0, 10)) == set(range(5, 10))

    rf_no_start = RangeFilter(end=5)
    assert rf_no_start(range(0, 10)) == set(range(0, 5))


def test_engine_single_process_with_mocks() -> None:
    class CollectingProvider(FakeProvider):
        def __init__(self):
            super().__init__()
            self.rows = {}

        def persist(self, rows_by_table):
            for k, v in rows_by_table.items():
                self.rows.setdefault(k, []).extend(v)

    provider = CollectingProvider()
    engine = Engine(
        provider=provider,
        calculator=_FakeCalculator(),
        algorithm="quantile",
        chunk_size=5,
    )
    report = engine.run()

    assert report.total_chunks == 2
    assert report.success_lakes == 8
    assert report.skipped_lakes == 2
    assert len(provider.rows.get("mock", [])) == 8


def test_engine_skips_all_done_lakes() -> None:
    class AllDoneProvider(FakeProvider):
        def __init__(self):
            super().__init__()
            self._done_ids = {0, 1}

        def fetch_lake_area_chunk(self, cs, ce):
            return {0: _make_series_df(), 1: _make_series_df()}

        def fetch_done_ids(self, algorithm, cs, ce):
            return {0, 1}

    engine = Engine(
        provider=AllDoneProvider(),
        calculator=_FakeCalculator(),
        algorithm="quantile",
        chunk_size=5,
    )
    report = engine.run()

    assert report.success_lakes == 0
    assert report.skipped_lakes > 0


def test_make_run_status_row_done() -> None:
    row = make_run_status_row(
        hylak_id=42,
        chunk_start=0,
        chunk_end=1000,
        workflow_version="test-v1",
        status=RUN_STATUS_DONE,
    )
    assert row["status"] == "done"
    assert row["hylak_id"] == 42


def test_make_run_status_row_error() -> None:
    row = make_run_status_row(
        hylak_id=42,
        chunk_start=0,
        chunk_end=1000,
        workflow_version="test-v1",
        status=RUN_STATUS_ERROR,
        error_message="boom",
    )
    assert row["status"] == "error"
    assert row["error_message"] == "boom"


class _FakeCalculator:
    def run(self, task):
        return {"hylak_id": task.hylak_id}

    def result_to_rows(self, result):
        return {"mock": [{"hylak_id": result["hylak_id"]}]}

    def error_to_rows(self, hylak_id, error, cs, ce):
        return {"mock": [{"hylak_id": hylak_id, "error": str(error)}]}
