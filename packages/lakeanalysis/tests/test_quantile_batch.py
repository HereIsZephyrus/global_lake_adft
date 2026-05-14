"""Tests for the unified batch framework (quantile calculator + engine)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakesource.quantile.schema import (
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.quantile.store import make_run_status_row
from lakesource.provider.base import LakeProvider
from lakeanalysis.batch import (
    Engine,
    LakeDataset,
    LakeTask,
    RangeFilter,
)
from lakeanalysis.batch.io import BatchReader, BatchWriter
from lakeanalysis.batch.calculator.quantile import QuantileCalculator


def _make_series_df() -> pd.DataFrame:
    rows = []
    for year, offset in ((2000, -10.0), (2001, 0.0), (2002, 10.0)):
        for month in range(1, 13):
            rows.append({"year": year, "month": month, "water_area": 100.0 + month + offset})
    return pd.DataFrame(rows)


def _make_single_lake_dataset(df: pd.DataFrame, hylak_id: int = 42) -> LakeDataset:
    ym = df["year"].astype(int) * 100 + df["month"].astype(int)
    return LakeDataset(
        hylak_ids=np.array([hylak_id], dtype=np.int64),
        year_months=ym.to_numpy(dtype=np.int64),
        values=df["water_area"].to_numpy(dtype=float).reshape(1, -1),
    )


class FakeProvider(LakeProvider):
    def __init__(self, lake_map=None, done_ids=None):
        self._lake_map = lake_map or {}
        self._done_ids = done_ids or set()
        self._quality_ids = set(range(9))

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

    def fetch_rows(self, table_name, start, end):
        if table_name == "area_quality":
            ids = sorted(i for i in self._quality_ids if max(start, 0) <= i < min(end, 9))
            return [{"hylak_id": i} for i in ids]
        return [{"hylak_id": i} for i in range(max(start, 0), min(end, 9))]

    def fetch_done_ids(self, table_name, start, end, status=None):
        return self._done_ids & set(range(max(start, 0), end))

    def fetch_lake_geometry_wkt_by_ids(self, hylak_ids, **kw):
        raise NotImplementedError

    def fetch_extremes_grid_agg(self, resolution=0.5, **kw):
        return pd.DataFrame()

    def fetch_grid_agg(self, query_name, resolution=0.5, **kw):
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

    @property
    def backend_name(self):
        return "fake"

    @property
    def cache_dir(self):
        return None


class FakeReader(BatchReader):
    def __init__(self, provider: FakeProvider):
        self._provider = provider

    def fetch_lake_area_chunk(self, cs, ce):
        return self._provider.fetch_lake_area_chunk(cs, ce)

    def fetch_lake_area_by_ids(self, id_list):
        return self._provider.fetch_lake_area_by_ids(id_list)

    def fetch_frozen_year_months_chunk(self, cs, ce):
        return self._provider.fetch_frozen_year_months_chunk(cs, ce)

    def fetch_frozen_year_months_by_ids(self, id_list):
        return self._provider.fetch_frozen_year_months_by_ids(id_list)

    def fetch_max_hylak_id(self):
        return self._provider.fetch_max_hylak_id()

    def fetch_done_ids(self, algorithm, cs, ce):
        del algorithm
        return self._provider.fetch_done_ids("done", cs, ce)

    def fetch_quality_ids(self):
        return set(self._provider._quality_ids)


class FakeWriter(BatchWriter):
    def __init__(self):
        self.rows = {}
        self.ensured = []

    def persist(self, rows_by_table):
        for k, v in rows_by_table.items():
            self.rows.setdefault(k, []).extend(v)

    def ensure_schema(self, algorithm):
        self.ensured.append(algorithm)

    def truncate_run_status(self, algorithm):
        pass


def test_quantile_calculator_run() -> None:
    calc = QuantileCalculator(
        min_valid_per_month=3,
        min_valid_observations=36,
    )
    ds = _make_single_lake_dataset(_make_series_df(), hylak_id=42)
    rows_by_table, success, _ = calc.run_dataset(ds)
    assert success == 1
    assert "quantile_labels" in rows_by_table


def test_quantile_calculator_result_to_rows() -> None:
    calc = QuantileCalculator(
        min_valid_per_month=3,
        min_valid_observations=36,
    )
    ds = _make_single_lake_dataset(_make_series_df(), hylak_id=42)
    rows, _, _ = calc.run_dataset(ds)

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
    from lakeanalysis.batch.lake_dataset_factory import LakeDatasetFactory
    provider = FakeProvider(done_ids={0, 1})
    writer = FakeWriter()
    factory = LakeDatasetFactory(provider=provider)
    engine = Engine(
        reader=FakeReader(provider),
        writer=writer,
        calculator=_FakeCalculator(),
        algorithm="quantile",
        chunk_size=5,
        dataset_factory=factory,
    )
    report = engine.run()

    assert report.source_lakes == 7
    assert report.success_lakes == 7
    assert len(writer.rows.get("mock", [])) == 7


def test_engine_skips_all_done_lakes() -> None:
    from lakeanalysis.batch.lake_dataset_factory import LakeDatasetFactory

    provider = FakeProvider(done_ids=set(range(9)))
    factory = LakeDatasetFactory(provider=provider)
    engine = Engine(
        reader=FakeReader(provider),
        writer=FakeWriter(),
        calculator=_FakeCalculator(),
        algorithm="quantile",
        chunk_size=5,
        dataset_factory=factory,
    )
    report = engine.run()

    assert report.success_lakes == 0


def test_engine_build_queries_uses_quality_domain_and_id_subsets() -> None:
    provider = FakeProvider(done_ids={11})
    provider._quality_ids = {2, 11, 100, 300}
    engine = Engine(
        reader=FakeReader(provider),
        writer=FakeWriter(),
        calculator=_FakeCalculator(),
        algorithm="quantile",
        chunk_size=2,
        dataset_factory=None,
    )

    queries = list(engine._build_queries())

    assert [query.id_subset for query in queries] == [
        frozenset({2, 100}),
        frozenset({300}),
    ]
    assert all(query.id_range is None for query in queries)
    assert all(query.exclude_done is False for query in queries)


def test_engine_build_queries_applies_range_filter_within_quality_domain() -> None:
    provider = FakeProvider()
    provider._quality_ids = {2, 11, 100, 300}
    engine = Engine(
        reader=FakeReader(provider),
        writer=FakeWriter(),
        calculator=_FakeCalculator(),
        algorithm="quantile",
        lake_filter=RangeFilter(start=10, end=200),
        chunk_size=10,
        dataset_factory=None,
    )

    queries = list(engine._build_queries())

    assert [query.id_subset for query in queries] == [frozenset({11, 100})]


def test_make_run_status_row_done() -> None:
    row = make_run_status_row(
        hylak_id=42,
        chunk_start=0,
        chunk_end=1000,
        status=RUN_STATUS_DONE,
    )
    assert row["status"] == "done"
    assert row["hylak_id"] == 42


def test_make_run_status_row_error() -> None:
    row = make_run_status_row(
        hylak_id=42,
        chunk_start=0,
        chunk_end=1000,
        status=RUN_STATUS_ERROR,
        error_message="boom",
    )
    assert row["status"] == "error"
    assert row["error_message"] == "boom"


class _FakeCalculator:
    def run_dataset(self, dataset, *, error_chunk=(0, 0)):
        rows = {"mock": []}
        success = 0
        for idx in range(len(dataset)):
            task = dataset.to_task(idx)
            rows["mock"].append({"hylak_id": task.hylak_id})
            success += 1
        return rows, success, 0

    def result_to_rows(self, result):
        return {"mock": [{"hylak_id": result["hylak_id"]}]}

    def error_to_rows(self, hylak_id, error, cs, ce):
        return {"mock": [{"hylak_id": hylak_id, "error": str(error)}]}
