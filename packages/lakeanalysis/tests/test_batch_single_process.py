from __future__ import annotations

import pandas as pd
import numpy as np

from lakesource.provider.base import LakeProvider
from lakeanalysis.batch import IdSetFilter, LakeDataset, LakeDatasetFactory, LakeDatasetQuery, RangeFilter
from lakeanalysis.batch.io import BatchReader, BatchWriter
from lakeanalysis.batch.single_process import (
    SingleProcessIdBatchRunner,
    SingleProcessLakeDatasetRunner,
    SingleProcessRunner,
)


def _make_series_df() -> pd.DataFrame:
    rows = []
    for month in range(1, 4):
        rows.append({"year": 2000, "month": month, "water_area": 100.0 + month})
    return pd.DataFrame(rows)


class _FakeProvider(LakeProvider):
    def __init__(self, *, done_ids: set[int] | None = None) -> None:
        self._done_ids = done_ids or set()
        self.persisted: list[dict[str, list[dict]]] = []
        self.ensured: list[str] = []

    def fetch_lake_area_chunk(self, cs, ce):
        return {i: _make_series_df() for i in range(cs, ce)}

    def fetch_lake_area_by_ids(self, id_list):
        return {i: _make_series_df() for i in id_list}

    def fetch_frozen_year_months_chunk(self, cs, ce):
        return {}

    def fetch_frozen_year_months_by_ids(self, id_list):
        return {}

    def fetch_max_hylak_id(self):
        return 3

    def fetch_lake_geometry_wkt_by_ids(self, hylak_ids, **kw):
        raise NotImplementedError

    def fetch_grid_agg(self, query_name, resolution=0.5, **kw):
        return pd.DataFrame()

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

    @property
    def backend_name(self):
        return "fake"

    @property
    def cache_dir(self):
        return None


class _FakeReader(BatchReader):
    def __init__(self, provider: _FakeProvider) -> None:
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
        return {hid for hid in self._provider._done_ids if cs <= hid < ce}


class _FakeWriter(BatchWriter):
    def __init__(self, provider: _FakeProvider) -> None:
        self._provider = provider

    def persist(self, rows_by_table):
        self._provider.persisted.append(rows_by_table)

    def ensure_schema(self, algorithm):
        self._provider.ensured.append(algorithm)

    def truncate_run_status(self, algorithm):
        pass


class _FakeCalculator:
    def run(self, task):
        return {"hylak_id": task.hylak_id}

    def result_to_rows(self, result):
        return {"mock": [result]}

    def error_to_rows(self, hylak_id, error, cs, ce):
        return {"mock": [{"hylak_id": hylak_id, "error": str(error)}]}


class _FakeDatasetFactory:
    def build(self, query):
        del query
        return LakeDataset(
            hylak_ids=np.asarray([10, 20], dtype=np.int64),
            year_months=np.asarray([200001, 200002], dtype=np.int64),
            values=np.asarray([[1.0, 2.0], [3.0, 4.0]], dtype=float),
            frozen_mask=np.asarray([[False, True], [False, False]], dtype=bool),
        )


class _FakeDatasetCalculator:
    def run_dataset(self, dataset, *, error_chunk=(0, 0)):
        del error_chunk
        rows = {"mock": [{"hylak_id": int(hid)} for hid in dataset.hylak_ids.tolist()]}
        return rows, len(dataset), 0


def test_single_process_runner_handles_range_batches() -> None:
    provider = _FakeProvider(done_ids={0})
    runner = SingleProcessRunner(
        _FakeReader(provider),
        _FakeWriter(provider),
        _FakeCalculator(),
        algorithm="quantile",
        lake_filter=RangeFilter(start=0, end=4),
        chunk_size=2,
    )

    report = runner.run()

    assert provider.ensured == ["quantile"]
    assert report.total_chunks == 2
    assert report.success_lakes == 3
    assert report.skipped_lakes == 1
    assert len(provider.persisted) == 2


def test_single_process_id_batch_runner_filters_done_ids() -> None:
    provider = _FakeProvider(done_ids={10})
    runner = SingleProcessIdBatchRunner(
        _FakeReader(provider),
        _FakeWriter(provider),
        _FakeCalculator(),
        algorithm="comparison",
        lake_filter=IdSetFilter({10, 20}),
        chunk_size=10,
    )

    report = runner.run()

    assert provider.ensured == ["comparison"]
    assert report.total_chunks == 1
    assert report.success_lakes == 1
    assert report.skipped_lakes == 1
    assert provider.persisted == [{"mock": [{"hylak_id": 20}]}]


def test_single_process_lake_dataset_runner_persists_dataset_results() -> None:
    provider = _FakeProvider()
    runner = SingleProcessLakeDatasetRunner(
        _FakeDatasetFactory(),
        LakeDatasetQuery(algorithm="pwm_extreme"),
        _FakeWriter(provider),
        _FakeDatasetCalculator(),
        algorithm="pwm_extreme",
    )

    report = runner.run()

    assert provider.ensured == ["pwm_extreme"]
    assert report.total_chunks == 1
    assert report.processed_chunks == 1
    assert report.source_lakes == 2
    assert report.success_lakes == 2
    assert provider.persisted == [{"mock": [{"hylak_id": 10}, {"hylak_id": 20}]}]
