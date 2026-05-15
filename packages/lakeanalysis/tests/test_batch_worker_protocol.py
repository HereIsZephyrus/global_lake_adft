from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.batch import LakeDataset, WorkerSliceAssignment
from lakeanalysis.batch.protocol import TAG_DATA, TAG_STATUS, WorkerState
from lakeanalysis.batch.worker import Worker


def _make_series_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"year": 2000, "month": 1, "water_area": 100.0},
            {"year": 2000, "month": 2, "water_area": 101.0},
        ]
    )


class _FakeComm:
    def __init__(self) -> None:
        self.messages: list[tuple[int, tuple | dict]] = []

    def send(self, payload, dest: int, tag: int) -> None:
        assert dest == 0
        self.messages.append((tag, payload))


class _FakeProvider:
    def __init__(self, *, empty: bool = False, done_ids: set[int] | None = None) -> None:
        self._empty = empty
        self._done_ids = done_ids or set()

    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        if self._empty:
            return {}
        return {hid: _make_series_df() for hid in range(chunk_start, chunk_end)}

    def fetch_lake_area_by_ids(self, id_list: list[int]):
        if self._empty:
            return {}
        return {hid: _make_series_df() for hid in id_list}

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        return {}

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        return {}

    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int):
        return set(self._done_ids)


class _FakeCalculator:
    def run_dataset(self, dataset, *, error_chunk=(0, 0)):
        del error_chunk
        rows = {"mock": [{"hylak_id": int(hid)} for hid in dataset.hylak_ids.tolist()]}
        return rows, len(dataset), 0

    def result_to_rows(self, result):
        return {"mock": [result]}

    def error_to_rows(self, hylak_id, error, chunk_start, chunk_end):
        return {"mock": [{"hylak_id": hylak_id, "error": str(error)}]}


class _FakeResidentSlice:
    def __init__(self, dataset: LakeDataset) -> None:
        self.dataset = dataset

    def build_query(self, id_subset: frozenset[int]) -> LakeDataset:
        indices = [idx for idx, hid in enumerate(self.dataset.hylak_ids.tolist()) if int(hid) in id_subset]
        return self.dataset.take(indices)


class _FakeDatasetFactory:
    def __init__(self) -> None:
        self.preload_calls: list[frozenset[int]] = []

    def preload(self, id_subset: frozenset[int], *, fields=("series", "frozen_mask")):
        del fields
        self.preload_calls.append(id_subset)
        hylak_ids = np.asarray(sorted(id_subset), dtype=np.int64)
        n = len(hylak_ids)
        return _FakeResidentSlice(LakeDataset(
            hylak_ids=hylak_ids,
            year_months=np.asarray([200001, 200002], dtype=np.int64),
            values=np.ones((n, 2), dtype=float),
        ))


def test_worker_run_dataset_id_batch_emits_preload_then_done_sequence() -> None:
    comm = _FakeComm()
    worker = Worker(
        rank=1,
        algorithm="pwm_extreme",
        calculator=_FakeCalculator(),
        chunk_size=10,
    )
    factory = _FakeDatasetFactory()

    assignment = WorkerSliceAssignment(
        algorithm="pwm_extreme",
        id_subset=frozenset({0, 1, 2, 5, 6, 7}),
        chunk_size=3,
    )

    worker.run_dataset_id_batch(comm, factory, assignment)

    status_messages = [payload for tag, payload in comm.messages if tag == TAG_STATUS]
    assert [state for state, _ in status_messages] == [
        WorkerState.PRELOADING,
        WorkerState.CALCULATING,
        WorkerState.DONE,
    ]
    assert factory.preload_calls == [frozenset({0, 1, 2, 5, 6, 7})]

    data_messages = [payload for tag, payload in comm.messages if tag == TAG_DATA]
    assert len(data_messages) == 2
    assert len(data_messages[0]["mock"]) == 3
    assert len(data_messages[1]["mock"]) == 3
