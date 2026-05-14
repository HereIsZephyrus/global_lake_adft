from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.batch import LakeDataset, LakeDatasetQuery
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
    def __init__(self, trigger_count: int) -> None:
        self._remaining_triggers = trigger_count
        self.messages: list[tuple[int, tuple | dict]] = []

    def recv(self, source: int, tag: int):
        assert source == 0
        assert self._remaining_triggers > 0
        self._remaining_triggers -= 1
        return "trigger_read"

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


class _FakeDatasetFactory:
    def __init__(self) -> None:
        self._build_count = 0

    def build(self, query):
        self._build_count += 1
        lo, hi = query.id_range or (0, 3)
        n = hi - lo
        return LakeDataset(
            hylak_ids=np.arange(lo, hi, dtype=np.int64),
            year_months=np.asarray([200001, 200002], dtype=np.int64),
            values=np.ones((n, 2), dtype=float),
        )


def test_worker_run_dataset_id_batch_emits_correct_status_sequence() -> None:
    comm = _FakeComm(trigger_count=2)
    worker = Worker(
        rank=1,
        reader=_FakeProvider(),
        algorithm="pwm_extreme",
        calculator=_FakeCalculator(),
        chunk_size=10,
    )
    factory = _FakeDatasetFactory()

    queries = [
        LakeDatasetQuery(id_range=(0, 3), algorithm="pwm_extreme"),
        LakeDatasetQuery(id_range=(5, 8), algorithm="pwm_extreme"),
    ]

    worker.run_dataset_id_batch(comm, factory, queries)

    status_messages = [payload for tag, payload in comm.messages if tag == TAG_STATUS]
    assert [state for state, _ in status_messages] == [
        WorkerState.PENDING,
        WorkerState.READING,
        WorkerState.CALCULATING,
        WorkerState.PENDING,
        WorkerState.READING,
        WorkerState.CALCULATING,
        WorkerState.DONE,
    ]

    data_messages = [payload for tag, payload in comm.messages if tag == TAG_DATA]
    assert len(data_messages) == 2
    assert len(data_messages[0]["mock"]) == 3
    assert len(data_messages[1]["mock"]) == 3
