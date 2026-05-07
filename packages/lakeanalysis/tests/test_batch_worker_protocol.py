from __future__ import annotations

import pandas as pd

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
    def run(self, task):
        return {"hylak_id": task.hylak_id}

    def result_to_rows(self, result):
        return {"mock": [result]}

    def error_to_rows(self, hylak_id, error, chunk_start, chunk_end):
        return {"mock": [{"hylak_id": hylak_id, "error": str(error)}]}


def test_worker_run_emits_expected_status_sequence() -> None:
    comm = _FakeComm(trigger_count=1)
    worker = Worker(
        rank=1,
        reader=_FakeProvider(),
        algorithm="quantile",
        calculator=_FakeCalculator(),
        chunk_size=2,
    )

    worker.run(comm, {1: (0, 2)})

    status_messages = [payload for tag, payload in comm.messages if tag == TAG_STATUS]
    assert [state for state, _ in status_messages] == [
        WorkerState.PENDING,
        WorkerState.READING,
        WorkerState.CALCULATING,
        WorkerState.DONE,
    ]

    data_messages = [payload for tag, payload in comm.messages if tag == TAG_DATA]
    assert data_messages == [{"mock": [{"hylak_id": 0}, {"hylak_id": 1}]}]


def test_worker_run_empty_chunk_returns_to_pending_then_done() -> None:
    comm = _FakeComm(trigger_count=2)
    worker = Worker(
        rank=1,
        reader=_FakeProvider(empty=True),
        algorithm="quantile",
        calculator=_FakeCalculator(),
        chunk_size=1,
    )

    worker.run(comm, {1: (0, 2)})

    status_messages = [payload for tag, payload in comm.messages if tag == TAG_STATUS]
    assert [state for state, _ in status_messages] == [
        WorkerState.PENDING,
        WorkerState.READING,
        WorkerState.PENDING,
        WorkerState.READING,
        WorkerState.DONE,
    ]


def test_worker_run_id_batch_masks_done_ids() -> None:
    comm = _FakeComm(trigger_count=1)
    worker = Worker(
        rank=1,
        reader=_FakeProvider(done_ids={10}),
        algorithm="comparison",
        calculator=_FakeCalculator(),
        chunk_size=10,
    )

    worker.run_id_batch(comm, {1: [[10, 20]]})

    data_messages = [payload for tag, payload in comm.messages if tag == TAG_DATA]
    assert data_messages == [{"mock": [{"hylak_id": 20}]}]

    done_state = [payload for tag, payload in comm.messages if tag == TAG_STATUS][-1]
    assert done_state == (
        WorkerState.DONE,
        {"source": 2, "skipped": 1, "success": 1, "error": 0, "chunks": 1},
    )
