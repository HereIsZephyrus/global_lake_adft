from __future__ import annotations

from lakeanalysis.batch.manager import Manager
from lakeanalysis.batch.protocol import TRIGGER_READ, WorkerState


class _FakeComm:
    def __init__(self) -> None:
        self.sent: list[tuple[str, int, int]] = []

    def send(self, payload, dest: int, tag: int) -> None:
        self.sent.append((payload, dest, tag))


class _CollectingProvider:
    def __init__(self) -> None:
        self.persist_calls: list[dict[str, list[dict]]] = []

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        copied = {table: list(rows) for table, rows in rows_by_table.items()}
        self.persist_calls.append(copied)


def test_schedule_respects_io_budget_one() -> None:
    comm = _FakeComm()
    manager = Manager(comm, size=3, io_budget=1, writer=_CollectingProvider())

    manager._on_status(1, WorkerState.PENDING, {})
    manager._on_status(2, WorkerState.PENDING, {})
    manager._schedule_io()

    assert comm.sent == [(TRIGGER_READ, 1, 2)]
    assert manager._io_active == 1
    assert list(manager._read_queue) == [2]


def test_worker_not_enqueued_twice() -> None:
    comm = _FakeComm()
    manager = Manager(comm, size=2, io_budget=1, writer=_CollectingProvider())

    manager._on_status(1, WorkerState.PENDING, {})
    manager._on_status(1, WorkerState.PENDING, {})

    assert list(manager._read_queue) == [1]


def test_reading_to_pending_releases_io_slot() -> None:
    comm = _FakeComm()
    manager = Manager(comm, size=2, io_budget=1, writer=_CollectingProvider())

    manager._on_status(1, WorkerState.PENDING, {})
    manager._schedule_io()
    manager._on_status(1, WorkerState.READING, {})
    manager._on_status(1, WorkerState.PENDING, {})

    assert manager._io_active == 0
    assert list(manager._read_queue) == [1]


def test_done_does_not_double_count_chunk_stats() -> None:
    comm = _FakeComm()
    manager = Manager(comm, size=2, io_budget=1, writer=_CollectingProvider())

    manager._on_status(1, WorkerState.PENDING, {})
    manager._schedule_io()
    manager._on_status(1, WorkerState.READING, {})
    manager._on_status(1, WorkerState.CALCULATING, {"source": 3, "skipped": 1, "success": 0, "error": 0})
    manager._on_status(
        1,
        WorkerState.DONE,
        {"source": 3, "skipped": 1, "success": 2, "error": 0, "chunks": 1},
    )

    assert manager.report.source_lakes == 3
    assert manager.report.skipped_lakes == 1
    assert manager.report.success_lakes == 2
    assert manager.report.error_lakes == 0
    assert manager.report.processed_chunks == 1


def test_flush_happens_at_shutdown() -> None:
    comm = _FakeComm()
    provider = _CollectingProvider()
    manager = Manager(comm, size=2, io_budget=1, writer=provider)

    manager._merge_rows({"mock": [{"hylak_id": 1}]})
    manager._flush()

    assert provider.persist_calls == [{"mock": [{"hylak_id": 1}]}]
    assert manager._chunks_since_flush == 0
