"""Unified batch computing framework with MPI scheduling."""

from .domain import Calculator, LakeFilter
from .core import LakeTask
from .lake_dataset import LakeDataset
from .lake_dataset_factory import LakeDatasetFactory
from .lake_dataset_query import LakeDatasetQuery

__all__ = [
    "Calculator",
    "BatchReader",
    "BatchWriter",
    "Engine",
    "IdSetFilter",
    "LakeDataset",
    "LakeDatasetFactory",
    "LakeDatasetQuery",
    "LakeFilter",
    "LakeTask",
    "Manager",
    "RangeFilter",
    "RunReport",
    "SingleProcessLakeDatasetRunner",
    "TAG_STATUS",
    "TAG_TRIGGER",
    "TRIGGER_READ",
    "Worker",
    "WorkerState",
    "build_batch_reader",
    "build_provider_batch_reader",
    "build_provider_batch_writer",
    "build_batch_writer",
]


def __getattr__(name: str):
    if name == "Engine":
        from .engine import Engine

        return Engine
    if name in {"IdSetFilter", "RangeFilter"}:
        from .filter import IdSetFilter, RangeFilter

        return {"IdSetFilter": IdSetFilter, "RangeFilter": RangeFilter}[name]
    if name in {
        "BatchReader",
        "BatchWriter",
        "build_batch_reader",
        "build_provider_batch_reader",
        "build_provider_batch_writer",
        "build_batch_writer",
    }:
        from .io import (
            BatchReader,
            BatchWriter,
            build_batch_reader,
            build_provider_batch_reader,
            build_provider_batch_writer,
            build_batch_writer,
        )

        return {
            "BatchReader": BatchReader,
            "BatchWriter": BatchWriter,
            "build_batch_reader": build_batch_reader,
            "build_provider_batch_reader": build_provider_batch_reader,
            "build_provider_batch_writer": build_provider_batch_writer,
            "build_batch_writer": build_batch_writer,
        }[name]
    if name == "Manager":
        from .manager import Manager

        return Manager
    if name in {
        "TAG_STATUS",
        "TAG_TRIGGER",
        "TRIGGER_READ",
        "RunReport",
        "WorkerState",
        "_iter_chunk_ranges",
        "_iter_id_batches",
    }:
        from .protocol import (
            TAG_STATUS,
            TAG_TRIGGER,
            TRIGGER_READ,
            RunReport,
            WorkerState,
            _iter_chunk_ranges,
            _iter_id_batches,
        )

        return {
            "TAG_STATUS": TAG_STATUS,
            "TAG_TRIGGER": TAG_TRIGGER,
            "TRIGGER_READ": TRIGGER_READ,
            "RunReport": RunReport,
            "WorkerState": WorkerState,
            "_iter_chunk_ranges": _iter_chunk_ranges,
            "_iter_id_batches": _iter_id_batches,
        }[name]
    if name == "Worker":
        from .worker import Worker

        return Worker
    if name == "SingleProcessLakeDatasetRunner":
        from .single_process import SingleProcessLakeDatasetRunner

        return SingleProcessLakeDatasetRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
