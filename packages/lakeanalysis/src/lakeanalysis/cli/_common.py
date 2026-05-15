"""Shared typer CLI helpers: common parameters, logging init, data dir."""

from typing import Annotated, Any

import typer

from lakeanalysis.batch import (  # pylint: disable=no-name-in-module
    Engine,
    IdSetFilter,
    RangeFilter,
    build_provider_batch_reader,
    build_provider_batch_writer,
)
from lakeanalysis.filters import build_lake_filter
from lakeanalysis.logger import Logger
from lakesource.config import OutputFilter, SourceConfig


def setup_logging(name: str) -> None:
    """Unified Logger initialisation for all CLI commands.

    Replaces the per-script ``Logger("script_name")`` side-effect pattern
    with a single call point.
    """
    log_version = True
    try:
        from mpi4py import MPI

        log_version = MPI.COMM_WORLD.Get_rank() == 0
    except ImportError:
        log_version = True
    Logger(name, log_version=log_version)


def run_batch_engine(
    name: str,
    algorithm: str,
    done_table: str,
    *,
    ensure_tables: tuple[str, ...] = (),
    done_requires_status: bool = True,
    chunk_size: int = 10_000,
    limit_id: int | None = None,
    id_start: int = 0,
    id_end: int | None = None,
    filter_name: str = OutputFilter.FULL.value,
    calculator_kwargs: dict[str, Any] | None = None,
) -> None:
    """Run a batch engine pipeline with the given calculator.

    This is the shared entry point for all batch-mode scripts (eot, quantile,
    pwm_extreme, pwm_hawkes).  It wires up Reader/Writer/Calculator/Engine
    from the batch framework.
    """
    setup_logging(name)
    source_config = SourceConfig(output_filter=OutputFilter(filter_name))

    try:
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
        has_mpi = True
    except ImportError:
        size = 1
        rank = 0
        has_mpi = False

    from lakeanalysis.batch.calculator.factory import CalculatorFactory
    calculator = CalculatorFactory.create(algorithm, **(calculator_kwargs or {}))

    dataset_factory = None
    reader = None
    writer = None

    if not has_mpi or size <= 1:
        from lakeanalysis.batch.lake_dataset_factory import LakeDatasetFactory

        dataset_factory = LakeDatasetFactory.from_config(source_config)
        reader = build_provider_batch_reader(
            source_config,
            done_table=done_table,
            done_requires_status=done_requires_status,
        )
        writer = build_provider_batch_writer(
            source_config,
            ensure_tables=list(ensure_tables),
        )
    elif rank == 0:
        reader = build_provider_batch_reader(
            source_config,
            done_table=done_table,
            done_requires_status=done_requires_status,
        )
        writer = build_provider_batch_writer(
            source_config,
            ensure_tables=list(ensure_tables),
        )
    else:
        from lakeanalysis.batch.lake_dataset_factory import LakeDatasetFactory

        dataset_factory = LakeDatasetFactory.from_config(source_config)

    range_filter = RangeFilter(start=id_start, end=limit_id if limit_id else id_end) if (id_start > 0 or limit_id or id_end) else None
    filter_spec = build_lake_filter(source_config)
    if range_filter is None:
        lake_filter = filter_spec
    elif filter_spec is None:
        lake_filter = range_filter
    elif isinstance(filter_spec, IdSetFilter):
        lake_filter = IdSetFilter(range_filter(filter_spec.ids))
    else:
        lake_filter = range_filter

    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        algorithm=algorithm,
        lake_filter=lake_filter,
        chunk_size=chunk_size,
        dataset_factory=dataset_factory,
    )
    engine.run()


# ── Shared parameter types ──────────────────────────────────────────────────

LimitIdOpt = Annotated[
    int | None,
    typer.Option("--limit-id", "-l", help="Only process first N lakes"),
]

ChunkSizeOpt = Annotated[
    int,
    typer.Option("--chunk-size", "-c", help="Lakes per batch chunk"),
]

FilterNameOpt = Annotated[
    str,
    typer.Option("--filter", help="Named lake filter for input/output routing"),
]

IdStartOpt = Annotated[
    int,
    typer.Option("--id-start", help="Start of hylak_id range"),
]

IdEndOpt = Annotated[
    int | None,
    typer.Option("--id-end", help="End of hylak_id range (exclusive)"),
]

DryRunOpt = Annotated[
    bool,
    typer.Option("--dry-run", help="Print plan only, do not execute"),
]
