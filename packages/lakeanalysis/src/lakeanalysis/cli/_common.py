"""Shared typer CLI helpers: common parameters, logging init, data dir."""

from pathlib import Path
from typing import Annotated, Any

import typer

from lakeanalysis.batch import Engine, RangeFilter, build_provider_batch_reader, build_provider_batch_writer
from lakeanalysis.logger import Logger
from lakesource.config import SourceConfig

DATA_DIR = (Path(__file__).resolve().parents[3] / "data").resolve()


def setup_logging(name: str) -> None:
    """Unified Logger initialisation for all CLI commands.

    Replaces the per-script ``Logger("script_name")`` side-effect pattern
    with a single call point.
    """
    Logger(name)


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
    io_budget: int = 4,
    calculator_kwargs: dict[str, Any] | None = None,
) -> None:
    """Run a batch engine pipeline with the given calculator.

    This is the shared entry point for all batch-mode scripts (eot, quantile,
    pwm_extreme, pwm_hawkes).  It wires up Reader/Writer/Calculator/Engine
    from the batch framework.
    """
    setup_logging(name)
    source_config = SourceConfig()

    from lakeanalysis.batch.calculator.factory import CalculatorFactory
    calculator = CalculatorFactory.create(algorithm, **(calculator_kwargs or {}))

    reader = build_provider_batch_reader(
        source_config,
        done_table=done_table,
        done_requires_status=done_requires_status,
    )
    writer = build_provider_batch_writer(
        source_config,
        ensure_tables=list(ensure_tables),
    )

    lake_filter = RangeFilter(start=id_start, end=limit_id if limit_id else id_end) if (id_start > 0 or limit_id or id_end) else None

    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        algorithm=algorithm,
        lake_filter=lake_filter,
        chunk_size=chunk_size,
        io_budget=io_budget,
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

IdStartOpt = Annotated[
    int,
    typer.Option("--id-start", help="Start of hylak_id range"),
]

IdEndOpt = Annotated[
    int | None,
    typer.Option("--id-end", help="End of hylak_id range (exclusive)"),
]

IoBudgetOpt = Annotated[
    int,
    typer.Option("--io-budget", help="Max concurrent DB I/O workers"),
]

DryRunOpt = Annotated[
    bool,
    typer.Option("--dry-run", help="Print plan only, do not execute"),
]
