"""Shared typer CLI helpers: common parameters, logging init, data dir."""

from pathlib import Path
from typing import Annotated

import typer

from lakeanalysis.logger import Logger

DATA_DIR = (Path(__file__).resolve().parents[3] / "data").resolve()


def setup_logging(name: str) -> None:
    """Unified Logger initialisation for all CLI commands.

    Replaces the per-script ``Logger("script_name")`` side-effect pattern
    with a single call point.
    """
    Logger(name)


# ── Shared parameter types ──────────────────────────────────────────────────

LimitIdOpt = Annotated[
    int | None,
    typer.Option("--limit-id", "-l", help="Only process first N lakes"),
]

ChunkSizeOpt = Annotated[
    int,
    typer.Option("--chunk-size", "-c", help="Lakes per batch chunk"),
]

DryRunOpt = Annotated[
    bool,
    typer.Option("--dry-run", help="Print plan only, do not execute"),
]
