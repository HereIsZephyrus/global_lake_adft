"""Factory: build the appropriate writer pipeline from WriteParams.

Writers are assembled via dependency injection – the factory is responsible
for constructing external dependencies (e.g. :class:`~hydrofetch.db.client.DBClient`)
and injecting them into the concrete writer implementations.  Individual
writers never instantiate their own dependencies.

Supported sink identifiers (``WriteParams.sinks``):
    ``"file"``  – :class:`~hydrofetch.write.file_writer.FileWriter`
    ``"db"``    – :class:`~hydrofetch.write.db_writer.DBWriter`

When *sinks* contains exactly one entry, the bare writer is returned.
When *sinks* contains multiple entries, they are wrapped in a
:class:`~hydrofetch.write.pipeline_writer.PipelineWriter` that executes them
in the order they appear in the list.
"""

from __future__ import annotations

import logging

from hydrofetch.jobs.models import WriteParams
from hydrofetch.write.base import BaseWriter
from hydrofetch.write.file_writer import FileWriter
from hydrofetch.write.pipeline_writer import PipelineWriter

log = logging.getLogger(__name__)


def get_writer(params: WriteParams) -> BaseWriter:
    """Build and return the writer pipeline described by *params*.

    Args:
        params: Write configuration from the job spec.

    Returns:
        A single :class:`BaseWriter` (possibly a :class:`PipelineWriter`).

    Raises:
        ValueError: If *sinks* is empty or contains an unknown sink name.
    """
    sinks = list(params.sinks)
    if not sinks:
        raise ValueError("WriteParams.sinks must contain at least one sink")

    unknown = [s for s in sinks if s not in ("file", "db")]
    if unknown:
        raise ValueError(f"Unknown sink(s): {unknown!r}. Valid values: 'file', 'db'")

    writers: list[BaseWriter] = []

    for sink in sinks:
        if sink == "file":
            writers.append(FileWriter(params))
            log.debug("Adding FileWriter to pipeline")
        elif sink == "db":
            writers.append(_build_db_writer(params))
            log.debug("Adding DBWriter to pipeline")

    if len(writers) == 1:
        return writers[0]

    log.debug("Building PipelineWriter with %d sink(s): %s", len(writers), sinks)
    return PipelineWriter(writers)


def _build_db_writer(params: WriteParams) -> BaseWriter:
    """Construct a DBWriter with an injected DBClient from config."""
    from hydrofetch.db.client import DBClient  # pylint: disable=import-outside-toplevel
    from hydrofetch.write.db_writer import DBWriter  # pylint: disable=import-outside-toplevel

    db = DBClient.from_config()
    log.debug("Injecting DBClient %r into DBWriter", db)
    return DBWriter(params, db)


__all__ = ["get_writer"]
