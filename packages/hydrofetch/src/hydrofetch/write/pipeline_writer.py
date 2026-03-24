"""PipelineWriter: fan-out to multiple writer sinks in sequence."""

from __future__ import annotations

import logging

from hydrofetch.jobs.models import JobRecord
from hydrofetch.write.base import BaseWriter

log = logging.getLogger(__name__)


class PipelineWriter(BaseWriter):
    """Execute a list of writers in order for every job record.

    If any writer raises, the exception propagates immediately and subsequent
    writers in the pipeline are not called.  This matches the existing
    single-writer contract: a write failure causes the job to stay in the
    ``WRITE`` state and be retried.

    Args:
        writers: Ordered list of :class:`~hydrofetch.write.base.BaseWriter`
            instances to invoke.  Must contain at least one element.

    Example::

        pipeline = PipelineWriter([FileWriter(params), DBWriter(params, db)])
        pipeline.write(record)
    """

    def __init__(self, writers: list[BaseWriter]) -> None:
        if not writers:
            raise ValueError("PipelineWriter requires at least one writer")
        self._writers = writers

    def write(self, record: JobRecord) -> None:
        for writer in self._writers:
            writer.write(record)
            log.debug(
                "Job %s: %s completed",
                record.spec.job_id,
                type(writer).__name__,
            )


__all__ = ["PipelineWriter"]
