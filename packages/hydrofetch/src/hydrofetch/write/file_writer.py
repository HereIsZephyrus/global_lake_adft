"""File-based sink: copy staged sample to the configured output directory."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd

from hydrofetch.jobs.models import JobRecord, WriteParams
from hydrofetch.write.base import BaseWriter

log = logging.getLogger(__name__)


class FileWriter(BaseWriter):
    """Persist sampled Parquet output in the configured output directory.

    In v1, the sample file is already written to the sample directory by
    :class:`~hydrofetch.state_machine.sample.SampleState`.  This writer
    optionally copies it to a separate final output directory (``output_dir``)
    and may convert the format to CSV if requested.

    Args:
        params: ``WriteParams`` controlling destination and format.
    """

    def __init__(self, params: WriteParams) -> None:
        self._params = params
        self._output_dir = Path(params.output_dir).expanduser().resolve()

    def write(self, record: JobRecord) -> None:
        if not record.local_sample_path:
            raise ValueError(
                f"Job {record.spec.job_id}: local_sample_path is not set"
            )

        src = Path(record.local_sample_path)
        if not src.is_file():
            raise FileNotFoundError(
                f"Job {record.spec.job_id}: sample file not found at {src}"
            )

        self._output_dir.mkdir(parents=True, exist_ok=True)
        fmt = self._params.output_format.lower()

        if fmt == "parquet":
            dest = self._output_dir / src.name
            if not dest.is_file():
                shutil.copy2(src, dest)
                log.info("Job %s: wrote parquet to %s", record.spec.job_id, dest)
            else:
                log.debug(
                    "Job %s: output already exists at %s, skipping copy",
                    record.spec.job_id,
                    dest,
                )
            return

        if fmt == "csv":
            stem = src.stem
            dest = self._output_dir / f"{stem}.csv"
            if not dest.is_file():
                df = pd.read_parquet(str(src))
                df.to_csv(str(dest), index=False)
                log.info("Job %s: wrote csv to %s", record.spec.job_id, dest)
            else:
                log.debug(
                    "Job %s: csv output already exists at %s",
                    record.spec.job_id,
                    dest,
                )
            return

        raise ValueError(
            f"Unsupported output_format {self._params.output_format!r}; "
            "use 'parquet' or 'csv'"
        )


__all__ = ["FileWriter"]
