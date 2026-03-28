"""DB sink: upsert sampled forcing rows into PostgreSQL via an injected DBClient."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from hydrofetch.db.schema import ensure_forcing_table, upsert_forcing
from hydrofetch.jobs.models import JobRecord, WriteParams
from hydrofetch.write.base import BaseWriter

if TYPE_CHECKING:
    from hydrofetch.db.client import DBClient

log = logging.getLogger(__name__)


class DBWriter(BaseWriter):
    """Write sampled forcing data to a PostgreSQL table.

    The target table is created (or extended with missing band columns) on the
    first write call per process.  Subsequent calls only invoke the upsert, so
    the idempotency overhead is negligible.

    Args:
        params: Write configuration (``db_table`` is read from here).
        db: Authenticated :class:`~hydrofetch.db.client.DBClient` instance
            injected by the factory.  This is the only external dependency;
            the writer itself has no knowledge of connection strings.
    """

    _ensured_tables: set[str] = set()

    def __init__(self, params: WriteParams, db: "DBClient") -> None:
        self._params = params
        self._db = db

    # ------------------------------------------------------------------
    # BaseWriter interface
    # ------------------------------------------------------------------

    def write(self, record: JobRecord) -> None:
        """Upsert sampled forcing rows for *record* into the configured table.

        Args:
            record: Completed or Write-state job record with a valid
                ``local_sample_path``.

        Raises:
            ValueError: If ``local_sample_path`` is not set.
            FileNotFoundError: If the sample Parquet file does not exist.
        """
        if not record.local_sample_path:
            raise ValueError(
                f"Job {record.spec.job_id}: local_sample_path is not set"
            )

        src = Path(record.local_sample_path)
        if not src.is_file():
            raise FileNotFoundError(
                f"Job {record.spec.job_id}: sample file not found at {src}"
            )

        df = pd.read_parquet(str(src))

        # Normalise the id column to "hylak_id" for the DB schema.
        id_col = record.spec.sample.id_column
        if id_col != "hylak_id":
            df = df.rename(columns={id_col: "hylak_id"})

        band_cols = [c for c in df.columns if c not in ("hylak_id", "date")]
        table = self._params.db_table

        with self._db.connection_context() as conn:
            if table not in DBWriter._ensured_tables:
                ensure_forcing_table(conn, table, band_cols)
                DBWriter._ensured_tables.add(table)
            upsert_forcing(conn, table, df)

        log.info(
            "Job %s: upserted %d row(s) into table %r",
            record.spec.job_id,
            len(df),
            table,
        )


__all__ = ["DBWriter"]
