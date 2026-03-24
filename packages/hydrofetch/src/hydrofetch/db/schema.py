"""DDL helpers and DML for the ERA5 forcing table.

The table schema is flexible: band columns are added dynamically via
``ADD COLUMN IF NOT EXISTS`` so any catalog band set is supported without
manual migrations.

Expected DataFrame columns from :func:`~hydrofetch.sample.raster.sample_raster_at_centroids`:
    ``[<id_column>, "date", <band1>, <band2>, ...]``

The upsert key is always ``(hylak_id, date)``.  The caller must rename the
id column to ``hylak_id`` before calling :func:`upsert_forcing` if a custom
``id_column`` name was used.
"""

from __future__ import annotations

import logging

import pandas as pd
import psycopg
from psycopg import sql

log = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id    INTEGER      NOT NULL,
    date        DATE         NOT NULL,
    ingested_at TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (hylak_id, date)
);
"""

_ADD_COLUMN_SQL = "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION;"


def ensure_forcing_table(
    conn: psycopg.Connection,
    table: str,
    band_columns: list[str],
) -> None:
    """Create *table* and add *band_columns* as ``DOUBLE PRECISION`` if absent.

    This function is idempotent: it is safe to call on every write cycle.

    Args:
        conn: Open psycopg connection.
        table: Target table name (not schema-qualified).
        band_columns: Band column names to ensure exist (e.g. ``["temperature_2m", ...]``).
    """
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(_CREATE_TABLE_SQL).format(table=sql.Identifier(table))
        )
        for col in band_columns:
            cur.execute(
                sql.SQL(_ADD_COLUMN_SQL).format(
                    table=sql.Identifier(table),
                    col=sql.Identifier(col),
                )
            )
    conn.commit()
    log.debug("Ensured forcing table %r with band columns %s", table, band_columns)


def upsert_forcing(
    conn: psycopg.Connection,
    table: str,
    df: pd.DataFrame,
) -> None:
    """Insert or update ERA5 forcing rows from *df* into *table*.

    The DataFrame must have at minimum the columns ``hylak_id``, ``date``, and
    one or more band value columns.  Any ``NaN`` band values are stored as
    ``NULL``.  The upsert key is ``(hylak_id, date)``.

    Args:
        conn: Open psycopg connection.
        table: Target table name.
        df: Forcing DataFrame with columns ``[hylak_id, date, <bands...>]``.
    """
    if df.empty:
        log.debug("Nothing to upsert into %r (empty DataFrame)", table)
        return

    band_cols = [c for c in df.columns if c not in ("hylak_id", "date")]
    all_cols = ["hylak_id", "date"] + band_cols

    tbl = sql.Identifier(table)
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in all_cols)
    placeholders = sql.SQL(", ").join(sql.Placeholder(c) for c in all_cols)
    update_clauses = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
        for c in band_cols
    )

    stmt = sql.SQL(
        "INSERT INTO {tbl} ({cols}) VALUES ({vals}) "
        "ON CONFLICT (hylak_id, date) DO UPDATE SET "
        "{updates}, ingested_at = now()"
    ).format(tbl=tbl, cols=col_idents, vals=placeholders, updates=update_clauses)

    rows = df[all_cols].where(df[all_cols].notna(), other=None).to_dict("records")

    with conn.cursor() as cur:
        cur.executemany(stmt, rows)
    conn.commit()
    log.info("Upserted %d row(s) into table %r", len(rows), table)


__all__ = ["ensure_forcing_table", "upsert_forcing"]
