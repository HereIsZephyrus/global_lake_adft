"""DDL helpers and DML for the ERA5 forcing table.

``ensure_forcing_table`` creates the table (with all band columns) using
``CREATE TABLE IF NOT EXISTS``.  This only acquires a ShareLock, which is
fully compatible with concurrent DML — no ``ALTER TABLE`` and therefore no
``AccessExclusiveLock`` is ever needed.

``upsert_forcing`` bulk-loads rows via ``COPY`` into a temporary staging
table, then merges them into the target table with a single
``INSERT … ON CONFLICT DO UPDATE``.  This is 10–30× faster than
``executemany`` and holds a ``RowExclusiveLock`` for seconds instead of
minutes.
"""

from __future__ import annotations

import logging

import pandas as pd
import psycopg
from psycopg import sql

log = logging.getLogger(__name__)


def ensure_forcing_table(
    conn: psycopg.Connection,
    table: str,
    band_columns: list[str],
) -> None:
    """Create *table* with all *band_columns* in a single DDL statement.

    Uses ``CREATE TABLE IF NOT EXISTS`` (ShareLock only) so multiple
    processes can call this concurrently without blocking each other or
    any ongoing DML.

    Args:
        conn: Open psycopg connection.
        table: Target table name (not schema-qualified).
        band_columns: Band column names (e.g. ``["temperature_2m", ...]``).
    """
    band_defs = sql.SQL(",\n    ").join(
        sql.SQL("{col} DOUBLE PRECISION").format(col=sql.Identifier(c))
        for c in band_columns
    )

    create_stmt = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {table} (\n"
        "    hylak_id    INTEGER      NOT NULL,\n"
        "    date        DATE         NOT NULL,\n"
        "    {bands},\n"
        "    ingested_at TIMESTAMPTZ  DEFAULT now(),\n"
        "    PRIMARY KEY (hylak_id, date)\n"
        ")"
    ).format(table=sql.Identifier(table), bands=band_defs)

    with conn.cursor() as cur:
        cur.execute(create_stmt)
    conn.commit()
    log.debug("Ensured forcing table %r with band columns %s", table, band_columns)


def upsert_forcing(
    conn: psycopg.Connection,
    table: str,
    df: pd.DataFrame,
) -> None:
    """Bulk-upsert ERA5 forcing rows from *df* into *table*.

    Strategy:
        1. ``COPY`` the DataFrame into a session-local ``TEMP TABLE``.
        2. ``INSERT INTO … SELECT … ON CONFLICT DO UPDATE`` from the
           staging table into the real table.

    This avoids per-row network round-trips and keeps the
    ``RowExclusiveLock`` on *table* for the duration of a single
    ``INSERT … SELECT`` rather than hundreds of thousands of individual
    ``INSERT`` calls.

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
    col_list = sql.SQL(", ").join(sql.Identifier(c) for c in all_cols)
    update_set = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
        for c in band_cols
    )

    staging = "_hydrofetch_staging"

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {stg} (LIKE {tbl} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                stg=sql.Identifier(staging), tbl=tbl,
            )
        )

    # Stream rows into the staging table via COPY.
    copy_cmd = sql.SQL("COPY {stg} ({cols}) FROM STDIN").format(
        stg=sql.Identifier(staging), cols=col_list,
    )
    clean = df[all_cols].where(df[all_cols].notna(), other=None)

    with conn.cursor() as cur:
        with cur.copy(copy_cmd) as copy:
            for row in clean.itertuples(index=False, name=None):
                copy.write_row(row)

    # Merge staging → target in one bulk INSERT … ON CONFLICT.
    merge_stmt = sql.SQL(
        "INSERT INTO {tbl} ({cols}) "
        "SELECT {cols} FROM {stg} "
        "ON CONFLICT (hylak_id, date) DO UPDATE SET "
        "{updates}, ingested_at = now()"
    ).format(tbl=tbl, cols=col_list, stg=sql.Identifier(staging), updates=update_set)

    with conn.cursor() as cur:
        cur.execute(merge_stmt)
    conn.commit()
    log.info("Upserted %d row(s) into table %r", len(df), table)


__all__ = ["ensure_forcing_table", "upsert_forcing"]
