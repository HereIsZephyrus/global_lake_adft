"""Database operations for persisting pfaf lookups and nearest-natural results in SERIES_DB."""

from __future__ import annotations

import logging

import psycopg

log = logging.getLogger(__name__)

_ENSURE_LAKE_PFAF_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lake_pfaf (
    hylak_id    INTEGER PRIMARY KEY,
    pfaf_id     BIGINT,
    computed_at TIMESTAMPTZ DEFAULT now()
);
"""

_UPSERT_LAKE_PFAF_SQL = """
INSERT INTO lake_pfaf (hylak_id, pfaf_id, computed_at)
VALUES (%(hylak_id)s, %(pfaf_id)s, now())
ON CONFLICT (hylak_id) DO UPDATE SET
    pfaf_id     = EXCLUDED.pfaf_id,
    computed_at = now();
"""


def ensure_lake_pfaf_table(conn: psycopg.Connection) -> None:
    """Create the lake_pfaf table in SERIES_DB if it does not already exist.

    Args:
        conn: An open psycopg connection to SERIES_DB.
    """
    with conn.cursor() as cur:
        cur.execute(_ENSURE_LAKE_PFAF_TABLE_SQL)
    conn.commit()
    log.debug("Ensured lake_pfaf table exists")


_ENSURE_AF_NEAREST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS af_nearest (
    hylak_id    INTEGER PRIMARY KEY,
    lake_type   SMALLINT NOT NULL,
    nearest_id  INTEGER,
    topo_level  SMALLINT,
    computed_at TIMESTAMPTZ DEFAULT now()
);
"""

_UPSERT_AF_NEAREST_SQL = """
INSERT INTO af_nearest (hylak_id, lake_type, nearest_id, topo_level, computed_at)
VALUES (%(hylak_id)s, %(lake_type)s, %(nearest_id)s, %(topo_level)s, now())
ON CONFLICT (hylak_id) DO UPDATE SET
    lake_type   = EXCLUDED.lake_type,
    nearest_id  = EXCLUDED.nearest_id,
    topo_level  = EXCLUDED.topo_level,
    computed_at = now();
"""


def ensure_af_nearest_table(conn: psycopg.Connection) -> None:
    """Create the af_nearest table in SERIES_DB if it does not already exist.

    Args:
        conn: An open psycopg connection to SERIES_DB.
    """
    with conn.cursor() as cur:
        cur.execute(_ENSURE_AF_NEAREST_TABLE_SQL)
    conn.commit()
    log.debug("Ensured af_nearest table exists")


def upsert_af_nearest(conn: psycopg.Connection, rows: list[dict]) -> None:
    """Insert or update rows in af_nearest.

    Each dict must contain keys: hylak_id, lake_type, nearest_id, topo_level.
    nearest_id and topo_level may be None for lakes outside HydroATLAS coverage.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        rows: List of result dicts produced by :func:`pfaf.nearest.compute_nearest_naturals`.
    """
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_AF_NEAREST_SQL, rows)
    conn.commit()
    log.info("Upserted %d af_nearest row(s)", len(rows))


def upsert_lake_pfaf(conn: psycopg.Connection, mapping: dict[int, int | None]) -> None:
    """Insert or update hylak_id → pfaf_id rows in lake_pfaf.

    Rows where pfaf_id is None (lake centroid outside HydroATLAS coverage) are
    stored with pfaf_id = NULL so that the record is still present and can be
    distinguished from lakes that were never processed.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        mapping: Dict mapping hylak_id to lev11 pfaf_id (or None).
    """
    rows = [{"hylak_id": hid, "pfaf_id": pfaf} for hid, pfaf in mapping.items()]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_LAKE_PFAF_SQL, rows)
    conn.commit()
    log.info("Upserted %d lake_pfaf row(s)", len(rows))
