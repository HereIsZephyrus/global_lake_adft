"""Spatial lookup of Pfafstetter basin IDs for lakes.

Two-pass strategy:
  1. Spatial join with BasinATLAS_v10_lev05 (few thousand basins) to obtain the
     5-digit pfaf prefix for each lake centroid.
  2. Filter BasinATLAS_v10_lev11 by prefix (b11.pfaf_id / 1_000_000 = lev5_pfaf)
     before the second spatial join, drastically reducing the search space.

Cross-database approach:
  lake_info.centroid (a pre-computed Point geometry) is fetched from SERIES_DB
  as WKT strings, then bulk-inserted into a temporary table in ALTAS_DB via
  executemany + ST_GeomFromText.  The two-pass CTE then runs entirely within
  ALTAS_DB.
"""

from __future__ import annotations

import logging

import psycopg

log = logging.getLogger(__name__)

_FETCH_CENTROIDS_SQL = """
SELECT hylak_id, ST_AsText(centroid) AS centroid_wkt
FROM lake_info
"""

_FETCH_CENTROIDS_LIMITED_SQL = """
SELECT hylak_id, ST_AsText(centroid) AS centroid_wkt
FROM lake_info
WHERE hylak_id < %(limit_id)s
"""

_FETCH_CENTROIDS_RANGE_SQL = """
SELECT hylak_id, ST_AsText(centroid) AS centroid_wkt
FROM lake_info
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
"""

_CREATE_TEMP_TABLE_SQL = """
CREATE TEMP TABLE _lake_centroids (
    hylak_id  INTEGER,
    centroid  GEOMETRY(Point, 4326)
) ON COMMIT DROP;
"""

_INSERT_CENTROID_SQL = """
INSERT INTO _lake_centroids (hylak_id, centroid)
VALUES (%s, ST_SetSRID(ST_GeomFromText(%s), 4326))
"""

# Single-pass spatial join: match each centroid against lev05 first (range
# filter on pfaf_id keeps the B-tree index usable and avoids applying a
# function to the column), then narrow to the matching lev11 basin.
# pfaf_id is stored as double precision, so a range condition is used instead
# of integer division to avoid fractional results.
_LOOKUP_PFAF_SQL = """
SELECT lc.hylak_id,
       b11.pfaf_id
FROM   _lake_centroids lc
JOIN   "BasinATLAS_v10_lev05" b5
  ON   ST_Contains(b5.geom, lc.centroid)
JOIN   "BasinATLAS_v10_lev11" b11
  ON   b11.pfaf_id >= b5.pfaf_id * 1000000.0
 AND   b11.pfaf_id <  (b5.pfaf_id + 1) * 1000000.0
 AND   ST_Contains(b11.geom, lc.centroid);
"""


def fetch_lake_centroids(
    series_conn: psycopg.Connection,
    limit_id: int | None = None,
) -> list[tuple[int, str]]:
    """Fetch (hylak_id, centroid_wkt) pairs from lake_info in SERIES_DB.

    Reads the pre-computed ``centroid`` geometry column directly; no
    ST_Centroid() computation is performed.

    Args:
        series_conn: An open psycopg connection to SERIES_DB.
        limit_id: If given, only rows with hylak_id < limit_id are returned
            (useful for testing).

    Returns:
        List of (hylak_id, wkt_point) tuples.
    """
    if limit_id is None:
        sql = _FETCH_CENTROIDS_SQL
        params = None
    else:
        sql = _FETCH_CENTROIDS_LIMITED_SQL
        params = {"limit_id": limit_id}

    with series_conn.cursor() as cur:
        cur.execute(sql, params)
        rows = [(int(row[0]), row[1]) for row in cur.fetchall()]
    log.debug("Fetched %d centroid(s) from lake_info", len(rows))
    return rows


def fetch_lake_centroids_chunk(
    series_conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> list[tuple[int, str]]:
    """Fetch (hylak_id, centroid_wkt) pairs for a specific hylak_id range.

    Retrieves only rows in [chunk_start, chunk_end) so that callers can
    process large datasets incrementally without loading the full table.

    Args:
        series_conn: An open psycopg connection to SERIES_DB.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.

    Returns:
        List of (hylak_id, wkt_point) tuples for the requested range.
    """
    with series_conn.cursor() as cur:
        cur.execute(
            _FETCH_CENTROIDS_RANGE_SQL,
            {"chunk_start": chunk_start, "chunk_end": chunk_end},
        )
        rows = [(int(row[0]), row[1]) for row in cur.fetchall()]
    log.debug("Fetched %d centroid(s) for chunk [%d, %d)", len(rows), chunk_start, chunk_end)
    return rows


def lookup_pfaf_chunk(
    atlas_conn: psycopg.Connection,
    centroids: list[tuple[int, str]],
) -> dict[int, int | None]:
    """Run the two-pass spatial join for a pre-fetched list of centroids.

    This is the core computation used by both the full-table path
    (``lookup_pfaf_ids``) and the chunked path.  Callers are responsible for
    fetching ``centroids`` and for committing or rolling back ``atlas_conn``
    after this function returns (the function calls ``atlas_conn.commit()``
    internally to release the temporary table created by ``ON COMMIT DROP``).

    Args:
        atlas_conn: An open psycopg connection to ALTAS_DB (PostGIS).
        centroids: List of (hylak_id, wkt_point) tuples to look up.

    Returns:
        Dict mapping each hylak_id to its lev11 pfaf_id (INTEGER), or None if
        the centroid falls outside all known lev5 basins.
    """
    if not centroids:
        log.debug("lookup_pfaf_chunk: no centroids, returning empty")
        return {}

    result: dict[int, int | None] = {hylak_id: None for hylak_id, _ in centroids}
    log.debug("lookup_pfaf_chunk: %d centroid(s), single-pass spatial join", len(centroids))

    with atlas_conn.cursor() as cur:
        cur.execute(_CREATE_TEMP_TABLE_SQL)
        cur.executemany(_INSERT_CENTROID_SQL, centroids)

        cur.execute(_LOOKUP_PFAF_SQL)
        for hylak_id, pfaf_id in cur.fetchall():
            result[int(hylak_id)] = int(pfaf_id)

    atlas_conn.commit()
    matched = sum(1 for v in result.values() if v is not None)
    log.debug("lookup_pfaf_chunk: %d/%d matched", matched, len(result))
    return result


def lookup_pfaf_ids(
    atlas_conn: psycopg.Connection,
    series_conn: psycopg.Connection,
    limit_id: int | None = None,
) -> dict[int, int | None]:
    """Look up the lev11 Pfafstetter basin ID for each lake in SERIES_DB.

    Fetches all matching centroids from SERIES_DB in one query, then
    delegates the two-pass spatial join to ``lookup_pfaf_chunk``.  For
    large datasets (> ~100 k lakes) prefer the chunked pipeline via
    ``ChunkedLakeProcessor`` in ``dbconnect.chunked``.

    Args:
        atlas_conn: An open psycopg connection to ALTAS_DB (PostGIS).
        series_conn: An open psycopg connection to SERIES_DB.
        limit_id: If given, only lakes with hylak_id < limit_id are processed.

    Returns:
        Dict mapping each hylak_id to its lev11 pfaf_id (INTEGER), or None if
        the centroid falls outside all known lev5 basins (e.g. ice sheets or
        endorheic lakes not covered by HydroATLAS).
    """
    centroids = fetch_lake_centroids(series_conn, limit_id=limit_id)
    return lookup_pfaf_chunk(atlas_conn, centroids)
