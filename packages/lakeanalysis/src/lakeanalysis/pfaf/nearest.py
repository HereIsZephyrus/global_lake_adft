"""In-memory skip-list search for the nearest type-1 (natural) lake.

All data is read from SERIES_DB.  Type-1 lake records are indexed in memory
with one Python dict per Pfafstetter level (lev11 → lev1).  For each type>1
lake the search walks from the finest level downward.  At each level the
candidate set is first filtered by the area-ratio constraint (areas must not
differ by more than a factor of ``max_area_ratio``), then the geographically
nearest passing candidate is returned.  If no candidate in the current level
passes the constraint the search continues to the next coarser level.

This avoids repeated DB round trips: the full dataset is loaded once and all
searches run in Python/NumPy.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import numpy as np
import psycopg

log = logging.getLogger(__name__)

# (topo_level, divisor) pairs ordered finest → coarsest (lev11 → lev1).
# Dividing an 11-digit pfaf_id by the divisor yields its first k digits,
# which is the unique identifier for the level-k ancestor basin.
_LEVELS: list[tuple[int, int]] = [
    (11, 1),
    (10, 10),
    (9, 100),
    (8, 1_000),
    (7, 10_000),
    (6, 100_000),
    (5, 1_000_000),
    (4, 10_000_000),
    (3, 100_000_000),
    (2, 1_000_000_000),
    (1, 10_000_000_000),
]

_FETCH_TYPE1_SQL = """
SELECT li.hylak_id,
       lp.pfaf_id::bigint AS pfaf_id,
       ST_Y(li.centroid)  AS lat,
       ST_X(li.centroid)  AS lon,
       li.lake_area
FROM   lake_info li
JOIN   lake_pfaf  lp USING (hylak_id)
WHERE  li.lake_type = 1
  AND  lp.pfaf_id IS NOT NULL;
"""

_FETCH_NON_TYPE1_SQL = """
SELECT li.hylak_id,
       li.lake_type,
       lp.pfaf_id::bigint AS pfaf_id,
       ST_Y(li.centroid)  AS lat,
       ST_X(li.centroid)  AS lon,
       li.lake_area
FROM   lake_info li
JOIN   lake_pfaf  lp USING (hylak_id)
WHERE  li.lake_type > 1
  AND  lp.pfaf_id IS NOT NULL;
"""

_FETCH_NON_TYPE1_LIMITED_SQL = """
SELECT li.hylak_id,
       li.lake_type,
       lp.pfaf_id::bigint AS pfaf_id,
       ST_Y(li.centroid)  AS lat,
       ST_X(li.centroid)  AS lon,
       li.lake_area
FROM   lake_info li
JOIN   lake_pfaf  lp USING (hylak_id)
WHERE  li.lake_type > 1
  AND  lp.pfaf_id IS NOT NULL
  AND  li.hylak_id < %(limit_id)s;
"""


class _Type1Index:
    """Skip-list index over type-1 lakes keyed by Pfafstetter prefix at each level.

    One dict per level maps prefix_value → list of row indices.  The search
    walks from lev11 (finest) to lev1 (coarsest).  At each level, candidates
    whose area ratio relative to the query lake exceeds ``max_area_ratio`` are
    excluded before selecting the geographically nearest survivor.  The search
    continues to the next coarser level when no candidate passes the constraint.
    """

    def __init__(
        self,
        hylak_ids: list[int],
        pfaf_ids: list[int],
        lats: list[float],
        lons: list[float],
        areas: list[float | None],
    ) -> None:
        self._hylak_ids = hylak_ids
        self._coords = np.array(list(zip(lats, lons)), dtype=np.float64)
        # NaN for NULL / non-positive areas so they are excluded from ratio filtering.
        self._areas = np.array(
            [a if (a is not None and a > 0) else np.nan for a in areas],
            dtype=np.float64,
        )

        # Build one prefix dict per level
        self._level_dicts: list[dict[int, list[int]]] = []
        for _lvl, div in _LEVELS:
            d: dict[int, list[int]] = defaultdict(list)
            for i, pfaf in enumerate(pfaf_ids):
                d[pfaf // div].append(i)
            self._level_dicts.append(dict(d))

    def find_nearest(
        self,
        pfaf_id: int,
        lat: float,
        lon: float,
        area: float | None,
        max_area_ratio: float = 10.0,
    ) -> tuple[int, int] | None:
        """Return (hylak_id, topo_level) of the nearest qualifying type-1 lake.

        At each Pfafstetter level (finest first), candidates are filtered to
        those whose area ratio with the query lake does not exceed
        ``max_area_ratio`` (i.e. neither lake is more than ``max_area_ratio``×
        larger than the other).  Among the survivors the geographically nearest
        is returned.  The search moves to the next coarser level when the
        current level has no qualifying candidates.

        When either the query area or a candidate area is NULL / non-positive
        the area constraint is skipped for that pair, so the candidate remains
        eligible.

        Args:
            pfaf_id: 11-digit Pfafstetter code of the query lake.
            lat: Latitude of the query lake centroid.
            lon: Longitude of the query lake centroid.
            area: Lake area of the query lake (km²), or None if unknown.
            max_area_ratio: Maximum allowed ratio between the two areas.
                Defaults to 2.0 (neither lake more than twice the other).

        Returns:
            Tuple (nearest_hylak_id, topo_level) or None if no qualifying
            type-1 lake shares any Pfafstetter prefix with the query lake.
        """
        pt = np.array([lat, lon], dtype=np.float64)
        query_area = float(area) if (area is not None and area > 0) else np.nan

        for level_idx, (topo_level, div) in enumerate(_LEVELS):
            key = pfaf_id // div
            candidates = self._level_dicts[level_idx].get(key)
            if not candidates:
                continue

            idxs = np.array(candidates, dtype=np.intp)

            # Apply area-ratio filter.  Pairs where either area is NaN bypass
            # the constraint (treat as always eligible).
            cand_areas = self._areas[idxs]
            if not np.isnan(query_area):
                ratio = np.where(
                    np.isnan(cand_areas),
                    1.0,                              # NaN candidate → eligible
                    np.maximum(cand_areas / query_area, query_area / cand_areas),
                )
                idxs = idxs[ratio <= max_area_ratio]

            if idxs.size == 0:
                continue

            cand_coords = self._coords[idxs]
            dists = np.linalg.norm(cand_coords - pt, axis=1)
            best_idx = idxs[int(np.argmin(dists))]
            return self._hylak_ids[best_idx], topo_level

        return None


def _build_type1_index(conn: psycopg.Connection) -> _Type1Index:
    """Load all type-1 lakes from SERIES_DB and build a skip-list index.

    Args:
        conn: An open psycopg connection to SERIES_DB.

    Returns:
        A :class:`_Type1Index` ready for nearest-lake queries.
    """
    with conn.cursor() as cur:
        cur.execute(_FETCH_TYPE1_SQL)
        rows = cur.fetchall()

    hylak_ids = [int(r[0])   for r in rows]
    pfaf_ids  = [int(r[1])   for r in rows]
    lats      = [float(r[2]) for r in rows]
    lons      = [float(r[3]) for r in rows]
    areas     = [float(r[4]) if r[4] is not None else None for r in rows]

    log.info("Loaded %d type-1 lake(s) into skip-list index", len(hylak_ids))
    return _Type1Index(hylak_ids, pfaf_ids, lats, lons, areas)


def compute_nearest_naturals(
    conn: psycopg.Connection,
    limit_id: int | None = None,
    max_area_ratio: float = 2.0,
) -> list[dict]:
    """Find the nearest type-1 lake for every type>1 lake in SERIES_DB.

    Loads all relevant records in two queries, builds the in-memory index,
    then performs the hierarchical skip-list search entirely in Python with
    no additional DB round trips.

    The result list includes a ``topo_level`` field (1–11) indicating how
    many Pfafstetter prefix levels are shared: 11 = same lev11 basin (most
    levels in common), 1 = only the continent-level basin in common.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        limit_id: If given, only type>1 lakes with hylak_id < limit_id are
            searched (type-1 index always loads the full dataset).
        max_area_ratio: Maximum ratio between the areas of the matched pair.
            Defaults to 10.0 (neither lake more than 10× the size of the
            other).  Set to ``float("inf")`` to disable the constraint.

    Returns:
        List of dicts with keys:
          - ``hylak_id``   (int)        : source lake
          - ``lake_type``  (int)        : lake type (2 or 3)
          - ``nearest_id`` (int | None) : nearest qualifying type-1 hylak_id
          - ``topo_level`` (int | None) : number of shared Pfafstetter prefix levels
    """
    index = _build_type1_index(conn)

    if limit_id is None:
        sql = _FETCH_NON_TYPE1_SQL
        params = None
    else:
        sql = _FETCH_NON_TYPE1_LIMITED_SQL
        params = {"limit_id": limit_id}

    with conn.cursor() as cur:
        cur.execute(sql, params)
        non_type1_rows = cur.fetchall()

    log.info(
        "Loaded %d type>1 lake(s) to search (max_area_ratio=%.1f)",
        len(non_type1_rows),
        max_area_ratio,
    )

    results: list[dict] = []
    matched = 0

    for hylak_id, lake_type, pfaf_id, lat, lon, area in non_type1_rows:
        query_area = float(area) if area is not None else None
        hit = index.find_nearest(
            int(pfaf_id), float(lat), float(lon), query_area, max_area_ratio
        )
        if hit is not None:
            nearest_id, topo_level = hit
            matched += 1
        else:
            nearest_id, topo_level = None, None

        results.append(
            {
                "hylak_id":   int(hylak_id),
                "lake_type":  int(lake_type),
                "nearest_id": nearest_id,
                "topo_level": topo_level,
            }
        )

    log.info(
        "Nearest-natural search complete: %d/%d matched",
        matched,
        len(non_type1_rows),
    )
    return results
