"""Fetch af_nearest (topo_level>8) and lake_area data via dbconnect."""

from __future__ import annotations

import logging

import pandas as pd
import psycopg

from lakesource.postgres import fetch_af_nearest_high_topo, fetch_lake_area_by_ids

log = logging.getLogger(__name__)


def load_pairs_and_areas(
    conn: psycopg.Connection,
) -> tuple[list[dict], dict[int, pd.DataFrame]]:
    """Load af_nearest pairs (topo_level>8) and lake_area for all involved hylak_ids.

    Args:
        conn: An open psycopg connection to SERIES_DB (caller uses dbconnect).

    Returns:
        (pairs, lake_frames): pairs is list of dicts with hylak_id, nearest_id, topo_level;
        lake_frames maps hylak_id to DataFrame with columns [year, month, water_area].
    """
    pairs = fetch_af_nearest_high_topo(conn)
    if not pairs:
        return pairs, {}

    all_ids = set()
    for row in pairs:
        all_ids.add(row["hylak_id"])
        all_ids.add(row["nearest_id"])
    id_list = sorted(all_ids)

    lake_frames = fetch_lake_area_by_ids(conn, id_list)
    return pairs, lake_frames
