"""Database operations for misc tables: comparison, interpolation_detect, frozen_months."""

from __future__ import annotations

from typing import Any
import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE, year_month_key_sql

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _fetch_frozen_year_months_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym_key}
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
  AND anomaly_type = 'frozen'
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("anomaly")),
        ym_key=year_month_key_sql(),
    )


def _fetch_frozen_year_months_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym_key}
FROM {table}
WHERE {chunk}
  AND anomaly_type = 'frozen'
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("anomaly")),
        chunk=sql.SQL(CHUNK_WHERE),
    )


def _fetch_seasonal_amplitude_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id, annual_means_std, mean_area
FROM {{table}}
WHERE {CHUNK_WHERE}
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _fetch_linear_trend_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, linear_trend_of_stl_trend_per_period
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _fetch_max_lake_info_hylak_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT MAX(hylak_id)
FROM {table}
""").format(table=sql.Identifier(tc.series_table("lake_area")))


def _count_source_hylak_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT COUNT(*)
FROM {{table}}
WHERE {CHUNK_WHERE}
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _fetch_source_hylak_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id
FROM {{table}}
WHERE {CHUNK_WHERE}
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _fetch_anomaly_hylak_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT DISTINCT hylak_id
FROM {table}
""").format(table=sql.Identifier(tc.series_table("area_anomalies")))


def _fetch_quality_done_hylak_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT DISTINCT la.hylak_id
FROM {{lake_area}} la
JOIN {{area_quality}} aq ON aq.hylak_id = la.hylak_id
WHERE {CHUNK_WHERE}
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
    )


def _fetch_area_quality_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT DISTINCT la.hylak_id
FROM {{lake_area}} la
JOIN {{area_quality}} aq ON aq.hylak_id = la.hylak_id
WHERE {CHUNK_WHERE}
ORDER BY la.hylak_id
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
    )


def _count_area_quality_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT COUNT(DISTINCT la.hylak_id)
FROM {{lake_area}} la
JOIN {{area_quality}} aq ON aq.hylak_id = la.hylak_id
WHERE {CHUNK_WHERE}
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
    )


def _ensure_comparison_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id         BIGINT        NOT NULL,
    workflow_version VARCHAR(64)   NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    error_message    TEXT,
    created_at       TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version)
);
""").format(table=sql.Identifier(tc.series_table("comparison_run_status")))


def _upsert_comparison_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, status, error_message, created_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    created_at    = now();
""").format(
        table=sql.Identifier(tc.series_table("comparison_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "workflow_version")
        ),
    )


def _fetch_comparison_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id
FROM {{table}}
WHERE {CHUNK_WHERE}
  AND status = 'done'
  AND workflow_version = %(workflow_version)s
""").format(table=sql.Identifier(tc.series_table("comparison_run_status")))


def _ensure_interpolation_detect_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id           INTEGER      PRIMARY KEY,
    n_linear_segments  INTEGER      NOT NULL,
    n_flat_segments    INTEGER      NOT NULL,
    max_linear_len     INTEGER      NOT NULL,
    max_flat_len       INTEGER      NOT NULL,
    collinear_ratio    DOUBLE PRECISION NOT NULL,
    first_linear_ym    INTEGER,
    n_obs              INTEGER      NOT NULL,
    computed_at        TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("interpolation_detect")))


def _upsert_interpolation_detect_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, n_linear_segments, n_flat_segments, max_linear_len,
    max_flat_len, collinear_ratio, first_linear_ym, n_obs, computed_at
) VALUES (
    %(hylak_id)s, %(n_linear_segments)s, %(n_flat_segments)s, %(max_linear_len)s,
    %(max_flat_len)s, %(collinear_ratio)s, %(first_linear_ym)s, %(n_obs)s, now()
)
ON CONFLICT (hylak_id) DO UPDATE SET
    n_linear_segments = EXCLUDED.n_linear_segments,
    n_flat_segments   = EXCLUDED.n_flat_segments,
    max_linear_len    = EXCLUDED.max_linear_len,
    max_flat_len      = EXCLUDED.max_flat_len,
    collinear_ratio   = EXCLUDED.collinear_ratio,
    first_linear_ym   = EXCLUDED.first_linear_ym,
    n_obs             = EXCLUDED.n_obs,
    computed_at       = now();
""").format(table=sql.Identifier(tc.series_table("interpolation_detect")))


def fetch_frozen_year_months_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, set[int]]:
    """Fetch YYYYMM keys flagged as frozen for the given hylak_ids."""
    if not id_list:
        return {}
    params = {"id_list": id_list}
    with conn.cursor() as cur:
        cur.execute(_fetch_frozen_year_months_by_ids_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, set[int]] = {int(hid): set() for hid in id_list}
    for hylak_id, year_month_key in rows:
        result[int(hylak_id)].add(int(year_month_key))
    log.debug("Fetched frozen anomaly months for %d lake(s)", len(result))
    return result


def fetch_frozen_year_months_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, set[int]]:
    """Fetch YYYYMM keys flagged as frozen for a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_frozen_year_months_chunk_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, set[int]] = {}
    for hylak_id, year_month_key in rows:
        hid = int(hylak_id)
        if hid not in result:
            result[hid] = set()
        result[hid].add(int(year_month_key))
    log.debug(
        "Fetched frozen months for chunk [%d, %d): %d lake(s) with frozen months",
        chunk_start,
        chunk_end,
        len(result),
    )
    return result


def fetch_seasonal_amplitude_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float | None]:
    """Fetch CV from lake_info: annual_means_std / mean_area."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_seasonal_amplitude_chunk_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, float | None] = {}
    for r in rows:
        hylak_id = int(r[0])
        annual_means_std = float(r[1]) if r[1] is not None else None
        mean_area = float(r[2]) if r[2] is not None else None
        if annual_means_std is not None and mean_area is not None and mean_area > 0:
            result[hylak_id] = annual_means_std / mean_area
        else:
            result[hylak_id] = None
    return result


def fetch_linear_trend_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float | None]:
    """Fetch linear trend from lake_info for the given hylak_ids."""
    if not id_list:
        return {}
    params = {"id_list": id_list}
    with conn.cursor() as cur:
        cur.execute(_fetch_linear_trend_by_ids_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, float | None] = {}
    for r in rows:
        hylak_id = int(r[0])
        trend = float(r[1]) if r[1] is not None else None
        result[hylak_id] = trend
    for hid in id_list:
        if hid not in result:
            result[hid] = None
    log.debug("Fetched linear_trend for %d lake(s)", len(result))
    return result


def fetch_anomaly_hylak_ids(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch all hylak_id values from area_anomalies."""
    with conn.cursor() as cur:
        cur.execute(_fetch_anomaly_hylak_ids_sql(table_config))
        rows = cur.fetchall()
    result = {int(r[0]) for r in rows}
    log.info("Fetched anomaly hylak_ids: %d lakes", len(result))
    return result


def fetch_quality_done_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch all quality-processed lake ids in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_quality_done_hylak_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}


def fetch_max_lake_info_hylak_id(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> int | None:
    """Return the maximum hylak_id present in lake_info."""
    with conn.cursor() as cur:
        cur.execute(_fetch_max_lake_info_hylak_id_sql(table_config))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def count_source_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    """Count source lake ids from lake_info in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_count_source_hylak_ids_in_range_sql(table_config), params)
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def fetch_source_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch source lake ids from lake_info in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_source_hylak_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}


def ensure_comparison_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_comparison_run_status_table_sql(table_config))
    conn.commit()


def upsert_comparison_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_comparison_run_status_sql(table_config), rows)
    if commit:
        conn.commit()


def fetch_comparison_status_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "workflow_version": workflow_version,
    }
    with conn.cursor() as cur:
        cur.execute(
            _fetch_comparison_status_ids_in_range_sql(table_config),
            params,
        )
        return {int(row[0]) for row in cur.fetchall()}


def ensure_interpolation_detect_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_interpolation_detect_table_sql(table_config))
    conn.commit()
    log.debug("Ensured interpolation_detect table exists")


def upsert_interpolation_detect(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_interpolation_detect_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d interpolation_detect row(s)", len(rows))
