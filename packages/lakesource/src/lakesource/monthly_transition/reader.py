"""Unified read interface for monthly transition data with backend dispatch."""

from __future__ import annotations

import pandas as pd
from psycopg import sql as psql

from lakesource.config import Backend, SourceConfig
from lakesource.table_config import TableConfig


def _extremes_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT e.hylak_id, e.event_type, e.year, e.month,
       ST_Y(l.centroid) AS lat, ST_X(l.centroid) AS lon
FROM   {extremes} e
JOIN   {lake_info} l USING (hylak_id)
WHERE  e.workflow_version = %(workflow_version)s
""").format(
        extremes=psql.Identifier(tc.series_table("monthly_transition_extremes")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _transitions_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT t.hylak_id, t.transition_type, t.from_year, t.from_month,
       ST_Y(l.centroid) AS lat, ST_X(l.centroid) AS lon
FROM   {transitions} t
JOIN   {lake_info} l USING (hylak_id)
WHERE  t.workflow_version = %(workflow_version)s
""").format(
        transitions=psql.Identifier(
            tc.series_table("monthly_transition_abrupt_transitions")
        ),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _lake_coords_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT hylak_id, ST_Y(centroid) AS lat, ST_X(centroid) AS lon
FROM   {lake_info}
""").format(lake_info=psql.Identifier(tc.series_table("lake_info")))


def _build_time_filters(config: SourceConfig) -> tuple[psql.Composed, dict]:
    clauses: list[psql.Composed] = []
    params: dict = {"workflow_version": config.workflow_version}
    if config.year_start is not None:
        clauses.append(psql.SQL("AND year >= %(year_start)s"))
        params["year_start"] = config.year_start
    if config.year_end is not None:
        clauses.append(psql.SQL("AND year <= %(year_end)s"))
        params["year_end"] = config.year_end
    return psql.SQL(" ").join(clauses), params


def _fetch_extremes_postgres(config: SourceConfig) -> pd.DataFrame:
    from lakesource.postgres import series_db

    time_clause, params = _build_time_filters(config)
    sql = _extremes_sql(config.t) + time_clause
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=colnames)


def _fetch_transitions_postgres(config: SourceConfig) -> pd.DataFrame:
    from lakesource.postgres import series_db

    time_clause, params = _build_time_filters(config)
    sql = _transitions_sql(config.t) + time_clause
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=colnames)


def _fetch_lake_coordinates_postgres(config: SourceConfig) -> pd.DataFrame:
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(_lake_coords_sql(config.t))
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=colnames)


def _fetch_extremes_parquet(config: SourceConfig) -> pd.DataFrame:
    raise NotImplementedError("Parquet backend for extremes is not yet implemented")


def _fetch_transitions_parquet(config: SourceConfig) -> pd.DataFrame:
    raise NotImplementedError("Parquet backend for transitions is not yet implemented")


def _fetch_lake_coordinates_parquet(config: SourceConfig) -> pd.DataFrame:
    raise NotImplementedError("Parquet backend for lake coordinates is not yet implemented")


def fetch_extremes_with_coords(config: SourceConfig) -> pd.DataFrame:
    if config.backend == Backend.POSTGRES:
        return _fetch_extremes_postgres(config)
    return _fetch_extremes_parquet(config)


def fetch_transitions_with_coords(config: SourceConfig) -> pd.DataFrame:
    if config.backend == Backend.POSTGRES:
        return _fetch_transitions_postgres(config)
    return _fetch_transitions_parquet(config)


def fetch_lake_coordinates(config: SourceConfig) -> pd.DataFrame:
    if config.backend == Backend.POSTGRES:
        return _fetch_lake_coordinates_postgres(config)
    return _fetch_lake_coordinates_parquet(config)
