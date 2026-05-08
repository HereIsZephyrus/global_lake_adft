"""Lake geometry WKT reads."""



from __future__ import annotations



import os

import re

from typing import TYPE_CHECKING



if TYPE_CHECKING:

    import pandas as pd

    import psycopg



from lakesource.table_config import TableConfig



_default_table_config = TableConfig.default()

_SAFE_SQL_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")



def _validate_sql_identifier(name: str, label: str) -> str:
    if not _SAFE_SQL_IDENT.match(name):
        raise ValueError(f"{label} must match {_SAFE_SQL_IDENT.pattern}, got {name!r}")
    return name


def _lake_geometry_table_sql_ident(
    table_config: TableConfig = _default_table_config,
) -> "psycopg.sql.Composed":
    from psycopg import sql
    env_ref = (os.environ.get("LAKE_GEOMETRY_TABLE") or "").strip()
    ref = env_ref or table_config.atlas_table("lake_geometry")
    parts = [p.strip() for p in ref.split(".") if p.strip()]
    if not parts or len(parts) > 2:
        raise ValueError(
            f"LAKE_GEOMETRY_TABLE must be 'table' or 'schema.table', got {ref!r}"
        )
    if len(parts) == 1:
        return sql.Identifier(_validate_sql_identifier(parts[0], "table name"))
    return sql.Identifier(
        _validate_sql_identifier(parts[0], "schema name"),
        _validate_sql_identifier(parts[1], "table name"),
    )


def fetch_lake_geometry_wkt_by_ids(
    conn: "psycopg.Connection",
    hylak_ids: list[int],
    *,
    id_column: str | None = None,
    geom_column: str | None = None,
    simplify_tolerance_meters: float | None = None,
    table_config: TableConfig = _default_table_config,
) -> "pd.DataFrame":
    import pandas as pd
    from psycopg import sql
    if not hylak_ids:
        return pd.DataFrame(columns=["hylak_id", "wkt"])
    id_col = (
        id_column
        or os.environ.get("LAKE_GEOMETRY_ID_COLUMN")
        or table_config.lake_geometry_id_column
    )
    geom_col = (
        geom_column
        or os.environ.get("LAKE_GEOMETRY_GEOM_COLUMN")
        or table_config.lake_geometry_geom_column
    )
    _validate_sql_identifier(id_col, "id_column")
    _validate_sql_identifier(geom_col, "geom_column")
    tol = simplify_tolerance_meters
    if tol is None:
        env_raw = (os.environ.get("LAKE_GEOMETRY_SIMPLIFY_METERS") or "").strip()
        if env_raw:
            try:
                tol = float(env_raw)
            except ValueError:
                raise ValueError(
                    f"LAKE_GEOMETRY_SIMPLIFY_METERS must be a float, got {env_raw!r}"
                ) from None
        elif table_config.lake_geometry_simplify_meters > 0:
            tol = table_config.lake_geometry_simplify_meters
    if tol is not None and tol <= 0:
        tol = None
    table_ident = _lake_geometry_table_sql_ident(table_config)
    if tol is not None:
        query = sql.SQL(
            "SELECT {id_c} AS hylak_id, "
            "ST_AsText(ST_Transform("
            "ST_SimplifyPreserveTopology(ST_Transform({g_c}, 3857), %(simplify_m)s), "
            "4326)) AS wkt "
            "FROM {tbl} WHERE {id_c} = ANY(%(ids)s)"
        ).format(
            id_c=sql.Identifier(id_col),
            g_c=sql.Identifier(geom_col),
            tbl=table_ident,
        )
        params: dict = {"ids": list(hylak_ids), "simplify_m": float(tol)}
    else:
        query = sql.SQL(
            "SELECT {id_c} AS hylak_id, ST_AsText({g_c}) AS wkt "
            "FROM {tbl} WHERE {id_c} = ANY(%(ids)s)"
        ).format(
            id_c=sql.Identifier(id_col),
            g_c=sql.Identifier(geom_col),
            tbl=table_ident,
        )
        params = {"ids": list(hylak_ids)}
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["hylak_id", "wkt"])
    return pd.DataFrame(rows, columns=["hylak_id", "wkt"])
