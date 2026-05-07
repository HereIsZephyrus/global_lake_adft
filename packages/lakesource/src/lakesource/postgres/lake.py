"""Database operations for lake data.

This module provides backward-compatible imports. All functions have been migrated
to domain-specific modules:
- lakesource.postgres.lake_area
- lakesource.postgres.lake_eot
- lakesource.postgres.lake_quantile
- lakesource.postgres.lake_hawkes
- lakesource.postgres.lake_pwm
- lakesource.postgres.lake_entropy
- lakesource.postgres.lake_misc
"""

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


def __getattr__(name: str):
    if name in (
        "fetch_lake_area",
        "fetch_lake_area_chunk",
        "fetch_af_nearest_high_topo",
        "fetch_impact_pairs",
        "fetch_lake_area_by_ids",
    ):
        from lakesource.postgres import lake_area
        return getattr(lake_area, name)
    if name in (
        "fetch_eot_extremes_by_id",
        "ensure_eot_results_table",
        "upsert_eot_results",
        "upsert_eot_extremes",
        "upsert_eot_run_status",
    ):
        from lakesource.postgres import lake_eot
        return getattr(lake_eot, name)
    if name in (
        "ensure_quantile_tables",
        "upsert_quantile_labels",
        "upsert_quantile_extremes",
        "upsert_quantile_abrupt_transitions",
        "upsert_quantile_run_status",
        "count_quantile_status_in_range",
        "fetch_quantile_status_ids_in_range",
    ):
        from lakesource.postgres import lake_quantile
        return getattr(lake_quantile, name)
    if name in (
        "ensure_hawkes_results_table",
        "upsert_hawkes_results",
        "upsert_hawkes_lrt",
        "upsert_hawkes_transition_monthly",
    ):
        from lakesource.postgres import lake_hawkes
        return getattr(lake_hawkes, name)
    if name in (
        "ensure_pwm_extreme_tables",
        "upsert_pwm_extreme_thresholds",
        "upsert_pwm_extreme_run_status",
        "count_pwm_extreme_status_in_range",
        "fetch_pwm_extreme_status_ids_in_range",
    ):
        from lakesource.postgres import lake_pwm
        return getattr(lake_pwm, name)
    if name in (
        "ensure_entropy_table",
        "upsert_entropy",
        "ensure_area_entropy_cv_table",
        "upsert_area_entropy_cv",
    ):
        from lakesource.postgres import lake_entropy
        return getattr(lake_entropy, name)
    if name in (
        "fetch_frozen_year_months_by_ids",
        "fetch_frozen_year_months_chunk",
        "fetch_seasonal_amplitude_chunk",
        "fetch_linear_trend_by_ids",
        "fetch_anomaly_hylak_ids",
        "fetch_quality_done_hylak_ids_in_range",
        "fetch_max_lake_info_hylak_id",
        "count_source_hylak_ids_in_range",
        "fetch_source_hylak_ids_in_range",
        "ensure_comparison_tables",
        "upsert_comparison_run_status",
        "fetch_comparison_status_ids_in_range",
        "ensure_interpolation_detect_table",
        "upsert_interpolation_detect",
    ):
        from lakesource.postgres import lake_misc
        return getattr(lake_misc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
