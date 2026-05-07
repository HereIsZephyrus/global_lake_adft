"""Database operations for entropy and area_entropy_cv tables."""

from __future__ import annotations

import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _ensure_entropy_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id                  INTEGER PRIMARY KEY,
    ae_overall                DOUBLE PRECISION,
    sens_slope                DOUBLE PRECISION,
    change_per_decade_pct     DOUBLE PRECISION,
    mk_trend                  TEXT,
    mk_p                      DOUBLE PRECISION,
    mk_z                      DOUBLE PRECISION,
    mk_significant            BOOLEAN,
    mean_seasonal_amplitude   DOUBLE PRECISION,
    computed_at               TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("entropy")))


def _upsert_entropy_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, ae_overall,
    sens_slope, change_per_decade_pct,
    mk_trend, mk_p, mk_z, mk_significant,
    mean_seasonal_amplitude,
    computed_at
) VALUES (
    %(hylak_id)s, %(ae_overall)s,
    %(sens_slope)s, %(change_per_decade_pct)s,
    %(mk_trend)s, %(mk_p)s, %(mk_z)s, %(mk_significant)s,
    %(mean_seasonal_amplitude)s,
    now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    ae_overall                = EXCLUDED.ae_overall,
    sens_slope                = EXCLUDED.sens_slope,
    change_per_decade_pct     = EXCLUDED.change_per_decade_pct,
    mk_trend                  = EXCLUDED.mk_trend,
    mk_p                      = EXCLUDED.mk_p,
    mk_z                      = EXCLUDED.mk_z,
    mk_significant            = EXCLUDED.mk_significant,
    mean_seasonal_amplitude   = EXCLUDED.mean_seasonal_amplitude,
    computed_at               = now();
""").format(
        table=sql.Identifier(tc.series_table("entropy")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )


def _ensure_area_entropy_cv_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id       INTEGER PRIMARY KEY,
    n_obs          INTEGER,
    n_distinct     INTEGER,
    dominant_ratio DOUBLE PRECISION,
    cv             DOUBLE PRECISION,
    H              DOUBLE PRECISION,
    h_cv           DOUBLE PRECISION,
    n_frozen       INTEGER,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("area_entropy_cv")))


def _upsert_area_entropy_cv_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, n_obs, n_distinct, dominant_ratio, cv, H, h_cv, n_frozen, computed_at
) VALUES (
    %(hylak_id)s, %(n_obs)s, %(n_distinct)s, %(dominant_ratio)s, %(cv)s, %(H)s, %(h_cv)s, %(n_frozen)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    n_obs          = EXCLUDED.n_obs,
    n_distinct     = EXCLUDED.n_distinct,
    dominant_ratio = EXCLUDED.dominant_ratio,
    cv             = EXCLUDED.cv,
    H              = EXCLUDED.H,
    h_cv           = EXCLUDED.h_cv,
    n_frozen       = EXCLUDED.n_frozen,
    computed_at    = now();
""").format(
        table=sql.Identifier(tc.series_table("area_entropy_cv")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )


def ensure_entropy_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the entropy table in SERIES_DB if it does not already exist."""
    with conn.cursor() as cur:
        cur.execute(_ensure_entropy_table_sql(table_config))
    conn.commit()
    log.debug("Ensured entropy table exists")


def upsert_entropy(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update entropy summary rows."""
    with conn.cursor() as cur:
        cur.executemany(_upsert_entropy_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d entropy row(s)", len(rows))


def ensure_area_entropy_cv_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_area_entropy_cv_table_sql(table_config))
    conn.commit()
    log.debug("Ensured area_entropy_cv table exists")


def upsert_area_entropy_cv(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    if not rows:
        return
    table = table_config.series_table("area_entropy_cv")
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE TEMP TABLE _tmp_ecv ("
                "hylak_id INTEGER, n_obs INTEGER, n_distinct INTEGER, "
                "dominant_ratio DOUBLE PRECISION, cv DOUBLE PRECISION, "
                "H DOUBLE PRECISION, h_cv DOUBLE PRECISION, n_frozen INTEGER"
                ") ON COMMIT DROP"
            )
        )
        with cur.copy(
            "COPY _tmp_ecv (hylak_id, n_obs, n_distinct, dominant_ratio, cv, H, h_cv, n_frozen) "
            "FROM STDIN"
        ) as copy:
            for r in rows:
                copy.write_row([
                    r["hylak_id"], r["n_obs"], r["n_distinct"],
                    r["dominant_ratio"], r["cv"], r["H"], r["h_cv"], r["n_frozen"],
                ])
        cur.execute(
            sql.SQL(
                "INSERT INTO {table} (hylak_id, n_obs, n_distinct, dominant_ratio, cv, H, h_cv, "
                "n_frozen, computed_at) "
                "SELECT t.hylak_id, t.n_obs, t.n_distinct, t.dominant_ratio, t.cv, t.H, t.h_cv, "
                "t.n_frozen, now() "
                "FROM _tmp_ecv t "
                "ON CONFLICT (hylak_id) DO UPDATE SET "
                "n_obs = EXCLUDED.n_obs, "
                "n_distinct = EXCLUDED.n_distinct, "
                "dominant_ratio = EXCLUDED.dominant_ratio, "
                "cv = EXCLUDED.cv, "
                "H = EXCLUDED.H, "
                "h_cv = EXCLUDED.h_cv, "
                "n_frozen = EXCLUDED.n_frozen, "
                "computed_at = now()"
            ).format(table=sql.Identifier(table))
        )
    if commit:
        conn.commit()
    log.info("Upserted %d area_entropy_cv row(s)", len(rows))
