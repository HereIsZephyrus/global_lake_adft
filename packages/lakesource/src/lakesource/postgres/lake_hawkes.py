"""Database operations for Hawkes process tables.

Split into PWM-Hawkes and EOT-Hawkes table sets to avoid cross-pipeline
contamination.  The DDL/DML is identical for each pair; only the logical
table names differ.
"""

from __future__ import annotations

import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


# ------------------------------------------------------------------
# SQL template builders — parametrised by logical table name
# ------------------------------------------------------------------

def _ensure_hawkes_results_table_sql(
    tc: TableConfig, logical_name: str
) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id              INTEGER      NOT NULL,
    threshold_quantile    NUMERIC(5,4) NOT NULL,
    converged             BOOLEAN,
    log_likelihood        DOUBLE PRECISION,
    objective_value       DOUBLE PRECISION,
    n_events              INTEGER,
    n_dry_events          INTEGER,
    n_wet_events          INTEGER,
    mu_d                  DOUBLE PRECISION,
    mu_w                  DOUBLE PRECISION,
    alpha_dd              DOUBLE PRECISION,
    alpha_dw              DOUBLE PRECISION,
    alpha_wd              DOUBLE PRECISION,
    alpha_ww              DOUBLE PRECISION,
    beta_dd               DOUBLE PRECISION,
    beta_dw               DOUBLE PRECISION,
    beta_wd               DOUBLE PRECISION,
    beta_ww               DOUBLE PRECISION,
    spectral_radius       DOUBLE PRECISION,
    lrt_p_d_to_w          DOUBLE PRECISION,
    lrt_p_w_to_d          DOUBLE PRECISION,
    qc_pass               BOOLEAN,
    qc_exceedance_rate    DOUBLE PRECISION,
    qc_relative_amplitude DOUBLE PRECISION,
    qc_median_excess      DOUBLE PRECISION,
    error_message         TEXT,
    computed_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, threshold_quantile)
);
""").format(table=sql.Identifier(tc.series_table(logical_name)))


def _ensure_hawkes_lrt_table_sql(
    tc: TableConfig, logical_name: str
) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id                   INTEGER      NOT NULL,
    threshold_quantile         NUMERIC(5,4) NOT NULL,
    test_name                  TEXT         NOT NULL,
    lr_statistic               DOUBLE PRECISION,
    df                         INTEGER,
    p_value                    DOUBLE PRECISION,
    significance_level         DOUBLE PRECISION,
    reject_null                BOOLEAN,
    restricted_log_likelihood  DOUBLE PRECISION,
    full_log_likelihood        DOUBLE PRECISION,
    computed_at                TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, threshold_quantile, test_name)
);
""").format(table=sql.Identifier(tc.series_table(logical_name)))


def _ensure_hawkes_transition_monthly_table_sql(
    tc: TableConfig, logical_name: str
) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id              INTEGER      NOT NULL,
    threshold_quantile    NUMERIC(5,4) NOT NULL,
    year                  INTEGER      NOT NULL,
    month                 INTEGER      NOT NULL,
    direction             TEXT         NOT NULL,
    score_raw             DOUBLE PRECISION,
    score_norm            DOUBLE PRECISION,
    significance_quantile DOUBLE PRECISION,
    significance_threshold DOUBLE PRECISION,
    significant           BOOLEAN,
    computed_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, threshold_quantile, year, month, direction)
);
""").format(table=sql.Identifier(tc.series_table(logical_name)))


def _ensure_eot_hawkes_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id              INTEGER      NOT NULL,
    chunk_start           INTEGER      NOT NULL,
    chunk_end             INTEGER      NOT NULL,
    status                TEXT         NOT NULL,
    error_message         TEXT,
    computed_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("eot_hawkes_run_status")))


# ------------------------------------------------------------------
# Upsert SQL templates
# ------------------------------------------------------------------

def _upsert_hawkes_results_sql(
    tc: TableConfig, logical_name: str
) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, threshold_quantile,
    converged, log_likelihood, objective_value,
    n_events, n_dry_events, n_wet_events,
    mu_d, mu_w,
    alpha_dd, alpha_dw, alpha_wd, alpha_ww,
    beta_dd, beta_dw, beta_wd, beta_ww,
    spectral_radius,
    lrt_p_d_to_w, lrt_p_w_to_d,
    qc_pass, qc_exceedance_rate, qc_relative_amplitude, qc_median_excess,
    error_message, computed_at
) VALUES (
    %(hylak_id)s, %(threshold_quantile)s,
    %(converged)s, %(log_likelihood)s, %(objective_value)s,
    %(n_events)s, %(n_dry_events)s, %(n_wet_events)s,
    %(mu_d)s, %(mu_w)s,
    %(alpha_dd)s, %(alpha_dw)s, %(alpha_wd)s, %(alpha_ww)s,
    %(beta_dd)s, %(beta_dw)s, %(beta_wd)s, %(beta_ww)s,
    %(spectral_radius)s,
    %(lrt_p_d_to_w)s, %(lrt_p_w_to_d)s,
    %(qc_pass)s, %(qc_exceedance_rate)s, %(qc_relative_amplitude)s, %(qc_median_excess)s,
    %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    converged             = EXCLUDED.converged,
    log_likelihood        = EXCLUDED.log_likelihood,
    objective_value       = EXCLUDED.objective_value,
    n_events              = EXCLUDED.n_events,
    n_dry_events          = EXCLUDED.n_dry_events,
    n_wet_events          = EXCLUDED.n_wet_events,
    mu_d                  = EXCLUDED.mu_d,
    mu_w                  = EXCLUDED.mu_w,
    alpha_dd              = EXCLUDED.alpha_dd,
    alpha_dw              = EXCLUDED.alpha_dw,
    alpha_wd              = EXCLUDED.alpha_wd,
    alpha_ww              = EXCLUDED.alpha_ww,
    beta_dd               = EXCLUDED.beta_dd,
    beta_dw               = EXCLUDED.beta_dw,
    beta_wd               = EXCLUDED.beta_wd,
    beta_ww               = EXCLUDED.beta_ww,
    spectral_radius       = EXCLUDED.spectral_radius,
    lrt_p_d_to_w          = EXCLUDED.lrt_p_d_to_w,
    lrt_p_w_to_d          = EXCLUDED.lrt_p_w_to_d,
    qc_pass               = EXCLUDED.qc_pass,
    qc_exceedance_rate    = EXCLUDED.qc_exceedance_rate,
    qc_relative_amplitude = EXCLUDED.qc_relative_amplitude,
    qc_median_excess      = EXCLUDED.qc_median_excess,
    error_message         = EXCLUDED.error_message,
    computed_at           = now();
""").format(
        table=sql.Identifier(tc.series_table(logical_name)),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "threshold_quantile")
        ),
    )


def _upsert_hawkes_lrt_sql(
    tc: TableConfig, logical_name: str
) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, threshold_quantile, test_name,
    lr_statistic, df, p_value, significance_level, reject_null,
    restricted_log_likelihood, full_log_likelihood, computed_at
) VALUES (
    %(hylak_id)s, %(threshold_quantile)s, %(test_name)s,
    %(lr_statistic)s, %(df)s, %(p_value)s, %(significance_level)s, %(reject_null)s,
    %(restricted_log_likelihood)s, %(full_log_likelihood)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    lr_statistic              = EXCLUDED.lr_statistic,
    df                        = EXCLUDED.df,
    p_value                   = EXCLUDED.p_value,
    significance_level        = EXCLUDED.significance_level,
    reject_null               = EXCLUDED.reject_null,
    restricted_log_likelihood = EXCLUDED.restricted_log_likelihood,
    full_log_likelihood       = EXCLUDED.full_log_likelihood,
    computed_at               = now();
""").format(
        table=sql.Identifier(tc.series_table(logical_name)),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "threshold_quantile", "test_name")
        ),
    )


def _upsert_hawkes_transition_monthly_sql(
    tc: TableConfig, logical_name: str
) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, threshold_quantile, year, month, direction,
    score_raw, score_norm, significance_quantile, significance_threshold,
    significant, computed_at
) VALUES (
    %(hylak_id)s, %(threshold_quantile)s, %(year)s, %(month)s, %(direction)s,
    %(score_raw)s, %(score_norm)s, %(significance_quantile)s, %(significance_threshold)s,
    %(significant)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    score_raw               = EXCLUDED.score_raw,
    score_norm              = EXCLUDED.score_norm,
    significance_quantile   = EXCLUDED.significance_quantile,
    significance_threshold  = EXCLUDED.significance_threshold,
    significant             = EXCLUDED.significant,
    computed_at             = now();
""").format(
        table=sql.Identifier(tc.series_table(logical_name)),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in ("hylak_id", "threshold_quantile", "year", "month", "direction")
        ),
    )


def _upsert_eot_hawkes_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, chunk_start, chunk_end, status, error_message, computed_at
) VALUES (
    %(hylak_id)s, %(chunk_start)s, %(chunk_end)s, %(status)s,
    %(error_message)s, now()
)
ON CONFLICT (hylak_id) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(table=sql.Identifier(tc.series_table("eot_hawkes_run_status")))


# ------------------------------------------------------------------
# PWM-Hawkes table ensure
# ------------------------------------------------------------------

def ensure_pwm_hawkes_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            _ensure_hawkes_results_table_sql(table_config, "pwm_hawkes_results")
        )
        cur.execute(
            _ensure_hawkes_lrt_table_sql(table_config, "pwm_hawkes_lrt")
        )
        cur.execute(
            _ensure_hawkes_transition_monthly_table_sql(
                table_config, "pwm_hawkes_transition_monthly"
            )
        )
    conn.commit()
    log.debug("Ensured PWM-Hawkes tables exist")


def ensure_eot_hawkes_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            _ensure_hawkes_results_table_sql(table_config, "eot_hawkes_results")
        )
        cur.execute(
            _ensure_hawkes_lrt_table_sql(table_config, "eot_hawkes_lrt")
        )
        cur.execute(
            _ensure_hawkes_transition_monthly_table_sql(
                table_config, "eot_hawkes_transition_monthly"
            )
        )
        cur.execute(
            _ensure_eot_hawkes_run_status_table_sql(table_config)
        )
    conn.commit()
    log.debug("Ensured EOT-Hawkes tables exist")


# ------------------------------------------------------------------
# Deprecated: legacy ensure_hawkes_results_table
# ------------------------------------------------------------------

def ensure_hawkes_results_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Legacy ensure — delegates to PWM + EOT table creation."""
    ensure_pwm_hawkes_tables(conn, table_config=table_config)
    ensure_eot_hawkes_tables(conn, table_config=table_config)


# ------------------------------------------------------------------
# PWM-Hawkes upsert
# ------------------------------------------------------------------

def upsert_pwm_hawkes_results(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_hawkes_results_sql(table_config, "pwm_hawkes_results"), rows
        )
    conn.commit()
    log.info("Upserted %d pwm_hawkes_results row(s)", len(rows))


def upsert_pwm_hawkes_lrt(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_hawkes_lrt_sql(table_config, "pwm_hawkes_lrt"), rows
        )
    conn.commit()
    log.info("Upserted %d pwm_hawkes_lrt row(s)", len(rows))


def upsert_pwm_hawkes_transition_monthly(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_hawkes_transition_monthly_sql(
                table_config, "pwm_hawkes_transition_monthly"
            ),
            rows,
        )
    conn.commit()
    log.info("Upserted %d pwm_hawkes_transition_monthly row(s)", len(rows))


# ------------------------------------------------------------------
# EOT-Hawkes upsert
# ------------------------------------------------------------------

def upsert_eot_hawkes_results(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_hawkes_results_sql(table_config, "eot_hawkes_results"), rows
        )
    conn.commit()
    log.info("Upserted %d eot_hawkes_results row(s)", len(rows))


def upsert_eot_hawkes_lrt(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_hawkes_lrt_sql(table_config, "eot_hawkes_lrt"), rows
        )
    conn.commit()
    log.info("Upserted %d eot_hawkes_lrt row(s)", len(rows))


def upsert_eot_hawkes_transition_monthly(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_hawkes_transition_monthly_sql(
                table_config, "eot_hawkes_transition_monthly"
            ),
            rows,
        )
    conn.commit()
    log.info("Upserted %d eot_hawkes_transition_monthly row(s)", len(rows))


def upsert_eot_hawkes_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _upsert_eot_hawkes_run_status_sql(table_config), rows
        )
    conn.commit()
    log.info("Upserted %d eot_hawkes_run_status row(s)", len(rows))


# ------------------------------------------------------------------
# Deprecated: legacy upsert functions (delegate to PWM variants)
# ------------------------------------------------------------------

def upsert_hawkes_results(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Legacy upsert — delegates to PWM-Hawkes tables."""
    upsert_pwm_hawkes_results(conn, rows, table_config=table_config)


def upsert_hawkes_lrt(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Legacy upsert — delegates to PWM-Hawkes tables."""
    upsert_pwm_hawkes_lrt(conn, rows, table_config=table_config)


def upsert_hawkes_transition_monthly(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Legacy upsert — delegates to PWM-Hawkes tables."""
    upsert_pwm_hawkes_transition_monthly(conn, rows, table_config=table_config)
