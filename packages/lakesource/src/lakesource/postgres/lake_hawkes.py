"""Database operations for Hawkes process tables."""

from __future__ import annotations

import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _ensure_hawkes_results_table_sql(tc: TableConfig) -> sql.Composed:
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
""").format(table=sql.Identifier(tc.series_table("hawkes_results")))


def _ensure_hawkes_lrt_table_sql(tc: TableConfig) -> sql.Composed:
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
""").format(table=sql.Identifier(tc.series_table("hawkes_lrt")))


def _ensure_hawkes_transition_monthly_table_sql(tc: TableConfig) -> sql.Composed:
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
""").format(table=sql.Identifier(tc.series_table("hawkes_transition_monthly")))


def _upsert_hawkes_results_sql(tc: TableConfig) -> sql.Composed:
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
        table=sql.Identifier(tc.series_table("hawkes_results")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "threshold_quantile")
        ),
    )


def _upsert_hawkes_lrt_sql(tc: TableConfig) -> sql.Composed:
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
        table=sql.Identifier(tc.series_table("hawkes_lrt")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "threshold_quantile", "test_name")
        ),
    )


def _upsert_hawkes_transition_monthly_sql(tc: TableConfig) -> sql.Composed:
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
        table=sql.Identifier(tc.series_table("hawkes_transition_monthly")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in ("hylak_id", "threshold_quantile", "year", "month", "direction")
        ),
    )


def ensure_hawkes_results_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create hawkes_results, hawkes_lrt, and monthly transition tables when missing."""
    with conn.cursor() as cur:
        cur.execute(_ensure_hawkes_results_table_sql(table_config))
        cur.execute(_ensure_hawkes_lrt_table_sql(table_config))
        cur.execute(_ensure_hawkes_transition_monthly_table_sql(table_config))
    conn.commit()
    log.debug(
        "Ensured hawkes_results, hawkes_lrt, and hawkes_transition_monthly tables exist"
    )


def upsert_hawkes_results(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update Hawkes fit result rows."""
    with conn.cursor() as cur:
        cur.executemany(_upsert_hawkes_results_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d hawkes_results row(s)", len(rows))


def upsert_hawkes_lrt(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update Hawkes LRT rows."""
    with conn.cursor() as cur:
        cur.executemany(_upsert_hawkes_lrt_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d hawkes_lrt row(s)", len(rows))


def upsert_hawkes_transition_monthly(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update Hawkes monthly transition-significance rows."""
    with conn.cursor() as cur:
        cur.executemany(_upsert_hawkes_transition_monthly_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d hawkes_transition_monthly row(s)", len(rows))
