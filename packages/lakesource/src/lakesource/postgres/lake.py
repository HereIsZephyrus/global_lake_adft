"""Database operations for lake_area reads and entropy table writes."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import logging
import os
import re

import pandas as pd
import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_DEFAULT_LAKE_GEOMETRY_TABLE = "LakeATLAS_v10_pol"
_LEGACY_WORKFLOW_VERSION = "legacy"

_default_table_config = TableConfig.default()


def _fetch_lake_area_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       EXTRACT(YEAR  FROM year_month)::int AS year,
       EXTRACT(MONTH FROM year_month)::int AS month,
       water_area
FROM {table}
ORDER BY hylak_id, year_month
""").format(table=sql.Identifier(tc.series_table("lake_area")))


def _fetch_lake_area_limited_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       EXTRACT(YEAR  FROM year_month)::int AS year,
       EXTRACT(MONTH FROM year_month)::int AS month,
       water_area
FROM {table}
WHERE hylak_id < %(limit_id)s
ORDER BY hylak_id, year_month
""").format(table=sql.Identifier(tc.series_table("lake_area")))


def _fetch_lake_area_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT la.hylak_id,
       EXTRACT(YEAR  FROM la.year_month)::int AS year,
       EXTRACT(MONTH FROM la.year_month)::int AS month,
       la.water_area
FROM {lake_area} la
JOIN {area_quality} aq ON aq.hylak_id = la.hylak_id
WHERE la.hylak_id >= %(chunk_start)s AND la.hylak_id < %(chunk_end)s
ORDER BY la.hylak_id, la.year_month
""").format(
    lake_area=sql.Identifier(tc.series_table("lake_area")),
    area_quality=sql.Identifier(tc.series_table("area_quality")),
)


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


def _fetch_seasonal_amplitude_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, annual_means_std, mean_area
FROM {table}
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _fetch_af_nearest_high_topo_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, nearest_id, topo_level
FROM {table}
WHERE topo_level > 8 AND nearest_id IS NOT NULL
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("af_nearest")))


def _fetch_lake_area_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       EXTRACT(YEAR  FROM year_month)::int AS year,
       EXTRACT(MONTH FROM year_month)::int AS month,
       water_area
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
ORDER BY hylak_id, year_month
""").format(table=sql.Identifier(tc.series_table("lake_area")))


def _fetch_eot_extremes_by_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       tail,
       threshold_quantile,
       cluster_id,
       cluster_size,
       year,
       month,
       water_area,
       threshold_at_event
FROM {table}
WHERE hylak_id = %(hylak_id)s
ORDER BY year, month, tail, cluster_id
""").format(table=sql.Identifier(tc.series_table("eot_extremes")))


def _fetch_eot_extremes_by_id_and_q_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       tail,
       threshold_quantile,
       cluster_id,
       cluster_size,
       year,
       month,
       water_area,
       threshold_at_event
FROM {table}
WHERE hylak_id = %(hylak_id)s
  AND threshold_quantile = %(threshold_quantile)s
ORDER BY year, month, tail, cluster_id
""").format(table=sql.Identifier(tc.series_table("eot_extremes")))


def _fetch_linear_trend_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, linear_trend_of_stl_trend_per_period
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _fetch_frozen_year_months_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       (EXTRACT(YEAR FROM year_month)::int * 100
        + EXTRACT(MONTH FROM year_month)::int) AS year_month_key
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
  AND anomaly_type = 'frozen'
ORDER BY hylak_id, year_month
""").format(table=sql.Identifier(tc.series_table("anomaly")))


def _fetch_frozen_year_months_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       (EXTRACT(YEAR FROM year_month)::int * 100
        + EXTRACT(MONTH FROM year_month)::int) AS year_month_key
FROM {table}
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
  AND anomaly_type = 'frozen'
ORDER BY hylak_id, year_month
""").format(table=sql.Identifier(tc.series_table("anomaly")))


def _ensure_eot_results_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id           INTEGER      NOT NULL,
    tail               TEXT         NOT NULL,
    threshold_quantile NUMERIC(5,4) NOT NULL,
    converged          BOOLEAN,
    log_likelihood     DOUBLE PRECISION,
    threshold          DOUBLE PRECISION,
    n_extremes         INTEGER,
    n_observations     INTEGER,
    n_frozen_months    INTEGER,
    beta0              DOUBLE PRECISION,
    beta1              DOUBLE PRECISION,
    sin_1              DOUBLE PRECISION,
    cos_1              DOUBLE PRECISION,
    sigma              DOUBLE PRECISION,
    xi                 DOUBLE PRECISION,
    error_message      TEXT,
    computed_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, tail, threshold_quantile)
);
""").format(table=sql.Identifier(tc.series_table("eot_results")))


def _ensure_eot_extremes_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id           INTEGER      NOT NULL,
    tail               TEXT         NOT NULL,
    threshold_quantile NUMERIC(5,4) NOT NULL,
    cluster_id         INTEGER      NOT NULL,
    cluster_size       INTEGER,
    year               INTEGER,
    month              INTEGER,
    water_area         DOUBLE PRECISION,
    threshold_at_event DOUBLE PRECISION,
    computed_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, tail, threshold_quantile, cluster_id)
);
""").format(table=sql.Identifier(tc.series_table("eot_extremes")))


def _upsert_eot_results_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, tail, threshold_quantile,
    converged, log_likelihood, threshold,
    n_extremes, n_observations, n_frozen_months,
    beta0, beta1, sin_1, cos_1, sigma, xi,
    error_message, computed_at
) VALUES (
    %(hylak_id)s, %(tail)s, %(threshold_quantile)s,
    %(converged)s, %(log_likelihood)s, %(threshold)s,
    %(n_extremes)s, %(n_observations)s, %(n_frozen_months)s,
    %(beta0)s, %(beta1)s, %(sin_1)s, %(cos_1)s, %(sigma)s, %(xi)s,
    %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    converged          = EXCLUDED.converged,
    log_likelihood     = EXCLUDED.log_likelihood,
    threshold          = EXCLUDED.threshold,
    n_extremes         = EXCLUDED.n_extremes,
    n_observations     = EXCLUDED.n_observations,
    n_frozen_months    = EXCLUDED.n_frozen_months,
    beta0              = EXCLUDED.beta0,
    beta1              = EXCLUDED.beta1,
    sin_1              = EXCLUDED.sin_1,
    cos_1              = EXCLUDED.cos_1,
    sigma              = EXCLUDED.sigma,
    xi                 = EXCLUDED.xi,
    error_message      = EXCLUDED.error_message,
    computed_at        = now();
""").format(
    table=sql.Identifier(tc.series_table("eot_results")),
    conflict_cols=sql.SQL(", ").join(
        sql.Identifier(c) for c in ("hylak_id", "tail", "threshold_quantile")
    ),
)


def _upsert_eot_extremes_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, tail, threshold_quantile, cluster_id,
    cluster_size, year, month, water_area, threshold_at_event, computed_at
) VALUES (
    %(hylak_id)s, %(tail)s, %(threshold_quantile)s, %(cluster_id)s,
    %(cluster_size)s, %(year)s, %(month)s, %(water_area)s, %(threshold_at_event)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    cluster_size       = EXCLUDED.cluster_size,
    year               = EXCLUDED.year,
    month              = EXCLUDED.month,
    water_area         = EXCLUDED.water_area,
    threshold_at_event = EXCLUDED.threshold_at_event,
    computed_at        = now();
""").format(
    table=sql.Identifier(tc.series_table("eot_extremes")),
    conflict_cols=sql.SQL(", ").join(
        sql.Identifier(c)
        for c in ("hylak_id", "tail", "threshold_quantile", "cluster_id")
    ),
)


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


def _ensure_monthly_transition_labels_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    workflow_version    TEXT         NOT NULL,
    year                INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    water_area          DOUBLE PRECISION,
    monthly_climatology DOUBLE PRECISION,
    anomaly             DOUBLE PRECISION,
    q_low               DOUBLE PRECISION,
    q_high              DOUBLE PRECISION,
    extreme_label       TEXT,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version, year, month)
);
""").format(table=sql.Identifier(tc.series_table("monthly_transition_labels")))


def _ensure_monthly_transition_extremes_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    workflow_version    TEXT         NOT NULL,
    year                INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    event_type          TEXT         NOT NULL,
    water_area          DOUBLE PRECISION,
    monthly_climatology DOUBLE PRECISION,
    anomaly             DOUBLE PRECISION,
    threshold           DOUBLE PRECISION,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version, year, month, event_type)
);
""").format(table=sql.Identifier(tc.series_table("monthly_transition_extremes")))


def _ensure_monthly_transition_abrupt_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    workflow_version  TEXT         NOT NULL,
    from_year         INTEGER      NOT NULL,
    from_month        INTEGER      NOT NULL,
    to_year           INTEGER      NOT NULL,
    to_month          INTEGER      NOT NULL,
    transition_type   TEXT         NOT NULL,
    from_anomaly      DOUBLE PRECISION,
    to_anomaly        DOUBLE PRECISION,
    from_label        TEXT,
    to_label          TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (
        hylak_id,
        workflow_version,
        from_year,
        from_month,
        to_year,
        to_month,
        transition_type
    )
);
""").format(table=sql.Identifier(tc.series_table("monthly_transition_abrupt_transitions")))


def _ensure_monthly_transition_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    workflow_version  TEXT         NOT NULL,
    chunk_start       INTEGER,
    chunk_end         INTEGER,
    status            TEXT         NOT NULL,
    error_message     TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version)
);
""").format(table=sql.Identifier(tc.series_table("monthly_transition_run_status")))


def _ensure_monthly_transition_status_workflow_column_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(
        "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS workflow_version TEXT"
    ).format(table=sql.Identifier(tc.series_table("monthly_transition_run_status")))


def _drop_monthly_transition_status_pk_sql(tc: TableConfig) -> sql.Composed:
    table_name = tc.series_table("monthly_transition_run_status")
    return sql.SQL(
        "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"
    ).format(
        table=sql.Identifier(table_name),
        constraint=sql.Identifier(f"{table_name}_pkey"),
    )


def _add_monthly_transition_status_pk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(
        "ALTER TABLE {table} ADD PRIMARY KEY ({columns})"
    ).format(
        table=sql.Identifier(tc.series_table("monthly_transition_run_status")),
        columns=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "workflow_version")
        ),
    )


def _create_monthly_transition_status_version_index_sql(tc: TableConfig) -> sql.Composed:
    table_name = tc.series_table("monthly_transition_run_status")
    return sql.SQL(
        "CREATE INDEX IF NOT EXISTS {index} ON {table} (workflow_version, hylak_id)"
    ).format(
        index=sql.Identifier(f"{table_name}_version_hylak_idx"),
        table=sql.Identifier(table_name),
    )


def _monthly_transition_versioned_tables(
    tc: TableConfig,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    return (
        (tc.series_table("monthly_transition_labels"), ("hylak_id", "workflow_version", "year", "month")),
        (tc.series_table("monthly_transition_extremes"), ("hylak_id", "workflow_version", "year", "month", "event_type")),
        (tc.series_table("monthly_transition_abrupt_transitions"), ("hylak_id", "workflow_version", "from_year", "from_month", "to_year", "to_month", "transition_type")),
        (tc.series_table("monthly_transition_run_status"), ("hylak_id", "workflow_version")),
    )


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


def _upsert_monthly_transition_labels_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, year, month,
    water_area, monthly_climatology, anomaly,
    q_low, q_high, extreme_label, computed_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(year)s, %(month)s,
    %(water_area)s, %(monthly_climatology)s, %(anomaly)s,
    %(q_low)s, %(q_high)s, %(extreme_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    water_area          = EXCLUDED.water_area,
    monthly_climatology = EXCLUDED.monthly_climatology,
    anomaly             = EXCLUDED.anomaly,
    q_low               = EXCLUDED.q_low,
    q_high              = EXCLUDED.q_high,
    extreme_label       = EXCLUDED.extreme_label,
    computed_at         = now();
""").format(
    table=sql.Identifier(tc.series_table("monthly_transition_labels")),
    conflict_cols=sql.SQL(", ").join(
        sql.Identifier(c)
        for c in ("hylak_id", "workflow_version", "year", "month")
    ),
)


def _upsert_monthly_transition_extremes_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, year, month, event_type,
    water_area, monthly_climatology, anomaly, threshold, computed_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(year)s, %(month)s, %(event_type)s,
    %(water_area)s, %(monthly_climatology)s, %(anomaly)s, %(threshold)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    water_area          = EXCLUDED.water_area,
    monthly_climatology = EXCLUDED.monthly_climatology,
    anomaly             = EXCLUDED.anomaly,
    threshold           = EXCLUDED.threshold,
    computed_at         = now();
""").format(
    table=sql.Identifier(tc.series_table("monthly_transition_extremes")),
    conflict_cols=sql.SQL(", ").join(
        sql.Identifier(c)
        for c in ("hylak_id", "workflow_version", "year", "month", "event_type")
    ),
)


def _upsert_monthly_transition_abrupt_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, from_year, from_month, to_year, to_month, transition_type,
    from_anomaly, to_anomaly, from_label, to_label, computed_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(from_year)s, %(from_month)s,
    %(to_year)s, %(to_month)s, %(transition_type)s, %(from_anomaly)s, %(to_anomaly)s,
    %(from_label)s, %(to_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    from_anomaly = EXCLUDED.from_anomaly,
    to_anomaly   = EXCLUDED.to_anomaly,
    from_label   = EXCLUDED.from_label,
    to_label     = EXCLUDED.to_label,
    computed_at  = now();
""").format(
    table=sql.Identifier(tc.series_table("monthly_transition_abrupt_transitions")),
    conflict_cols=sql.SQL(", ").join(
        sql.Identifier(c)
        for c in (
            "hylak_id",
            "workflow_version",
            "from_year",
            "from_month",
            "to_year",
            "to_month",
            "transition_type",
        )
    ),
)


def _upsert_monthly_transition_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, chunk_start, chunk_end, status, error_message, computed_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(
    table=sql.Identifier(tc.series_table("monthly_transition_run_status")),
    conflict_cols=sql.SQL(", ").join(
        sql.Identifier(c) for c in ("hylak_id", "workflow_version")
    ),
)


def _count_area_quality_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT COUNT(DISTINCT la.hylak_id)
FROM {lake_area} la
JOIN {area_quality} aq ON aq.hylak_id = la.hylak_id
WHERE la.hylak_id >= %(chunk_start)s AND la.hylak_id < %(chunk_end)s
""").format(
    lake_area=sql.Identifier(tc.series_table("lake_area")),
    area_quality=sql.Identifier(tc.series_table("area_quality")),
)


def _count_monthly_transition_status_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT COUNT(*)
FROM {table}
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
  AND status = 'done'
  AND workflow_version = %(workflow_version)s
""").format(table=sql.Identifier(tc.series_table("monthly_transition_run_status")))


def _fetch_monthly_transition_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
  AND status = 'done'
  AND workflow_version = %(workflow_version)s
""").format(table=sql.Identifier(tc.series_table("monthly_transition_run_status")))


def _fetch_area_quality_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT DISTINCT la.hylak_id
FROM {lake_area} la
JOIN {area_quality} aq ON aq.hylak_id = la.hylak_id
WHERE la.hylak_id >= %(chunk_start)s AND la.hylak_id < %(chunk_end)s
ORDER BY la.hylak_id
""").format(
    lake_area=sql.Identifier(tc.series_table("lake_area")),
    area_quality=sql.Identifier(tc.series_table("area_quality")),
)


def _fetch_max_area_quality_hylak_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT MAX(hylak_id)
FROM {table}
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def fetch_lake_area(
    conn: psycopg.Connection,
    limit_id: int | None = None,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, pd.DataFrame]:
    """Fetch all lake_area rows and split by hylak_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        limit_id: If given, only rows with id < limit_id are returned (for testing).
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].
    """
    if limit_id is None:
        query = _fetch_lake_area_sql(table_config)
        params = None
    else:
        query = _fetch_lake_area_limited_sql(table_config)
        params = {"limit_id": limit_id}

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    result = {
        int(hylak_id): group.drop(columns="hylak_id").reset_index(drop=True)
        for hylak_id, group in df.groupby("hylak_id")
    }
    log.info("Fetched lake_area: %d rows, %d lakes", len(df), len(result))
    return result


def fetch_lake_area_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, pd.DataFrame]:
    """Fetch lake_area rows for a hylak_id range [chunk_start, chunk_end) and split by hylak_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].
    """
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_lake_area_chunk_sql(table_config), params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return {}
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    result = {
        int(hylak_id): group.drop(columns="hylak_id").reset_index(drop=True)
        for hylak_id, group in df.groupby("hylak_id")
    }
    log.debug(
        "Fetched lake_area chunk [%d, %d): %d rows, %d lakes",
        chunk_start,
        chunk_end,
        len(df),
        len(result),
    )
    return result


def fetch_af_nearest_high_topo(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> list[dict]:
    """Fetch af_nearest rows with topo_level > 8 and non-null nearest_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        table_config: Table name configuration.

    Returns:
        List of dicts with keys hylak_id, nearest_id, topo_level.
    """
    with conn.cursor() as cur:
        cur.execute(_fetch_af_nearest_high_topo_sql(table_config))
        rows = cur.fetchall()
    result = [
        {"hylak_id": int(r[0]), "nearest_id": int(r[1]), "topo_level": int(r[2])}
        for r in rows
    ]
    log.info("Fetched af_nearest (topo_level>8): %d pairs", len(result))
    return result


def fetch_lake_area_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, pd.DataFrame]:
    """Fetch lake_area rows for the given hylak_id set and split by hylak_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        id_list: List of hylak_id values to fetch.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].
    """
    if not id_list:
        return {}
    params = {"id_list": id_list}
    with conn.cursor() as cur:
        cur.execute(_fetch_lake_area_by_ids_sql(table_config), params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return {}
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    result = {
        int(hylak_id): group.drop(columns="hylak_id").reset_index(drop=True)
        for hylak_id, group in df.groupby("hylak_id")
    }
    log.debug(
        "Fetched lake_area by ids: %d rows, %d lakes",
        len(df),
        len(result),
    )
    return result


def fetch_eot_extremes_by_id(
    conn: psycopg.Connection,
    hylak_id: int,
    threshold_quantile: float | None = None,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Fetch EOT extreme rows for one lake, optionally filtered by threshold quantile.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        hylak_id: Target lake id.
        threshold_quantile: Optional quantile filter (e.g., 0.95). If None, returns
            all quantiles available for the lake.
        table_config: Table name configuration.

    Returns:
        DataFrame with columns:
            [hylak_id, tail, threshold_quantile, cluster_id, cluster_size,
             year, month, water_area, threshold_at_event].
        Returns an empty DataFrame when no rows are found.
    """
    params: dict[str, int | Decimal] = {"hylak_id": int(hylak_id)}
    query = _fetch_eot_extremes_by_id_sql(table_config)
    if threshold_quantile is not None:
        try:
            params["threshold_quantile"] = Decimal(str(threshold_quantile))
        except (InvalidOperation, ValueError) as err:
            raise ValueError(
                f"threshold_quantile must be numeric, got: {threshold_quantile!r}"
            ) from err
        query = _fetch_eot_extremes_by_id_and_q_sql(table_config)
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return df

    for col in ("hylak_id", "cluster_id", "cluster_size", "year", "month"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("threshold_quantile", "water_area", "threshold_at_event"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "tail" in df.columns:
        df["tail"] = df["tail"].astype(str)

    return df


def fetch_linear_trend_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float | None]:
    """Fetch linear trend (km²/year) from lake_info for the given hylak_ids.

    The column linear_trend_of_stl_trend_per_period stores the STL-based linear
    trend in km²/year. Returns None for a lake if the value is missing or NULL.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        id_list: List of hylak_id values to fetch.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to trend in km²/year (float) or None.
    """
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


def ensure_entropy_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the entropy table in SERIES_DB if it does not already exist.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.execute(_ensure_entropy_table_sql(table_config))
    conn.commit()
    log.debug("Ensured entropy table exists")


def fetch_seasonal_amplitude_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float | None]:
    """Fetch CV (coefficient of variation) from lake_info: annual_means_std / mean_area.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to CV (float or None). None if mean_area missing or <= 0.
    """
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


def upsert_entropy(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update entropy summary rows.

    Each dict in rows must contain the keys matching the entropy table columns
    (hylak_id, ae_overall, sens_slope, change_per_decade_pct,
    mk_trend, mk_p, mk_z, mk_significant).

    Args:
        conn: An open psycopg connection to SERIES_DB.
        rows: List of dicts, one per hylak_id.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.executemany(_upsert_entropy_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d entropy row(s)", len(rows))


# ---------------------------------------------------------------------------
# area_quality table operations
# ---------------------------------------------------------------------------

def _fetch_atlas_area_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, lake_area AS atlas_area
FROM {table}
WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _ensure_area_quality_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def _upsert_area_quality_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (hylak_id, rs_area_mean, rs_area_median, atlas_area, computed_at)
VALUES (%(hylak_id)s, %(rs_area_mean)s, %(rs_area_median)s, %(atlas_area)s, now())
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    rs_area_mean   = EXCLUDED.rs_area_mean,
    rs_area_median = EXCLUDED.rs_area_median,
    atlas_area     = EXCLUDED.atlas_area,
    computed_at    = now();
""").format(
    table=sql.Identifier(tc.series_table("area_quality")),
    conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
)


def _ensure_area_anomalies_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("area_anomalies")))


def _ensure_area_processed_view_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE OR REPLACE VIEW area_processed AS
    SELECT hylak_id FROM {area_quality}
    UNION ALL
    SELECT hylak_id FROM {area_anomalies};
""").format(
    area_quality=sql.Identifier(tc.series_table("area_quality")),
    area_anomalies=sql.Identifier(tc.series_table("area_anomalies")),
)


def _upsert_area_anomalies_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (hylak_id, rs_area_mean, rs_area_median, atlas_area, computed_at)
VALUES (%(hylak_id)s, %(rs_area_mean)s, %(rs_area_median)s, %(atlas_area)s, now())
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    rs_area_mean   = EXCLUDED.rs_area_mean,
    rs_area_median = EXCLUDED.rs_area_median,
    atlas_area     = EXCLUDED.atlas_area,
    computed_at    = now();
""").format(
    table=sql.Identifier(tc.series_table("area_anomalies")),
    conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
)


def _move_area_quality_to_anomalies_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {area_anomalies} (hylak_id, rs_area_mean, rs_area_median, atlas_area, computed_at)
SELECT q.hylak_id, q.rs_area_mean, q.rs_area_median, q.atlas_area, now()
FROM {area_quality} q
WHERE q.hylak_id = ANY(%(id_list)s)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    rs_area_mean   = EXCLUDED.rs_area_mean,
    rs_area_median = EXCLUDED.rs_area_median,
    atlas_area     = EXCLUDED.atlas_area,
    computed_at    = now();
""").format(
    area_anomalies=sql.Identifier(tc.series_table("area_anomalies")),
    area_quality=sql.Identifier(tc.series_table("area_quality")),
    conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
)


def _delete_area_quality_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
DELETE FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def fetch_atlas_area_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float]:
    """Fetch atlas_area (lake_info.lake_area) for a hylak_id range.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to atlas_area (float).
    """
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_atlas_area_chunk_sql(table_config), params)
        rows = cur.fetchall()
    return {int(r[0]): float(r[1]) if r[1] is not None else 0.0 for r in rows}


def _fetch_area_quality_hylak_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
ORDER BY hylak_id
LIMIT %(lim)s OFFSET %(off)s
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def fetch_area_quality_hylak_ids(
    conn: psycopg.Connection,
    *,
    limit: int,
    offset: int = 0,
    table_config: TableConfig = _default_table_config,
) -> list[int]:
    """Return ``hylak_id`` values from ``area_quality`` (stable order, paginated).

    Use with ``series_db.connection_context()`` (SERIES_DB).

    Args:
        conn: Open connection to SERIES_DB.
        limit: Maximum number of ids (e.g. 100).
        offset: SQL ``OFFSET`` for repeatable slices.
        table_config: Table name configuration.

    Returns:
        ``hylak_id`` integers ascending.

    Raises:
        ValueError: If ``limit`` or ``offset`` is invalid.
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")
    params = {"lim": limit, "off": offset}
    with conn.cursor() as cur:
        cur.execute(_fetch_area_quality_hylak_ids_sql(table_config), params)
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def ensure_area_quality_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the area_quality table in SERIES_DB if it does not already exist.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.execute(_ensure_area_quality_table_sql(table_config))
    conn.commit()
    log.debug("Ensured area_quality table exists")


def upsert_area_quality(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update area_quality rows.

    Each dict in rows must contain: hylak_id, rs_area, atlas_area.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        rows: List of dicts, one per hylak_id.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.executemany(_upsert_area_quality_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d area_quality row(s)", len(rows))


def ensure_area_anomalies_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the area_anomalies table and area_processed view in SERIES_DB.

    ``area_anomalies`` stores lakes whose rs_area_median is 0.
    ``area_processed`` is a view that unions ``area_quality`` and
    ``area_anomalies`` on hylak_id, used by ``ChunkedLakeProcessor``
    as the checkpoint table so that all lakes (normal and anomalous)
    are counted toward chunk completion.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.execute(_ensure_area_anomalies_table_sql(table_config))
        cur.execute(_ensure_area_processed_view_sql(table_config))
    conn.commit()
    log.debug("Ensured area_anomalies table and area_processed view exist")


def upsert_area_anomalies(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update area_anomalies rows.

    Each dict in rows must contain: hylak_id, rs_area_mean, rs_area_median, atlas_area.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        rows: List of dicts, one per anomalous hylak_id.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.executemany(_upsert_area_anomalies_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d area_anomalies row(s)", len(rows))


def move_area_quality_to_anomalies(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    """Move selected hylak_ids from area_quality to area_anomalies.

    Copies rows from ``area_quality`` to ``area_anomalies`` (upsert semantics),
    then deletes the same ids from ``area_quality``.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        id_list: Target hylak_id values to move.
        table_config: Table name configuration.

    Returns:
        Number of rows deleted from ``area_quality``.
    """
    if not id_list:
        return 0
    params = {"id_list": [int(hylak_id) for hylak_id in id_list]}
    with conn.cursor() as cur:
        cur.execute(_move_area_quality_to_anomalies_sql(table_config), params)
        cur.execute(_delete_area_quality_by_ids_sql(table_config), params)
        moved = int(cur.rowcount or 0)
    conn.commit()
    log.info("Moved %d hylak_id(s) from area_quality to area_anomalies", moved)
    return moved


def fetch_frozen_year_months_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, set[int]]:
    """Fetch frozen YYYYMM keys for all hylak_ids in [chunk_start, chunk_end).

    Args:
        conn: An open psycopg connection to SERIES_DB.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a set of YYYYMM integer keys.
        Lakes with no frozen months are absent from the returned dict.
    """
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


def ensure_eot_results_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the eot_results and eot_extremes tables in SERIES_DB if they do not exist.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.execute(_ensure_eot_results_table_sql(table_config))
        cur.execute(_ensure_eot_extremes_table_sql(table_config))
    conn.commit()
    log.debug("Ensured eot_results and eot_extremes tables exist")


def upsert_eot_results(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update EOT fit result rows.

    Each dict in rows must contain the keys:
        hylak_id, tail, threshold_quantile,
        converged, log_likelihood, threshold,
        n_extremes, n_observations, n_frozen_months,
        beta0, beta1, sin_1, cos_1, sigma, xi, error_message.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        rows: List of dicts, one per (hylak_id, tail, threshold_quantile) triple.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.executemany(_upsert_eot_results_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d eot_results row(s)", len(rows))


def upsert_eot_extremes(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update EOT extreme-event rows.

    Each dict in rows must contain the keys:
        hylak_id, tail, threshold_quantile, cluster_id,
        cluster_size, year, month, water_area, threshold_at_event.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        rows: List of dicts, one per declustered extreme event.
        table_config: Table name configuration.
    """
    with conn.cursor() as cur:
        cur.executemany(_upsert_eot_extremes_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d eot_extremes row(s)", len(rows))


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


def ensure_monthly_transition_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create monthly transition result and status tables when missing."""
    with conn.cursor() as cur:
        cur.execute(_ensure_monthly_transition_labels_table_sql(table_config))
        cur.execute(_ensure_monthly_transition_extremes_table_sql(table_config))
        cur.execute(_ensure_monthly_transition_abrupt_table_sql(table_config))
        cur.execute(_ensure_monthly_transition_status_table_sql(table_config))
        _ensure_monthly_transition_workflow_versioning(cur, table_config)
        cur.execute(_create_monthly_transition_status_version_index_sql(table_config))
    conn.commit()
    log.debug("Ensured monthly transition tables exist")


def _ensure_monthly_transition_workflow_versioning(
    cur: psycopg.Cursor,
    table_config: TableConfig = _default_table_config,
) -> None:
    for table_name, primary_key_columns in _monthly_transition_versioned_tables(table_config):
        table_ident = sql.Identifier(table_name)
        constraint_ident = sql.Identifier(f"{table_name}_pkey")
        pk_columns_sql = sql.SQL(", ").join(
            sql.Identifier(column_name) for column_name in primary_key_columns
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS workflow_version TEXT"
            ).format(
                table=table_ident,
            )
        )
        cur.execute(
            sql.SQL(
                "UPDATE {table} SET workflow_version = %s WHERE workflow_version IS NULL"
            ).format(table=table_ident),
            (_LEGACY_WORKFLOW_VERSION,),
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {table} ALTER COLUMN workflow_version SET NOT NULL"
            ).format(table=table_ident)
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"
            ).format(
                table=table_ident,
                constraint=constraint_ident,
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {table} ADD PRIMARY KEY ({columns})").format(
                table=table_ident,
                columns=pk_columns_sql,
            )
        )


def upsert_monthly_transition_labels(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition label rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_monthly_transition_labels_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d monthly_transition_labels row(s)", len(rows))


def upsert_monthly_transition_extremes(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition extreme rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_monthly_transition_extremes_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d monthly_transition_extremes row(s)", len(rows))


def upsert_monthly_transition_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition abrupt transition rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_monthly_transition_abrupt_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d monthly_transition_abrupt_transitions row(s)", len(rows))


def upsert_monthly_transition_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition run-status rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_monthly_transition_status_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d monthly_transition_run_status row(s)", len(rows))


def fetch_area_quality_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch quality-filtered source lake ids in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_area_quality_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}


def count_area_quality_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    """Count `area_quality` lakes in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_count_area_quality_in_range_sql(table_config), params)
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def count_monthly_transition_status_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
    table_config: TableConfig = _default_table_config,
) -> int:
    """Count monthly transition run-status rows in a hylak_id range."""
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "workflow_version": workflow_version,
    }
    with conn.cursor() as cur:
        cur.execute(_count_monthly_transition_status_in_range_sql(table_config), params)
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def fetch_monthly_transition_status_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch processed monthly transition hylak_ids in a hylak_id range."""
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "workflow_version": workflow_version,
    }
    with conn.cursor() as cur:
        cur.execute(_fetch_monthly_transition_status_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}


def fetch_max_area_quality_hylak_id(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> int | None:
    """Return the maximum hylak_id present in area_quality."""
    with conn.cursor() as cur:
        cur.execute(_fetch_max_area_quality_hylak_id_sql(table_config))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


_SAFE_SQL_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_sql_identifier(name: str, label: str) -> str:
    if not _SAFE_SQL_IDENT.match(name):
        raise ValueError(f"{label} must match {_SAFE_SQL_IDENT.pattern}, got {name!r}")
    return name


def _lake_geometry_table_sql_ident() -> sql.Composed:
    """Build ``schema.table`` SQL identifier from ``LAKE_GEOMETRY_TABLE``.

    If the env var is unset or empty, uses ``LakeATLAS_v10_pol`` on ALTAS_DB.
    """
    ref = (
        os.environ.get("LAKE_GEOMETRY_TABLE") or ""
    ).strip() or _DEFAULT_LAKE_GEOMETRY_TABLE
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
    conn: psycopg.Connection,
    hylak_ids: list[int],
    *,
    id_column: str | None = None,
    geom_column: str | None = None,
    simplify_tolerance_meters: float | None = None,
) -> pd.DataFrame:
    """Load lake outlines as WKT for use with Earth Engine ``ee.Geometry`` helpers.

    Expects a PostGIS geometry column. Table/schema come from ``LAKE_GEOMETRY_TABLE``,
    or default ``LakeATLAS_v10_pol`` if unset; columns default to ``hylak_id`` and
    ``geom`` and can be overridden with env ``LAKE_GEOMETRY_ID_COLUMN`` /
    ``LAKE_GEOMETRY_GEOM_COLUMN`` or the keyword args.

    When ``simplify_tolerance_meters`` is not set, the env variable
    ``LAKE_GEOMETRY_SIMPLIFY_METERS`` is read if it is a positive float; otherwise
    geometries are not simplified.

    Args:
        conn: Open connection (typically ``ALTAS_DB`` / HydroATLAS side).
        hylak_ids: Lake ids to fetch.
        id_column: Primary key column name (default from env or ``hylak_id``).
        geom_column: Geometry column name (default from env or ``geom``).
        simplify_tolerance_meters: If > 0, apply ``ST_SimplifyPreserveTopology`` in
            Web Mercator (meters), then emit WKT as EPSG:4326 for EE-friendly coords.

    Returns:
        DataFrame with columns ``hylak_id``, ``wkt`` (may be empty if no rows).
    """
    if not hylak_ids:
        return pd.DataFrame(columns=["hylak_id", "wkt"])

    id_col = id_column or os.environ.get("LAKE_GEOMETRY_ID_COLUMN") or "hylak_id"
    geom_col = geom_column or os.environ.get("LAKE_GEOMETRY_GEOM_COLUMN") or "geom"
    _validate_sql_identifier(id_col, "id_column")
    _validate_sql_identifier(geom_col, "geom_column")

    tol = simplify_tolerance_meters
    if tol is None:
        raw = (os.environ.get("LAKE_GEOMETRY_SIMPLIFY_METERS") or "").strip()
        if raw:
            try:
                tol = float(raw)
            except ValueError:
                raise ValueError(
                    f"LAKE_GEOMETRY_SIMPLIFY_METERS must be a float, got {raw!r}"
                ) from None
    if tol is not None and tol <= 0:
        tol = None

    table_ident = _lake_geometry_table_sql_ident()
    if tol is not None:
        log.info(
            "Lake geometry WKT: ST_SimplifyPreserveTopology(%s m in EPSG:3857) -> WKT EPSG:4326",
            tol,
        )
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
