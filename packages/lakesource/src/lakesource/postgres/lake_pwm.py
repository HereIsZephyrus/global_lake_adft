"""Database operations for PWM extreme quantile tables."""

from __future__ import annotations

from typing import Any
import logging

import pandas as pd
import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _ensure_pwm_extreme_thresholds_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    mean_area           DOUBLE PRECISION,
    epsilon             DOUBLE PRECISION,
    lambda_0            DOUBLE PRECISION,
    lambda_1            DOUBLE PRECISION,
    lambda_2            DOUBLE PRECISION,
    lambda_3            DOUBLE PRECISION,
    lambda_4            DOUBLE PRECISION,
    b_0                 DOUBLE PRECISION,
    b_1                 DOUBLE PRECISION,
    b_2                 DOUBLE PRECISION,
    b_3                 DOUBLE PRECISION,
    b_4                 DOUBLE PRECISION,
    threshold_high      DOUBLE PRECISION,
    threshold_low       DOUBLE PRECISION,
    converged           BOOLEAN,
    objective_value     DOUBLE PRECISION,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, month)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_thresholds")))


def _ensure_pwm_extreme_labels_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    year                INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    water_area          DOUBLE PRECISION,
    index_value         DOUBLE PRECISION,
    threshold_low       DOUBLE PRECISION,
    threshold_high      DOUBLE PRECISION,
    extreme_label       TEXT,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, year, month)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_labels")))


def _ensure_pwm_extreme_extremes_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    year                INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    event_type          TEXT,
    water_area          DOUBLE PRECISION,
    index_value         DOUBLE PRECISION,
    threshold           DOUBLE PRECISION,
    severity            DOUBLE PRECISION,
    extreme_label       TEXT,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, year, month)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_extremes")))


def _ensure_pwm_extreme_abrupt_transitions_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    from_year           INTEGER      NOT NULL,
    from_month          INTEGER      NOT NULL,
    to_year             INTEGER      NOT NULL,
    to_month            INTEGER      NOT NULL,
    transition_type     TEXT,
    from_water_area     DOUBLE PRECISION,
    to_water_area       DOUBLE PRECISION,
    from_label          TEXT,
    to_label            TEXT,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, from_year, from_month, to_year, to_month)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_abrupt_transitions")))


def _ensure_pwm_hawkes_segments_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id             INTEGER      NOT NULL,
    segment_id           INTEGER      NOT NULL,
    start_year           INTEGER      NOT NULL,
    start_month          INTEGER      NOT NULL,
    end_year             INTEGER      NOT NULL,
    end_month            INTEGER      NOT NULL,
    duration_months      INTEGER,
    segment_type         TEXT,
    has_high             BOOLEAN,
    has_low              BOOLEAN,
    max_C                DOUBLE PRECISION,
    mean_C               DOUBLE PRECISION,
    integral_C           DOUBLE PRECISION,
    n_extreme_events     INTEGER,
    first_extreme_type   TEXT,
    last_extreme_type    TEXT,
    workflow_version     TEXT,
    computed_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, segment_id)
);
""").format(table=sql.Identifier(tc.series_table("pwm_hawkes_segments")))


def _ensure_pwm_extreme_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    chunk_start       INTEGER,
    chunk_end         INTEGER,
    status            TEXT         NOT NULL,
    error_message     TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_run_status")))


def _upsert_pwm_extreme_thresholds_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, month,
    mean_area, epsilon,
    lambda_0, lambda_1, lambda_2, lambda_3, lambda_4,
    b_0, b_1, b_2, b_3, b_4,
    threshold_high, threshold_low, converged, objective_value, computed_at
) VALUES (
    %(hylak_id)s, %(month)s,
    %(mean_area)s, %(epsilon)s,
    %(lambda_0)s, %(lambda_1)s, %(lambda_2)s, %(lambda_3)s, %(lambda_4)s,
    %(b_0)s, %(b_1)s, %(b_2)s, %(b_3)s, %(b_4)s,
    %(threshold_high)s, %(threshold_low)s, %(converged)s, %(objective_value)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    mean_area       = EXCLUDED.mean_area,
    epsilon         = EXCLUDED.epsilon,
    lambda_0        = EXCLUDED.lambda_0,
    lambda_1        = EXCLUDED.lambda_1,
    lambda_2        = EXCLUDED.lambda_2,
    lambda_3        = EXCLUDED.lambda_3,
    lambda_4        = EXCLUDED.lambda_4,
    b_0             = EXCLUDED.b_0,
    b_1             = EXCLUDED.b_1,
    b_2             = EXCLUDED.b_2,
    b_3             = EXCLUDED.b_3,
    b_4             = EXCLUDED.b_4,
    threshold_high  = EXCLUDED.threshold_high,
    threshold_low   = EXCLUDED.threshold_low,
    converged       = EXCLUDED.converged,
    objective_value = EXCLUDED.objective_value,
    computed_at     = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_thresholds")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "month")
        ),
    )


def _upsert_pwm_extreme_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, chunk_start, chunk_end, status, error_message, computed_at
) VALUES (
    %(hylak_id)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id",)
        ),
    )


def _upsert_pwm_extreme_labels_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, year, month,
    water_area, index_value,
    threshold_low, threshold_high, extreme_label, computed_at
) VALUES (
    %(hylak_id)s, %(year)s, %(month)s,
    %(water_area)s, %(index_value)s,
    %(threshold_low)s, %(threshold_high)s, %(extreme_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    water_area      = EXCLUDED.water_area,
    index_value     = EXCLUDED.index_value,
    threshold_low   = EXCLUDED.threshold_low,
    threshold_high  = EXCLUDED.threshold_high,
    extreme_label   = EXCLUDED.extreme_label,
    computed_at     = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_labels")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "year", "month")
        ),
    )


def _upsert_pwm_extreme_extremes_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, year, month,
    event_type, water_area, index_value,
    threshold, severity, extreme_label, computed_at
) VALUES (
    %(hylak_id)s, %(year)s, %(month)s,
    %(event_type)s, %(water_area)s, %(index_value)s,
    %(threshold)s, %(severity)s, %(extreme_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    event_type      = EXCLUDED.event_type,
    water_area      = EXCLUDED.water_area,
    index_value     = EXCLUDED.index_value,
    threshold       = EXCLUDED.threshold,
    severity        = EXCLUDED.severity,
    extreme_label   = EXCLUDED.extreme_label,
    computed_at     = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_extremes")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "year", "month")
        ),
    )


def _upsert_pwm_extreme_abrupt_transitions_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, from_year, from_month, to_year, to_month,
    transition_type,
    from_water_area, to_water_area, from_label, to_label, computed_at
) VALUES (
    %(hylak_id)s, %(from_year)s, %(from_month)s, %(to_year)s, %(to_month)s,
    %(transition_type)s,
    %(from_water_area)s, %(to_water_area)s, %(from_label)s, %(to_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    transition_type     = EXCLUDED.transition_type,
    from_water_area     = EXCLUDED.from_water_area,
    to_water_area       = EXCLUDED.to_water_area,
    from_label          = EXCLUDED.from_label,
    to_label            = EXCLUDED.to_label,
    computed_at         = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_abrupt_transitions")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in (
                "hylak_id",
                "from_year",
                "from_month",
                "to_year",
                "to_month",
            )
        ),
    )


def _upsert_pwm_hawkes_segments_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, segment_id,
    start_year, start_month, end_year, end_month,
    duration_months, segment_type,
    has_high, has_low,
    max_C, mean_C, integral_C,
    n_extreme_events,
    first_extreme_type, last_extreme_type,
    workflow_version, computed_at
) VALUES (
    %(hylak_id)s, %(segment_id)s,
    %(start_year)s, %(start_month)s, %(end_year)s, %(end_month)s,
    %(duration_months)s, %(segment_type)s,
    %(has_high)s, %(has_low)s,
    %(max_C)s, %(mean_C)s, %(integral_C)s,
    %(n_extreme_events)s,
    %(first_extreme_type)s, %(last_extreme_type)s,
    %(workflow_version)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    start_year         = EXCLUDED.start_year,
    start_month        = EXCLUDED.start_month,
    end_year           = EXCLUDED.end_year,
    end_month          = EXCLUDED.end_month,
    duration_months    = EXCLUDED.duration_months,
    segment_type       = EXCLUDED.segment_type,
    has_high           = EXCLUDED.has_high,
    has_low            = EXCLUDED.has_low,
    max_C              = EXCLUDED.max_C,
    mean_C             = EXCLUDED.mean_C,
    integral_C         = EXCLUDED.integral_C,
    n_extreme_events   = EXCLUDED.n_extreme_events,
    first_extreme_type = EXCLUDED.first_extreme_type,
    last_extreme_type  = EXCLUDED.last_extreme_type,
    workflow_version   = EXCLUDED.workflow_version,
    computed_at        = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_hawkes_segments")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "segment_id")
        ),
    )


def _count_pwm_extreme_status_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT COUNT(*)
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
  AND status = 'done'
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_run_status")))


def _fetch_pwm_extreme_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
  AND status = 'done'
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_run_status")))


def ensure_pwm_extreme_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create all PWM extreme quantile tables if they do not exist."""
    with conn.cursor() as cur:
        cur.execute(_ensure_pwm_extreme_thresholds_table_sql(table_config))
        cur.execute(_ensure_pwm_extreme_status_table_sql(table_config))
        cur.execute(_ensure_pwm_extreme_labels_table_sql(table_config))
        cur.execute(_ensure_pwm_extreme_extremes_table_sql(table_config))
        cur.execute(_ensure_pwm_extreme_abrupt_transitions_table_sql(table_config))
        cur.execute(_ensure_pwm_hawkes_run_status_table_sql(table_config))
        cur.execute(_ensure_pwm_hawkes_segments_table_sql(table_config))
    conn.commit()


def upsert_pwm_extreme_thresholds(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme threshold rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_thresholds_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_thresholds row(s)", len(rows))


def upsert_pwm_extreme_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme run status rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_status_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_run_status row(s)", len(rows))


def upsert_pwm_extreme_labels(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme label rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_labels_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_labels row(s)", len(rows))


def upsert_pwm_extreme_extremes(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme extreme-event rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_extremes_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_extremes row(s)", len(rows))


def upsert_pwm_extreme_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme abrupt-transition rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_abrupt_transitions_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_abrupt_transitions row(s)", len(rows))


def count_pwm_extreme_status_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
    }
    with conn.cursor() as cur:
        cur.execute(
            _count_pwm_extreme_status_in_range_sql(table_config),
            params,
        )
        return int(cur.fetchone()[0])


def fetch_pwm_extreme_status_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
    }
    with conn.cursor() as cur:
        cur.execute(
            _fetch_pwm_extreme_status_ids_in_range_sql(table_config),
            params,
        )
        return {int(row[0]) for row in cur.fetchall()}


def _ensure_pwm_hawkes_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    chunk_start       INTEGER,
    chunk_end         INTEGER,
    status            TEXT         NOT NULL,
    error_message     TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("pwm_hawkes_run_status")))


def _upsert_pwm_hawkes_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, chunk_start, chunk_end, status, error_message, computed_at
) VALUES (
    %(hylak_id)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_hawkes_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id",)
        ),
    )


def ensure_pwm_hawkes_run_status_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the pwm_hawkes_run_status table if it does not exist."""
    with conn.cursor() as cur:
        cur.execute(_ensure_pwm_hawkes_run_status_table_sql(table_config))
    conn.commit()


def upsert_pwm_hawkes_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM-Hawkes run status rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_hawkes_run_status_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_hawkes_run_status row(s)", len(rows))


def _fetch_pwm_extreme_extremes_by_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, year, month, event_type, water_area, threshold, severity, extreme_label
FROM {table}
WHERE hylak_id = %(hylak_id)s
ORDER BY year, month
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_extremes")))


def _fetch_pwm_extreme_labels_by_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, year, month, water_area, threshold_low, threshold_high, extreme_label
FROM {table}
WHERE hylak_id = %(hylak_id)s
ORDER BY year, month
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_labels")))


def fetch_pwm_extreme_extremes_by_id(
    conn: psycopg.Connection,
    hylak_id: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Fetch PWM extreme event rows for one lake.

    Returns:
        DataFrame with columns: hylak_id, year, month, event_type,
        water_area, threshold, severity, extreme_label.
    """
    with conn.cursor() as cur:
        cur.execute(
            _fetch_pwm_extreme_extremes_by_id_sql(table_config),
            {"hylak_id": int(hylak_id)},
        )
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return df

    for col in ("hylak_id", "year", "month"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("water_area", "threshold", "severity"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "event_type" in df.columns:
        df["event_type"] = df["event_type"].astype(str)
    if "extreme_label" in df.columns:
        df["extreme_label"] = df["extreme_label"].astype(str)
    return df


def fetch_pwm_extreme_labels_by_id(
    conn: psycopg.Connection,
    hylak_id: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Fetch PWM extreme labels for one lake.

    Returns:
        DataFrame with columns: hylak_id, year, month, water_area,
        threshold_low, threshold_high, extreme_label.
    """
    with conn.cursor() as cur:
        cur.execute(
            _fetch_pwm_extreme_labels_by_id_sql(table_config),
            {"hylak_id": int(hylak_id)},
        )
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return df

    for col in ("hylak_id", "year", "month"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("water_area", "threshold_low", "threshold_high"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "extreme_label" in df.columns:
        df["extreme_label"] = df["extreme_label"].astype(str)
    return df


def ensure_pwm_hawkes_segments_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the pwm_hawkes_segments table if it does not exist."""
    with conn.cursor() as cur:
        cur.execute(_ensure_pwm_hawkes_segments_table_sql(table_config))
    conn.commit()


def upsert_pwm_hawkes_segments(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM-Hawkes segments rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_hawkes_segments_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_hawkes_segments row(s)", len(rows))
