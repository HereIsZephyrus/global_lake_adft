"""Unified write interface for monthly transition data with backend dispatch."""

from __future__ import annotations

from lakesource.config import Backend, SourceConfig


def _upsert_extremes_postgres(rows: list[dict], config: SourceConfig) -> None:
    from lakesource.postgres.lake import upsert_monthly_transition_extremes
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        upsert_monthly_transition_extremes(conn, rows, commit=True)


def _upsert_transitions_postgres(rows: list[dict], config: SourceConfig) -> None:
    from lakesource.postgres.lake import upsert_monthly_transition_abrupt_transitions
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        upsert_monthly_transition_abrupt_transitions(conn, rows, commit=True)


def _upsert_labels_postgres(rows: list[dict], config: SourceConfig) -> None:
    from lakesource.postgres.lake import upsert_monthly_transition_labels
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        upsert_monthly_transition_labels(conn, rows, commit=True)


def _upsert_run_status_postgres(rows: list[dict], config: SourceConfig) -> None:
    from lakesource.postgres.lake import upsert_monthly_transition_run_status
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        upsert_monthly_transition_run_status(conn, rows, commit=True)


def _ensure_tables_postgres(config: SourceConfig) -> None:
    from lakesource.postgres.lake import ensure_monthly_transition_tables
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        ensure_monthly_transition_tables(conn)


def _upsert_extremes_parquet(rows: list[dict], config: SourceConfig) -> None:
    raise NotImplementedError("Parquet backend for upsert_extremes is not yet implemented")


def _upsert_transitions_parquet(rows: list[dict], config: SourceConfig) -> None:
    raise NotImplementedError("Parquet backend for upsert_transitions is not yet implemented")


def _upsert_labels_parquet(rows: list[dict], config: SourceConfig) -> None:
    raise NotImplementedError("Parquet backend for upsert_labels is not yet implemented")


def _upsert_run_status_parquet(rows: list[dict], config: SourceConfig) -> None:
    raise NotImplementedError("Parquet backend for upsert_run_status is not yet implemented")


def _ensure_tables_parquet(config: SourceConfig) -> None:
    raise NotImplementedError("Parquet backend for ensure_tables is not yet implemented")


def upsert_extremes(rows: list[dict], config: SourceConfig) -> None:
    if config.backend == Backend.POSTGRES:
        _upsert_extremes_postgres(rows, config)
    else:
        _upsert_extremes_parquet(rows, config)


def upsert_transitions(rows: list[dict], config: SourceConfig) -> None:
    if config.backend == Backend.POSTGRES:
        _upsert_transitions_postgres(rows, config)
    else:
        _upsert_transitions_parquet(rows, config)


def upsert_labels(rows: list[dict], config: SourceConfig) -> None:
    if config.backend == Backend.POSTGRES:
        _upsert_labels_postgres(rows, config)
    else:
        _upsert_labels_parquet(rows, config)


def upsert_run_status(rows: list[dict], config: SourceConfig) -> None:
    if config.backend == Backend.POSTGRES:
        _upsert_run_status_postgres(rows, config)
    else:
        _upsert_run_status_parquet(rows, config)


def ensure_tables(config: SourceConfig) -> None:
    if config.backend == Backend.POSTGRES:
        _ensure_tables_postgres(config)
    else:
        _ensure_tables_parquet(config)
