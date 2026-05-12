"""CLI commands for data synchronisation between parquet and PostgreSQL.

Provides three subcommands:
- ``status``: compare row counts and timestamps between parquet and postgres
- ``push``: bulk-load parquet data into postgres (TRUNCATE + COPY)
- ``pull``: export postgres tables to parquet (full overwrite)
"""

from __future__ import annotations

import io
import logging
import time
from pathlib import Path
from typing import Annotated

import typer

from ._common import setup_logging

log = logging.getLogger(__name__)

app = typer.Typer(help="Parquet ↔ PostgreSQL data sync", no_args_is_help=True)

# ── Syncable tables ─────────────────────────────────────────────────────────

SYNC_TABLES: list[str] = [
    "eot_results",
    "eot_extremes",
    "quantile_labels",
    "quantile_extremes",
    "quantile_abrupt_transitions",
    "pwm_extreme_thresholds",
    "hawkes_results",
    "hawkes_lrt",
    "hawkes_transition_monthly",
    "area_shift_labels",
    "area_quality",
    "area_anomalies",
]

# Maps each sync table to the ensure_table key used by PostgresLakeProvider.
TABLE_TO_ENSURE_KEY: dict[str, str] = {
    "eot_results": "eot",
    "eot_extremes": "eot",
    "quantile_labels": "quantile",
    "quantile_extremes": "quantile",
    "quantile_abrupt_transitions": "quantile",
    "pwm_extreme_thresholds": "pwm_extreme",
    "hawkes_results": "hawkes",
    "hawkes_lrt": "hawkes",
    "hawkes_transition_monthly": "hawkes",
    "area_shift_labels": "area_shift_labels",
    "area_quality": "area_quality",
    "area_anomalies": "area_anomalies",
}

# ── Shared option types ─────────────────────────────────────────────────────

TableOpt = Annotated[
    str | None,
    typer.Option("--table", "-t", help="Single table to sync (omit for --all)"),
]
AllOpt = Annotated[
    bool,
    typer.Option("--all", "-a", help="Sync all tables"),
]
DataDirOpt = Annotated[
    Path | None,
    typer.Option("--data-dir", help="Parquet data directory (default: PARQUET_DATA_DIR env)"),
]
ChunkSizeOpt = Annotated[
    int,
    typer.Option("--chunk-size", "-c", help="Rows per chunk for bulk operations"),
]
ForceOpt = Annotated[
    bool,
    typer.Option("--force", "-f", help="Skip safety checks"),
]
DryRunOpt = Annotated[
    bool,
    typer.Option("--dry-run", help="Print plan only, do not execute"),
]


# ── Helpers ─────────────────────────────────────────────────────────────────


def _resolve_data_dir(data_dir: Path | None) -> Path:
    """Resolve parquet data directory from option or SourceConfig (PARQUET_DATA_DIR)."""
    if data_dir is not None:
        return data_dir.resolve()

    from lakesource.config import SourceConfig

    config = SourceConfig()
    if config.data_dir is not None:
        return config.data_dir.resolve()

    # SourceConfig reads PARQUET_DATA_DIR only when backend=parquet.
    # For sync we always need it, so read env directly as last resort.
    import os

    from lakesource.env import load_env

    load_env()
    env_dir = os.environ.get("PARQUET_DATA_DIR")
    if env_dir:
        return Path(env_dir).resolve()
    raise typer.BadParameter(
        "Parquet data directory not specified. "
        "Set PARQUET_DATA_DIR env or pass --data-dir."
    )


def _resolve_tables(table: str | None, all_flag: bool) -> list[str]:
    """Resolve which tables to operate on."""
    if table and all_flag:
        raise typer.BadParameter("Cannot specify both --table and --all")
    if not table and not all_flag:
        raise typer.BadParameter("Must specify --table or --all")
    if all_flag:
        return list(SYNC_TABLES)
    if table not in SYNC_TABLES:
        raise typer.BadParameter(
            f"Unknown table: {table!r}. Available: {', '.join(SYNC_TABLES)}"
        )
    return [table]


def _parquet_path(data_dir: Path, table: str) -> Path:
    """Return the parquet file or directory path for a table.

    Uses TableConfig to resolve the logical table name to a file stem,
    then checks the filesystem: directory (chunked) takes priority over
    single file.
    """
    from lakesource.table_config import TableConfig

    tc = TableConfig.default()
    file_stem = tc.parquet_file(table)

    # Directory (chunked) layout: data_dir/stem/*.parquet
    dir_path = data_dir / file_stem
    if dir_path.is_dir():
        return dir_path

    # Single file layout: data_dir/stem.parquet
    return data_dir / f"{file_stem}.parquet"


def _parquet_row_count(path: Path) -> int:
    """Count rows in a parquet file or directory without loading data."""
    import pyarrow.parquet as pq

    if path.is_dir():
        total = 0
        for f in sorted(path.glob("*.parquet")):
            total += pq.read_metadata(f).num_rows
        return total
    return pq.read_metadata(path).num_rows


def _parquet_columns(path: Path) -> list[str]:
    """Get column names from a parquet file or directory."""
    import pyarrow.parquet as pq

    if path.is_dir():
        first = next(path.glob("*.parquet"), None)
        if first is None:
            return []
        return pq.read_schema(first).names
    return pq.read_schema(path).names


def _pg_estimated_rows(cur: object, table: str) -> int:
    """Get estimated row count from pg_class (fast, no full scan)."""
    cur.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE relname = %s",
        (table,),
    )
    row = cur.fetchone()
    if row is None:
        return 0
    return max(0, int(row[0]))


def _pg_max_timestamp(cur: object, table: str) -> str:
    """Get MAX(computed_at) from a table. Returns string or 'N/A'."""
    # eot_run_status uses created_at, but we don't sync run_status tables
    try:
        cur.execute(f"SELECT MAX(computed_at) FROM {table}")  # noqa: S608
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])[:19]
    except Exception:
        pass
    return "N/A"


# ── Commands ────────────────────────────────────────────────────────────────


@app.command()
def status(
    table: TableOpt = None,
    data_dir: DataDirOpt = None,
) -> None:
    """Compare parquet and PostgreSQL row counts and timestamps."""
    setup_logging("sync-status")
    from lakesource.postgres.client import series_db

    parquet_dir = _resolve_data_dir(data_dir)
    tables = [table] if table else list(SYNC_TABLES)

    # Validate single table
    if table and table not in SYNC_TABLES:
        raise typer.BadParameter(
            f"Unknown table: {table!r}. Available: {', '.join(SYNC_TABLES)}"
        )

    header = f"{'Table':<35} {'Parquet':>10} {'Postgres':>10} {'Last Sync':>20} {'Status':<10}"
    typer.echo(header)
    typer.echo("-" * len(header))

    with series_db.connect() as conn:
        with conn.cursor() as cur:
            for t in tables:
                path = _parquet_path(parquet_dir, t)
                # Parquet rows
                if path.exists():
                    pq_rows = _parquet_row_count(path)
                else:
                    pq_rows = -1

                # Postgres rows (estimated)
                pg_rows = _pg_estimated_rows(cur, t)

                # Timestamp (skip for very large tables to avoid timeout)
                if pg_rows < 2_000_000:
                    ts = _pg_max_timestamp(cur, t)
                else:
                    ts = "(large table)"

                # Status
                if pq_rows < 0:
                    st = "NO FILE"
                elif pg_rows <= 0:
                    st = "EMPTY PG"
                elif pq_rows > pg_rows * 2:
                    st = "STALE"
                elif pq_rows < pg_rows * 0.1 and pg_rows > 100:
                    st = "PQ SMALL"
                else:
                    st = "OK"

                pq_str = str(pq_rows) if pq_rows >= 0 else "N/A"
                typer.echo(
                    f"{t:<35} {pq_str:>10} {pg_rows:>10} {ts:>20} {st:<10}"
                )


@app.command()
def push(
    table: TableOpt = None,
    all_flag: AllOpt = False,
    chunk_size: ChunkSizeOpt = 50_000,
    force: ForceOpt = False,
    dry_run: DryRunOpt = False,
    data_dir: DataDirOpt = None,
) -> None:
    """Push parquet data to PostgreSQL (TRUNCATE + COPY, full replace)."""
    setup_logging("sync-push")
    import pandas as pd
    import pyarrow.parquet as pq

    from lakesource.config import SourceConfig
    from lakesource.postgres.client import series_db
    from lakesource.provider.postgres_provider import PostgresLakeProvider

    parquet_dir = _resolve_data_dir(data_dir)
    tables = _resolve_tables(table, all_flag)
    provider = PostgresLakeProvider(SourceConfig())

    results: list[tuple[str, str, int]] = []  # (table, status, rows)

    with series_db.connect() as conn:
        with conn.cursor() as cur:
            for t in tables:
                path = _parquet_path(parquet_dir, t)
                if not path.exists():
                    typer.echo(f"  {t}: SKIP (parquet not found: {path})")
                    results.append((t, "SKIP", 0))
                    continue

                pq_rows = _parquet_row_count(path)
                pg_rows = _pg_estimated_rows(cur, t)

                # Safety check
                if not force and pg_rows > 100 and pq_rows < pg_rows * 0.1:
                    typer.echo(
                        f"  {t}: SKIP (parquet={pq_rows:,} << postgres={pg_rows:,}). "
                        f"Use --force to override."
                    )
                    results.append((t, "SKIP-SAFETY", 0))
                    continue

                if dry_run:
                    typer.echo(
                        f"  {t}: WOULD PUSH {pq_rows:,} rows "
                        f"(current postgres: {pg_rows:,})"
                    )
                    results.append((t, "DRY-RUN", pq_rows))
                    continue

                # Ensure table DDL exists
                ensure_key = TABLE_TO_ENSURE_KEY[t]
                try:
                    provider.ensure_table(ensure_key)
                except Exception as e:
                    typer.echo(f"  {t}: ERROR ensure_table: {e}")
                    results.append((t, "ERROR", 0))
                    continue

                # Push: TRUNCATE + COPY in one transaction
                t0 = time.time()
                try:
                    _push_table(conn, cur, t, path, chunk_size)
                    conn.commit()
                    elapsed = time.time() - t0
                    typer.echo(
                        f"  {t}: OK ({pq_rows:,} rows in {elapsed:.1f}s)"
                    )
                    results.append((t, "OK", pq_rows))
                except Exception as e:
                    conn.rollback()
                    typer.echo(f"  {t}: ERROR ({e})")
                    log.exception("Push failed for %s", t)
                    results.append((t, "ERROR", 0))

    # Summary
    typer.echo("")
    ok_count = sum(1 for _, s, _ in results if s == "OK")
    total_rows = sum(r for _, s, r in results if s == "OK")
    typer.echo(f"Done: {ok_count}/{len(tables)} tables pushed, {total_rows:,} total rows.")


def _push_table(
    conn: object,
    cur: object,
    table: str,
    path: Path,
    chunk_size: int,
) -> None:
    """Execute TRUNCATE + COPY for a single table within the current transaction."""
    import pyarrow.parquet as pq

    # Get columns from parquet, exclude computed_at (has DEFAULT now())
    all_columns = _parquet_columns(path)
    columns = [c for c in all_columns if c != "computed_at"]

    if not columns:
        raise ValueError(f"No columns found in {path}")

    # Query postgres integer columns for type coercion
    int_columns = _get_pg_integer_columns(cur, table)

    # TRUNCATE
    cur.execute(f"TRUNCATE {table}")

    # COPY FROM STDIN
    col_list = ", ".join(columns)
    copy_sql = f"COPY {table}({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')"

    with cur.copy(copy_sql) as copy:
        if path.is_dir():
            for f in sorted(path.glob("*.parquet")):
                _copy_parquet_file(copy, f, columns, chunk_size, int_columns)
        else:
            _copy_parquet_file(copy, path, columns, chunk_size, int_columns)


def _get_pg_integer_columns(cur: object, table: str) -> set[str]:
    """Query information_schema to find integer-typed columns for a table."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s AND data_type IN ('integer', 'bigint', 'smallint')",
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def _coerce_int_columns(df: object, int_columns: set[str]) -> None:
    """Convert float columns that should be integer to nullable Int64 in-place.

    Parquet stores nullable integers as float64 (NaN for NULL). When writing
    CSV for COPY, postgres rejects '1.0' for an integer column. Converting to
    pandas nullable Int64 produces clean integer strings with <NA> for nulls.
    """
    import pandas as pd

    for col in df.columns:
        if col in int_columns and df[col].dtype.kind == "f":
            df[col] = df[col].astype("Int64")


def _copy_parquet_file(
    copy: object,
    file_path: Path,
    columns: list[str],
    chunk_size: int,
    int_columns: set[str] | None = None,
) -> None:
    """Stream a parquet file into a COPY operation in chunks."""
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(batch_size=chunk_size, columns=columns):
        df = batch.to_pandas()
        if int_columns:
            _coerce_int_columns(df, int_columns)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="")
        copy.write(buf.getvalue())


@app.command()
def pull(
    table: TableOpt = None,
    all_flag: AllOpt = False,
    chunk_size: ChunkSizeOpt = 200_000,
    data_dir: DataDirOpt = None,
) -> None:
    """Pull PostgreSQL tables to parquet (full overwrite)."""
    setup_logging("sync-pull")
    import pandas as pd

    from lakesource.postgres.client import series_db

    parquet_dir = _resolve_data_dir(data_dir)
    tables = _resolve_tables(table, all_flag)

    results: list[tuple[str, str, int]] = []

    for t in tables:
        t0 = time.time()
        try:
            total_rows = _pull_table(series_db, t, parquet_dir, chunk_size)
            elapsed = time.time() - t0
            typer.echo(f"  {t}: OK ({total_rows:,} rows in {elapsed:.1f}s)")
            results.append((t, "OK", total_rows))
        except Exception as e:
            typer.echo(f"  {t}: ERROR ({e})")
            log.exception("Pull failed for %s", t)
            results.append((t, "ERROR", 0))

    # Summary
    typer.echo("")
    ok_count = sum(1 for _, s, _ in results if s == "OK")
    total_rows = sum(r for _, s, r in results if s == "OK")
    typer.echo(f"Done: {ok_count}/{len(tables)} tables pulled, {total_rows:,} total rows.")


def _pull_table(
    db_client: object,
    table: str,
    parquet_dir: Path,
    chunk_size: int,
) -> int:
    """Export a single postgres table to parquet."""
    import pandas as pd

    # Determine max hylak_id
    with db_client.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(hylak_id) FROM {table}")  # noqa: S608
            row = cur.fetchone()
            max_id = int(row[0]) if row and row[0] else 0

    if max_id == 0:
        typer.echo(f"  {table}: empty table, skipping")
        return 0

    # Determine output format: if existing path is a directory, use chunked
    out_path = _parquet_path(parquet_dir, table)
    if out_path.is_dir():
        return _pull_chunked(db_client, table, out_path, chunk_size, max_id)
    return _pull_single(db_client, table, out_path, chunk_size, max_id)


def _pull_single(
    db_client: object,
    table: str,
    out_path: Path,
    chunk_size: int,
    max_id: int,
) -> int:
    """Pull a table into a single parquet file."""
    import pandas as pd

    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = []
    with db_client.connect() as conn:
        with conn.cursor() as cur:
            for start in range(0, max_id + 1, chunk_size):
                end = start + chunk_size
                cur.execute(
                    f"SELECT * FROM {table} WHERE hylak_id >= %s AND hylak_id < %s",
                    (start, end),
                )
                rows = cur.fetchall()
                if rows:
                    columns = [desc.name for desc in cur.description]
                    frames.append(pd.DataFrame(rows, columns=columns))

    if not frames:
        return 0

    combined = pd.concat(frames, ignore_index=True)
    combined.to_parquet(out_path, index=False)
    return len(combined)


def _pull_chunked(
    db_client: object,
    table: str,
    out_dir: Path,
    chunk_size: int,
    max_id: int,
) -> int:
    """Pull a table into a chunked parquet directory."""
    import pandas as pd
    import shutil

    # Clear existing chunks
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    with db_client.connect() as conn:
        with conn.cursor() as cur:
            for start in range(0, max_id + 1, chunk_size):
                end = start + chunk_size
                cur.execute(
                    f"SELECT * FROM {table} WHERE hylak_id >= %s AND hylak_id < %s",
                    (start, end),
                )
                rows = cur.fetchall()
                if not rows:
                    continue
                columns = [desc.name for desc in cur.description]
                df = pd.DataFrame(rows, columns=columns)
                df.to_parquet(out_dir / f"{start:08d}.parquet", index=False)
                total += len(df)

    return total
