"""Public SQL template fragments for postgres module."""

from __future__ import annotations

from psycopg import sql

CHUNK_WHERE = "WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint"

COMPUTED_AT_COL = "computed_at TIMESTAMPTZ DEFAULT now()"


def year_month_extract_sql(table_alias: str = "") -> sql.Composed:
    """Generate year/month extraction SQL from year_month column.

    Args:
        table_alias: Optional table alias prefix.

    Returns:
        SQL fragment extracting year and month from year_month column.
    """
    prefix = f"{table_alias}." if table_alias else ""
    return sql.SQL(
        f"EXTRACT(YEAR FROM {prefix}year_month)::int AS year, "
        f"EXTRACT(MONTH FROM {prefix}year_month)::int AS month"
    )


def year_month_key_sql(prefix: str = "") -> sql.Composed:
    """Generate YYYYMM integer key from year_month column.

    Args:
        prefix: Optional table alias prefix.

    Returns:
        SQL fragment computing YYYYMM as integer.
    """
    p = f"{prefix}." if prefix else ""
    return sql.SQL(
        f"(EXTRACT(YEAR FROM {p}year_month)::int * 100 "
        f"+ EXTRACT(MONTH FROM {p}year_month)::int) AS year_month_key"
    )


def upsert_set_clause(columns: list[str]) -> sql.Composed:
    """Generate SET clause for ON CONFLICT DO UPDATE.

    Args:
        columns: List of column names to upsert.

    Returns:
        SQL fragment: col1 = EXCLUDED.col1, col2 = EXCLUDED.col2, ...
    """
    assignments = [
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in columns
    ]
    assignments.append(sql.SQL("computed_at = now()"))
    return sql.SQL(", ").join(assignments)


def upsert_conflict_clause(primary_keys: list[str]) -> sql.Composed:
    """Generate ON CONFLICT clause.

    Args:
        primary_keys: List of primary key column names.

    Returns:
        SQL fragment: ON CONFLICT (key1, key2) DO UPDATE SET ...
    """
    keys = sql.SQL(", ").join(sql.Identifier(k) for k in primary_keys)
    return sql.SQL("ON CONFLICT ({}) DO UPDATE SET ").format(keys)


def build_upsert_sql(
    table: sql.Identifier,
    columns: list[str],
    primary_keys: list[str],
) -> sql.Composed:
    """Build a complete INSERT ... ON CONFLICT ... DO UPDATE SET statement.

    Args:
        table: SQL identifier for the table.
        columns: List of column names to insert/update.
        primary_keys: List of primary key column names for conflict detection.

    Returns:
        Complete upsert SQL statement.
    """
    col_ids = [sql.Identifier(c) for c in columns]
    placeholders = [sql.Placeholder(c) for c in columns]

    insert_cols = sql.SQL(", ").join(col_ids)
    insert_vals = sql.SQL(", ").join(placeholders)

    set_clause = upsert_set_clause(columns)
    conflict_clause = upsert_conflict_clause(primary_keys)

    return sql.SQL(
        "INSERT INTO {tbl} ({cols}) VALUES ({vals}) "
        "{conflict}{set}"
    ).format(
        tbl=table,
        cols=insert_cols,
        vals=insert_vals,
        conflict=conflict_clause,
        set=set_clause,
    )


def table_exists_sql(table: sql.Identifier) -> sql.Composed:
    """Generate CREATE TABLE IF NOT EXISTS statement skeleton.

    Args:
        table: SQL identifier for the table.

    Returns:
        CREATE TABLE IF NOT EXISTS {table} (...) SQL fragment.
    """
    return sql.SQL("CREATE TABLE IF NOT EXISTS {tbl} (").format(tbl=table)


def ensure_table_with_columns(
    table: sql.Identifier,
    columns_and_types: list[tuple[str, str]],
    primary_key: str | None = None,
    extra_columns: list[str] | None = None,
) -> sql.Composed:
    """Build CREATE TABLE IF NOT EXISTS with common computed_at column.

    Args:
        table: SQL identifier for the table.
        columns_and_types: List of (column_name, type) tuples.
        primary_key: Optional primary key column name.
        extra_columns: Optional list of extra column definitions (e.g., "PRIMARY KEY (...)").

    Returns:
        Complete CREATE TABLE IF NOT EXISTS statement.
    """
    col_defs: list[sql.Composed] = []
    for col_name, col_type in columns_and_types:
        col_defs.append(
            sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
        )

    col_defs.append(sql.SQL("computed_at TIMESTAMPTZ DEFAULT now()"))

    if primary_key:
        col_defs.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(primary_key)))

    if extra_columns:
        for extra in extra_columns:
            col_defs.append(sql.SQL(extra))

    return sql.SQL("CREATE TABLE IF NOT EXISTS {tbl} ({cols})").format(
        tbl=table,
        cols=sql.SQL(", ").join(col_defs),
    )
