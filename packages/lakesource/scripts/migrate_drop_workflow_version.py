"""Migration script: drop workflow_version columns from quantile/pwm_extreme/eot tables.

This script removes the workflow_version column from all relevant tables.
It should be run during maintenance windows as it requires table rewrites
for large tables (quantile_labels has ~1.2M rows).

Usage:
    python migrate_drop_workflow_version.py [--dry-run]

--dry-run: Print SQL statements without executing.

Tables affected:
    - quantile_labels
    - quantile_extremes
    - quantile_abrupt_transitions
    - quantile_run_status
    - pwm_extreme_thresholds
    - pwm_extreme_run_status
    - eot_run_status

Estimated time:
    - run_status tables: seconds
    - quantile_labels: 5-10 minutes (table rewrite)
    - quantile_extremes: 2-5 minutes
    - quantile_abrupt_transitions: 1-2 minutes
    - pwm_extreme_thresholds: seconds
"""

from __future__ import annotations

import argparse
import time

import psycopg
from lakesource.postgres import series_db


MIGRATIONS = [
    {
        "table": "quantile_labels",
        "old_pk": "monthly_transition_labels_pkey",
        "new_pk_columns": ("hylak_id", "year", "month"),
    },
    {
        "table": "quantile_extremes",
        "old_pk": "monthly_transition_extremes_pkey",
        "new_pk_columns": ("hylak_id", "year", "month", "event_type"),
    },
    {
        "table": "quantile_abrupt_transitions",
        "old_pk": "monthly_transition_abrupt_transitions_pkey",
        "new_pk_columns": ("hylak_id", "from_year", "from_month", "to_year", "to_month", "transition_type"),
    },
    {
        "table": "quantile_run_status",
        "old_pk": "monthly_transition_run_status_pkey",
        "new_pk_columns": ("hylak_id",),
    },
    {
        "table": "pwm_extreme_thresholds",
        "old_pk": "pwm_extreme_thresholds_pkey",
        "new_pk_columns": ("hylak_id", "month"),
    },
    {
        "table": "pwm_extreme_run_status",
        "old_pk": "pwm_extreme_run_status_pkey",
        "new_pk_columns": ("hylak_id",),
    },
    {
        "table": "eot_run_status",
        "old_pk": "eot_run_status_pkey",
        "new_pk_columns": ("hylak_id",),
    },
]

DROP_INDEXES = [
    "quantile_run_status_version_hylak_idx",
]


def get_row_count(cur: psycopg.Cursor, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cur.fetchone()[0])


def has_column(cur: psycopg.Cursor, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone() is not None


def has_pk(cur: psycopg.Cursor, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.table_constraints WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'",
        (table,),
    )
    return cur.fetchone() is not None


def run_migration(conn: psycopg.Connection, dry_run: bool = False) -> None:
    with conn.cursor() as cur:
        for mig in MIGRATIONS:
            table = mig["table"]
            old_pk = mig["old_pk"]
            new_pk_columns = mig["new_pk_columns"]

            print(f"\n=== {table} ===")
            row_count = get_row_count(cur, table)
            print(f"  Rows: {row_count:,}")

            if not has_column(cur, table, "workflow_version"):
                print("  SKIP: workflow_version column does not exist")
                continue

            sqls = [
                f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {old_pk}",
                f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_pkey",
                f"ALTER TABLE {table} DROP COLUMN IF EXISTS workflow_version",
                f"ALTER TABLE {table} ADD PRIMARY KEY ({', '.join(new_pk_columns)})",
            ]

            if dry_run:
                for sql in sqls:
                    print(f"  [DRY-RUN] {sql}")
                continue

            start = time.time()
            for sql in sqls:
                print(f"  Executing: {sql[:60]}...")
                cur.execute(sql)
            conn.commit()
            elapsed = time.time() - start
            print(f"  Done in {elapsed:.2f}s")

        print("\n=== Dropping indexes ===")
        for idx in DROP_INDEXES:
            sql = f"DROP INDEX IF EXISTS {idx}"
            if dry_run:
                print(f"  [DRY-RUN] {sql}")
            else:
                cur.execute(sql)
                conn.commit()
                print(f"  Dropped: {idx}")

    print("\nMigration complete!")


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop workflow_version columns from tables")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    if args.dry_run:
        print("=== DRY RUN MODE ===")

    with series_db.connection_context() as conn:
        run_migration(conn, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
