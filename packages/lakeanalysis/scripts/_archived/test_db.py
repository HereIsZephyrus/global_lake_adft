"""Runnable script to test DB connection and PostGIS/pghydro extensions."""

import logging

from lakeanalysis.logger import Logger

from lakesource.postgres import atlas_db, check_extensions, series_db

log = logging.getLogger(__name__)


def test_series_tables(conn) -> None:
    """Log column names of lake_area and lake_info in SERIES_DB."""
    tables = ("lake_area", "lake_info")
    for table in tables:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} LIMIT 0")
            colnames = [d.name for d in cur.description]
        log.info("%s columns: %s", table, colnames)


def main() -> None:
    """Test ALTAS_DB (extensions) and SERIES_DB (connectivity + lake_area, lake_info)."""
    Logger("test_db")
    log.info("=== ALTAS_DB ===")
    with atlas_db.connection_context() as conn:
        check_extensions(conn)

    log.info("=== SERIES_DB ===")
    with series_db.connection_context() as conn:
        log.info("Connected. Tables:")
        test_series_tables(conn)


if __name__ == "__main__":
    main()
