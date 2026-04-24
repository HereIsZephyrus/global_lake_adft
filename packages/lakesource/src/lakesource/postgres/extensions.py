"""Check PostGIS and pghydro extensions on a database connection."""

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

log = logging.getLogger(__name__)

EXTENSIONS_SQL = """
SELECT extname, extversion
FROM pg_extension
ORDER BY extname;
"""

POSTGIS_VERSION_SQL = "SELECT PostGIS_Full_Version();"

PGHYDRO_TABLES_SQL = """
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema IN ('pghydro', 'pgh_raster', 'pgh_hgm', 'pgh_consistency', 'pgh_output')
ORDER BY table_schema, table_name;
"""


@dataclass
class ExtensionInfo:
    """Installed extension name and version."""

    name: str
    version: str


@dataclass
class TableInfo:
    """Table or view in a pghydro-related schema."""

    schema: str
    name: str
    kind: str


def list_installed_extensions(conn: psycopg.Connection) -> list[ExtensionInfo]:
    """Query pg_extension and return installed extensions with versions.

    Args:
        conn: An open psycopg connection.

    Returns:
        List of ExtensionInfo (extname, extversion).
    """
    with conn.cursor() as cur:
        cur.execute(EXTENSIONS_SQL)
        rows: list[tuple[Any, ...]] = cur.fetchall()
    return [ExtensionInfo(name=str(r[0]), version=str(r[1])) for r in rows]


def get_postgis_version(conn: psycopg.Connection) -> str | None:
    """Return PostGIS full version string if PostGIS is available, else None.

    Args:
        conn: An open psycopg connection.

    Returns:
        PostGIS full version string or None if not installed / error.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(POSTGIS_VERSION_SQL)
            row = cur.fetchone()
        return str(row[0]) if row else None
    except psycopg.Error:
        return None


def list_pghydro_objects(conn: psycopg.Connection) -> list[TableInfo]:
    """List tables/views in pghydro-related schemas if they exist.

    Args:
        conn: An open psycopg connection.

    Returns:
        List of TableInfo for tables/views in pghydro, pgh_raster, pgh_hgm, etc.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(PGHYDRO_TABLES_SQL)
            rows: list[tuple[Any, ...]] = cur.fetchall()
        return [
            TableInfo(schema=str(r[0]), name=str(r[1]), kind=str(r[2]))
            for r in rows
        ]
    except psycopg.Error:
        return []


def check_extensions(conn: psycopg.Connection) -> list[ExtensionInfo]:
    """Print extension summary and return installed extensions.

    Lists pg_extension rows, prints PostGIS version if available, and lists
    pghydro-related schema objects. Returns the same list as
    list_installed_extensions(conn).

    Args:
        conn: An open psycopg connection.

    Returns:
        List of ExtensionInfo for all installed extensions.
    """
    extensions = list_installed_extensions(conn)
    log.info("Installed extensions: %s", [f"{e.name} {e.version}" for e in extensions])

    postgis_ver = get_postgis_version(conn)
    if postgis_ver:
        log.info("PostGIS: %s", postgis_ver)

    objects = list_pghydro_objects(conn)
    if objects:
        log.info(
            "PgHydro-related objects (tables/views): %d",
            len(objects),
        )
        for obj in objects:
            log.debug("  %s.%s (%s)", obj.schema, obj.name, obj.kind)
    else:
        log.warning("No pghydro-related schemas/tables found (or extension not fully loaded).")

    return extensions
