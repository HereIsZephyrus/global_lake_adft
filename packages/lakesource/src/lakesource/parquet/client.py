"""DuckDB file data client with table name mapping support."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


class DuckDBClient:
    """DuckDB client for querying Parquet files.

    Supports table name mapping via TableConfig: Parquet file stems
    are mapped to logical names so that SQL queries can use the same
    logical table names regardless of the backend (postgres / parquet).
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        table_config: TableConfig = _default_table_config,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir else None
        self._table_config = table_config
        self._registered_views: set[str] = set()
        self.con = duckdb.connect(database=":memory:")
        self.con.execute("SET threads=1")

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> list[dict]:
        result = self.con.execute(sql, parameters=parameters).fetchdf()
        return result.to_dict(orient="records")

    def query_df(self, sql: str, parameters: dict[str, Any] | None = None) -> pd.DataFrame:
        return self.con.execute(sql, parameters=parameters).fetchdf()

    def register_parquet(self, name: str, path: Path) -> None:
        self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM '{path}'")
        self._registered_views.add(name)
        log.debug("Registered view %s: %s", name, path)

    def register_or_replace(self, name: str, path: Path) -> None:
        self.con.execute(f"DROP VIEW IF EXISTS {name}")
        self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM '{path}'")
        self._registered_views.add(name)
        log.debug("Replaced view %s: %s", name, path)

    def register_or_replace_glob(self, name: str, glob_pattern: str) -> None:
        self.con.execute(f"DROP VIEW IF EXISTS {name}")
        self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{glob_pattern}')")
        self._registered_views.add(name)
        log.debug("Replaced view %s with glob: %s", name, glob_pattern)

    def ensure_registered(self, name: str, *, data_dir: Path | None = None) -> bool:
        """Register a logical parquet table lazily when it exists."""
        if name in self._registered_views:
            return True

        root = Path(data_dir) if data_dir is not None else self.data_dir
        if root is None:
            raise ValueError("data_dir is not set")

        file_stem = self._table_config.parquet_file(name)
        table_file = root / f"{file_stem}.parquet"
        table_dir = root / file_stem

        if table_file.exists():
            self.register_parquet(name, table_file)
            return True
        if table_dir.is_dir() and any(table_dir.glob("*.parquet")):
            self.register_or_replace_glob(name, str(table_dir / "*.parquet"))
            return True
        return False

    def register_dir(self) -> None:
        """Register all Parquet files in data_dir as DuckDB views.

        Supports two layouts:
          1. Subdirectory (chunked large table): data_dir/table_name/*.parquet
             → registered as a single view via read_parquet glob.
          2. Single file (small table): data_dir/table_name.parquet
             → registered as a view directly.

        Directory/file names are mapped to logical names via TableConfig.parquet.
        """
        if not self.data_dir:
            raise ValueError("data_dir is not set")

        reverse_map: dict[str, str] = {}
        for logical, file_stem in self._table_config.parquet.items():
            reverse_map[file_stem] = logical

        for entry in sorted(self.data_dir.iterdir()):
            if entry.is_dir():
                view_name = reverse_map.get(entry.name, entry.name)
                glob_pattern = str(entry / "*.parquet")
                try:
                    self.con.execute(
                        f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{glob_pattern}')"
                    )
                    log.info("Registered table %s: %s/*.parquet", view_name, entry.name)
                except Exception as e:
                    log.warning("Failed to register %s: %s", entry, e)
            elif entry.suffix == ".parquet":
                file_stem = entry.stem
                view_name = reverse_map.get(file_stem, file_stem)
                try:
                    self.register_parquet(view_name, entry)
                    log.info("Registered table %s: %s", view_name, entry.name)
                except Exception as e:
                    log.warning("Failed to register %s: %s", entry, e)

    def list_registered_tables(self) -> list[str]:
        result = self.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
        )
        return [row[0] for row in result.fetchall()]

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "DuckDBClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def create_client(
    data_dir: str | Path | None = None,
    table_config: TableConfig = _default_table_config,
) -> DuckDBClient:
    return DuckDBClient(data_dir=Path(data_dir) if data_dir else None, table_config=table_config)
