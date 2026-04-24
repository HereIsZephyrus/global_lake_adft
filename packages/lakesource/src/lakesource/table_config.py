"""Table name configuration loaded from config.toml.

Provides ``TableConfig`` — a frozen dataclass that maps logical table names
to actual PostgreSQL table names and Parquet file names.  The mapping is
loaded from a TOML file (default: ``config.toml`` bundled with the package).
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent.parent / "lakesource" / "config.toml"


@dataclass(frozen=True)
class TableConfig:
    """Mapping of logical table names to actual names.

    Attributes:
        series_db: Logical → actual table name for SERIES_DB.
        atlas_db: Logical → actual table name for ALTAS_DB.
        parquet: Logical → Parquet file stem (no ``.parquet`` suffix).
    """

    series_db: dict[str, str] = field(default_factory=dict)
    atlas_db: dict[str, str] = field(default_factory=dict)
    parquet: dict[str, str] = field(default_factory=dict)

    def series_table(self, logical: str) -> str:
        """Resolve a logical name to the SERIES_DB actual table name."""
        return self.series_db.get(logical, logical)

    def atlas_table(self, logical: str) -> str:
        """Resolve a logical name to the ALTAS_DB actual table name."""
        return self.atlas_db.get(logical, logical)

    def parquet_file(self, logical: str) -> str:
        """Resolve a logical name to the Parquet file stem."""
        return self.parquet.get(logical, logical)

    @classmethod
    def from_toml(cls, path: Path) -> TableConfig:
        """Load table configuration from a TOML file.

        The file must contain a ``[tables]`` section with optional
        ``series_db``, ``atlas_db``, and ``parquet`` sub-sections.

        Args:
            path: Path to the TOML file.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file is missing the ``[tables]`` section.
        """
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "rb") as f:
            data = tomllib.load(f)
        tables = data.get("tables")
        if tables is None:
            raise ValueError(f"Missing [tables] section in {path}")
        return cls(
            series_db=dict(tables.get("series_db", {})),
            atlas_db=dict(tables.get("atlas_db", {})),
            parquet=dict(tables.get("parquet", {})),
        )

    @classmethod
    def default(cls) -> TableConfig:
        """Load the default ``config.toml`` bundled with the package."""
        return cls.from_toml(_DEFAULT_CONFIG_PATH)
