"""Table name configuration loaded from config/tables.yaml.

Provides ``TableConfig`` — a frozen dataclass that maps logical table names
to actual PostgreSQL table names and Parquet file names.  The mapping is
loaded from a YAML file (default: ``config/tables.yaml``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from .config_loader import tables_config


@dataclass(frozen=True)
class TableConfig:
    """Mapping of logical table names to actual names.

    Attributes:
        series_db: Logical → actual table name for SERIES_DB.
        atlas_db: Logical → actual table name for ALTAS_DB.
        parquet: Logical → Parquet file stem (no ``.parquet`` suffix).
        lake_geometry_id_column: Primary key column in the lake geometry table.
        lake_geometry_geom_column: Geometry column in the lake geometry table.
        lake_geometry_simplify_meters: Simplification tolerance in meters (0 = none).
    """

    _cached_default: ClassVar[TableConfig | None] = None

    series_db: dict[str, str] = field(default_factory=dict)
    atlas_db: dict[str, str] = field(default_factory=dict)
    parquet: dict[str, str] = field(default_factory=dict)
    lake_geometry_id_column: str = "hylak_id"
    lake_geometry_geom_column: str = "geom"
    lake_geometry_simplify_meters: float = 0.0

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
    def from_yaml(cls) -> TableConfig:
        """Load table configuration from ``config/tables.yaml``."""
        data = tables_config()
        atlas_section = data.get("atlas_db", {})
        atlas = dict(atlas_section)
        geometry = atlas.pop("geometry", {})
        return cls(
            series_db=dict(data.get("series_db", {})),
            atlas_db=atlas,
            parquet=dict(data.get("parquet", {})),
            lake_geometry_id_column=geometry.get("id_column", "hylak_id"),
            lake_geometry_geom_column=geometry.get("geom_column", "geom"),
            lake_geometry_simplify_meters=float(
                geometry.get("simplify_meters", 0)
            ),
        )

    @classmethod
    def default(cls) -> TableConfig:
        """Load the default ``config/tables.yaml``.

        The result is cached, so subsequent calls return the same instance.
        """
        if cls._cached_default is None:
            cls._cached_default = cls.from_yaml()
        return cls._cached_default
