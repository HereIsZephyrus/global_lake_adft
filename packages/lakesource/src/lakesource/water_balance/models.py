"""Water-balance planning models and static dataset metadata."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DownloadFile:
    """One downloadable file and its expected size."""

    url: str
    relative_path: Path
    expected_bytes: int


@dataclass(frozen=True)
class DatasetPlan:
    """Plan metadata for one dataset family."""

    name: str
    target_dirname: str
    auth_required: bool
    description: str
    estimated_bytes: int
    files: tuple[DownloadFile, ...] = ()

    @property
    def target_subdir(self) -> Path:
        """Relative target directory under the warehouse root."""
        return Path(self.target_dirname)


@dataclass(frozen=True)
class StorageReport:
    """Warehouse capacity summary."""

    warehouse_root: Path
    total_bytes: int
    used_bytes: int
    free_bytes: int
    required_public_bytes: int
    recommended_total_bytes: int

    @property
    def enough_for_public_downloads(self) -> bool:
        """Whether the warehouse can hold the public download set."""
        return self.free_bytes >= self.required_public_bytes

    @property
    def enough_for_full_plan(self) -> bool:
        """Whether the warehouse can hold all planned datasets."""
        return self.free_bytes >= self.recommended_total_bytes


@dataclass(frozen=True)
class GraceAuthConfig:
    """Credentials and settings for authenticated GRACE downloads."""

    username: str | None
    password: str | None
    url: str | None

    @property
    def ready(self) -> bool:
        """Whether the GRACE download can be attempted."""
        return bool(self.username and self.password and self.url)

    @property
    def missing_fields(self) -> tuple[str, ...]:
        """List missing environment fields required for download."""
        missing = []
        if not self.username:
            missing.append("EARTHDATA_USERNAME")
        if not self.password:
            missing.append("EARTHDATA_PASSWORD")
        if not self.url:
            missing.append("GRACE_URL")
        return tuple(missing)


@dataclass(frozen=True)
class GleamAuthConfig:
    """Credentials and settings for authenticated GLEAM downloads."""

    host: str
    port: int
    username: str | None
    password: str | None
    remote_dir: str | None
    remote_files: tuple[str, ...]

    @property
    def ready(self) -> bool:
        """Whether the GLEAM download can be attempted."""
        return bool(
            self.username and self.password and self.remote_dir and self.remote_files
        )

    @property
    def missing_fields(self) -> tuple[str, ...]:
        """List missing environment fields required for download."""
        missing = []
        if not self.username:
            missing.append("GLEAM_SFTP_USERNAME")
        if not self.password:
            missing.append("GLEAM_SFTP_PASSWORD")
        if not self.remote_dir:
            missing.append("GLEAM_REMOTE_DIR")
        if not self.remote_files:
            missing.append("GLEAM_REMOTE_FILES")
        return tuple(missing)


@dataclass(frozen=True)
class DownloadAuthConfig:
    """All optional authenticated download settings."""

    grace: GraceAuthConfig
    gleam: GleamAuthConfig


GPCC_PUBLIC_FILES = (
    DownloadFile(
        url=(
            "https://opendata.dwd.de/climate_environment/GPCC/"
            "full_data_monthly_v2022/025/full_data_monthly_v2022_2001_2010_025.nc.gz"
        ),
        relative_path=Path("GPCC/full_data_monthly_v2022/025/")
        / "full_data_monthly_v2022_2001_2010_025.nc.gz",
        expected_bytes=285_951_583,
    ),
    DownloadFile(
        url=(
            "https://opendata.dwd.de/climate_environment/GPCC/"
            "full_data_monthly_v2022/025/full_data_monthly_v2022_2011_2020_025.nc.gz"
        ),
        relative_path=Path("GPCC/full_data_monthly_v2022/025/")
        / "full_data_monthly_v2022_2011_2020_025.nc.gz",
        expected_bytes=294_817_487,
    ),
)

WATER_BALANCE_DATASETS = (
    DatasetPlan(
        name="GPCC Full Data Monthly v2022 (0.25° overlap archive)",
        target_dirname="GPCC",
        auth_required=False,
        description="Monthly precipitation for 2001-2020 from DWD open data.",
        estimated_bytes=sum(file.expected_bytes for file in GPCC_PUBLIC_FILES),
        files=GPCC_PUBLIC_FILES,
    ),
    DatasetPlan(
        name="GRACE/GRACE-FO JPL Mascon RL06.3v04",
        target_dirname="GRACE",
        auth_required=True,
        description=(
            "Single monthly mascon NetCDF from NASA PO.DAAC; Earthdata login required."
        ),
        estimated_bytes=44_962_349,
    ),
    DatasetPlan(
        name="GLEAM monthly evaporation archive",
        target_dirname="GLEAM",
        auth_required=True,
        description=(
            "Prefer monthly E archive; SFTP credentials from gleam.eu required."
        ),
        estimated_bytes=2_200_000_000,
    ),
)

SAFETY_BUFFER_BYTES = 10 * 1024 * 1024 * 1024
DEFAULT_GRACE_URL = (
    "https://archive.podaac.earthdata.nasa.gov/"
    "podaac-ops-cumulus-protected/TELLUS_GRAC-GRFO_MASCON_CRI_GRID_RL06.3_V4/"
    "GRCTellus.JPL.200204_202601.GLO.RL06.3M.MSCNv04CRI.nc"
)
