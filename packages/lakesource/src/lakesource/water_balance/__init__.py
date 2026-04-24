"""Water-balance raw-data planning and download helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import posixpath
import shutil
import subprocess
import tempfile
from urllib.parse import urlparse


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


def format_bytes(num_bytes: int) -> str:
    """Format bytes using binary units."""
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def build_storage_report(warehouse_root: Path) -> StorageReport:
    """Return storage capacity information for the warehouse root."""
    usage = shutil.disk_usage(warehouse_root)
    required_public_bytes = remaining_public_download_bytes(warehouse_root)
    recommended_total_bytes = (
        sum(dataset.estimated_bytes for dataset in WATER_BALANCE_DATASETS)
        + SAFETY_BUFFER_BYTES
    )
    return StorageReport(
        warehouse_root=warehouse_root,
        total_bytes=usage.total,
        used_bytes=usage.used,
        free_bytes=usage.free,
        required_public_bytes=required_public_bytes,
        recommended_total_bytes=recommended_total_bytes,
    )


def load_auth_config(env: Mapping[str, str] | None = None) -> DownloadAuthConfig:
    """Load optional authenticated download settings from environment variables."""
    source = os.environ if env is None else env
    grace = GraceAuthConfig(
        username=_get_optional_str(source, "EARTHDATA_USERNAME"),
        password=_get_optional_str(source, "EARTHDATA_PASSWORD"),
        url=_get_optional_str(source, "GRACE_URL") or DEFAULT_GRACE_URL,
    )
    gleam = GleamAuthConfig(
        host=_get_optional_str(source, "GLEAM_SFTP_HOST") or "sftp.gleam.eu",
        port=_get_optional_int(source, "GLEAM_SFTP_PORT", 2225),
        username=_get_optional_str(source, "GLEAM_SFTP_USERNAME"),
        password=_get_optional_str(source, "GLEAM_SFTP_PASSWORD"),
        remote_dir=_get_optional_str(source, "GLEAM_REMOTE_DIR"),
        remote_files=_get_csv_values(source, "GLEAM_REMOTE_FILES"),
    )
    return DownloadAuthConfig(grace=grace, gleam=gleam)


def ensure_dataset_directories(warehouse_root: Path) -> None:
    """Create the warehouse subdirectories used by this feature."""
    for dataset in WATER_BALANCE_DATASETS:
        (warehouse_root / dataset.target_subdir).mkdir(parents=True, exist_ok=True)


def remaining_public_download_bytes(warehouse_root: Path) -> int:
    """Compute the remaining size of unauthenticated files not yet present."""
    total = 0
    for dataset in WATER_BALANCE_DATASETS:
        if dataset.auth_required:
            continue
        for download_file in dataset.files:
            if not (warehouse_root / download_file.relative_path).exists():
                total += download_file.expected_bytes
    return total


def download_public_datasets(warehouse_root: Path) -> list[Path]:
    """Download all public files that are still missing."""
    downloaded_paths: list[Path] = []
    for dataset in WATER_BALANCE_DATASETS:
        if dataset.auth_required:
            continue
        for download_file in dataset.files:
            target_path = warehouse_root / download_file.relative_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if target_path.exists():
                continue
            subprocess.run(
                ["wget", "-nc", "-O", str(target_path), download_file.url],
                check=True,
            )
            downloaded_paths.append(target_path)
    return downloaded_paths


def download_authenticated_datasets(
    warehouse_root: Path,
    auth_config: DownloadAuthConfig,
) -> dict[str, list[Path]]:
    """Download all authenticated datasets with complete credentials."""
    downloaded_paths: dict[str, list[Path]] = {}
    if auth_config.grace.ready:
        downloaded_paths["GRACE"] = [
            download_grace_dataset(warehouse_root, auth_config.grace)
        ]
    if auth_config.gleam.ready:
        downloaded_paths["GLEAM"] = download_gleam_datasets(
            warehouse_root,
            auth_config.gleam,
        )
    return downloaded_paths


def download_grace_dataset(
    warehouse_root: Path,
    grace_config: GraceAuthConfig,
) -> Path:
    """Download one authenticated GRACE NetCDF via Earthdata login."""
    if not grace_config.ready:
        raise ValueError("GRACE credentials are incomplete")
    grace_dir = warehouse_root / "GRACE"
    grace_dir.mkdir(parents=True, exist_ok=True)
    target_path = grace_dir / Path(urlparse(grace_config.url or "").path).name
    if target_path.exists():
        return target_path

    with tempfile.TemporaryDirectory() as temp_dir:
        netrc_path = Path(temp_dir) / ".netrc"
        cookie_path = Path(temp_dir) / "earthdata.cookies"
        netrc_path.write_text(
            "machine urs.earthdata.nasa.gov "
            f"login {grace_config.username} password {grace_config.password}\n",
            encoding="utf-8",
        )
        netrc_path.chmod(0o600)
        subprocess.run(
            [
                "curl",
                "-fL",
                "--netrc-file",
                str(netrc_path),
                "-c",
                str(cookie_path),
                "-b",
                str(cookie_path),
                "-o",
                str(target_path),
                grace_config.url or "",
            ],
            check=True,
        )
    return target_path


def download_gleam_datasets(
    warehouse_root: Path,
    gleam_config: GleamAuthConfig,
) -> list[Path]:
    """Download authenticated GLEAM files from the configured SFTP path."""
    if not gleam_config.ready:
        raise ValueError("GLEAM credentials are incomplete")
    gleam_dir = warehouse_root / "GLEAM"
    gleam_dir.mkdir(parents=True, exist_ok=True)
    downloaded_paths: list[Path] = []
    remote_dir = (gleam_config.remote_dir or "").strip("/")
    for remote_file in gleam_config.remote_files:
        target_path = gleam_dir / remote_file
        if target_path.exists():
            continue
        remote_url = (
            f"sftp://{gleam_config.host}:{gleam_config.port}/{remote_dir}/{remote_file}"
        )
        subprocess.run(
            [
                "curl",
                "-k",
                "-f",
                "-u",
                f"{gleam_config.username}:{gleam_config.password}",
                "-o",
                str(target_path),
                remote_url,
            ],
            check=True,
        )
        downloaded_paths.append(target_path)
    return downloaded_paths


def dataset_plan_lines(warehouse_root: Path) -> list[str]:
    """Build human-readable plan lines for CLI output."""
    lines: list[str] = []
    for dataset in WATER_BALANCE_DATASETS:
        status = "auth" if dataset.auth_required else "public"
        lines.append(
            f"- {dataset.name}: {status}, target={warehouse_root / dataset.target_subdir}, "
            f"estimate={format_bytes(dataset.estimated_bytes)}"
        )
    return lines


def auth_config_lines(auth_config: DownloadAuthConfig) -> list[str]:
    """Render authentication readiness for terminal output."""
    lines = [
        "Auth config",
        (
            f"- GRACE ready={auth_config.grace.ready}; "
            f"missing={', '.join(auth_config.grace.missing_fields) or 'none'}"
        ),
        (
            f"- GLEAM ready={auth_config.gleam.ready}; "
            f"missing={', '.join(auth_config.gleam.missing_fields) or 'none'}"
        ),
    ]
    return lines


def storage_report_lines(report: StorageReport) -> list[str]:
    """Render a storage report for terminal output."""
    return [
        f"- warehouse={report.warehouse_root}",
        f"- total={format_bytes(report.total_bytes)}",
        f"- used={format_bytes(report.used_bytes)}",
        f"- free={format_bytes(report.free_bytes)}",
        f"- remaining_public_downloads={format_bytes(report.required_public_bytes)}",
        f"- recommended_full_plan={format_bytes(report.recommended_total_bytes)}",
        f"- enough_for_public_downloads={report.enough_for_public_downloads}",
        f"- enough_for_full_plan={report.enough_for_full_plan}",
    ]


def download_gleam_monthly_datasets(
    warehouse_root: Path,
    gleam_config: GleamAuthConfig,
) -> dict[str, list[Path]]:
    """Download all GLEAM monthly files from variable subdirectories.

    Expects ``remote_dir`` to point to the parent monthly directory, e.g.
    ``/data/v4.2a/monthly``.  Each variable subdirectory (E, SMs, etc.) is
    listed via SFTP and all NetCDF files within are downloaded, preserving the
    variable subdirectory in the local warehouse.
    """
    if not (gleam_config.username and gleam_config.password and gleam_config.remote_dir):
        raise ValueError("GLEAM credentials are incomplete (need username, password, remote_dir)")

    try:
        import paramiko
    except ImportError as exc:
        raise ImportError(
            "paramiko is required for GLEAM monthly downloads. "
            "Install it with: uv add paramiko"
        ) from exc

    transport = paramiko.Transport((gleam_config.host, gleam_config.port))
    transport.connect(
        username=gleam_config.username,
        password=gleam_config.password,
    )
    sftp = paramiko.SFTPClient.from_transport(transport)

    monthly_root = (gleam_config.remote_dir or "").rstrip("/")
    local_root = warehouse_root / "GLEAM"
    downloaded_paths: dict[str, list[Path]] = {}

    try:
        for var in sorted(sftp.listdir(monthly_root)):
            var_remote_dir = f"{monthly_root}/{var}"
            try:
                files = sorted(sftp.listdir(var_remote_dir))
            except OSError:
                continue
            nc_files = [f for f in files if f.endswith(".nc")]
            if not nc_files:
                continue

            var_local_dir = local_root / var
            var_local_dir.mkdir(parents=True, exist_ok=True)
            downloaded_paths[var] = []

            for remote_file in nc_files:
                local_path = var_local_dir / remote_file
                if local_path.exists():
                    continue
                remote_full_path = posixpath.join(var_remote_dir, remote_file)
                sftp.get(remote_full_path, str(local_path))
                downloaded_paths[var].append(local_path)
                attrs = sftp.stat(remote_full_path)
                size_mb = attrs.st_size / 1024 / 1024
                print(
                    f"  [GLEAM monthly] {remote_file} ({size_mb:.1f} MB) "
                    f"→ {local_path}"
                )
    finally:
        sftp.close()
        transport.close()

    return downloaded_paths


def _get_optional_str(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key)
    if value is None:
        return None
    stripped = value.strip().strip('"')
    return stripped or None


def _get_optional_int(env: Mapping[str, str], key: str, default: int) -> int:
    value = _get_optional_str(env, key)
    if value is None:
        return default
    return int(value)


def _get_csv_values(env: Mapping[str, str], key: str) -> tuple[str, ...]:
    value = _get_optional_str(env, key)
    if value is None:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())
