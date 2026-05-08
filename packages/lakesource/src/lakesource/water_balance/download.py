"""Dataset download helpers for the water-balance warehouse."""

from __future__ import annotations

from pathlib import Path
import posixpath
import subprocess
import tempfile
from urllib.parse import urlparse

from .models import DownloadAuthConfig, GraceAuthConfig, GleamAuthConfig, WATER_BALANCE_DATASETS


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


def download_gleam_monthly_datasets(  # pylint: disable=too-many-locals
    warehouse_root: Path,
    gleam_config: GleamAuthConfig,
) -> dict[str, list[Path]]:
    """Download all GLEAM monthly files from variable subdirectories."""
    if not (gleam_config.username and gleam_config.password and gleam_config.remote_dir):
        raise ValueError("GLEAM credentials are incomplete (need username, password, remote_dir)")

    try:
        import paramiko  # pylint: disable=import-outside-toplevel
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
        for variable_name in sorted(sftp.listdir(monthly_root)):
            var_remote_dir = f"{monthly_root}/{variable_name}"
            try:
                files = sorted(sftp.listdir(var_remote_dir))
            except OSError:
                continue
            nc_files = [name for name in files if name.endswith(".nc")]
            if not nc_files:
                continue

            var_local_dir = local_root / variable_name
            var_local_dir.mkdir(parents=True, exist_ok=True)
            downloaded_paths[variable_name] = []

            for remote_file in nc_files:
                local_path = var_local_dir / remote_file
                if local_path.exists():
                    continue
                remote_full_path = posixpath.join(var_remote_dir, remote_file)
                sftp.get(remote_full_path, str(local_path))
                downloaded_paths[variable_name].append(local_path)
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
