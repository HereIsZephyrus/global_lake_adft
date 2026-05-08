"""Storage planning and report formatting for water-balance datasets."""

from __future__ import annotations

from pathlib import Path
import shutil

from .models import DownloadAuthConfig, SAFETY_BUFFER_BYTES, StorageReport, WATER_BALANCE_DATASETS


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
    return [
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
