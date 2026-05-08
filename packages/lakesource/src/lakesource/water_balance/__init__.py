"""Water-balance raw-data planning and download helpers."""

from .auth import load_auth_config
from .download import (
    download_authenticated_datasets,
    download_gleam_datasets,
    download_gleam_monthly_datasets,
    download_grace_dataset,
    download_public_datasets,
)
from .models import (
    DEFAULT_GRACE_URL,
    GPCC_PUBLIC_FILES,
    SAFETY_BUFFER_BYTES,
    WATER_BALANCE_DATASETS,
    DatasetPlan,
    DownloadAuthConfig,
    DownloadFile,
    GraceAuthConfig,
    GleamAuthConfig,
    StorageReport,
)
from .reports import (
    auth_config_lines,
    build_storage_report,
    dataset_plan_lines,
    ensure_dataset_directories,
    format_bytes,
    remaining_public_download_bytes,
    storage_report_lines,
)

__all__ = [
    "DEFAULT_GRACE_URL",
    "GPCC_PUBLIC_FILES",
    "SAFETY_BUFFER_BYTES",
    "WATER_BALANCE_DATASETS",
    "DatasetPlan",
    "DownloadAuthConfig",
    "DownloadFile",
    "GraceAuthConfig",
    "GleamAuthConfig",
    "StorageReport",
    "auth_config_lines",
    "build_storage_report",
    "dataset_plan_lines",
    "download_authenticated_datasets",
    "download_gleam_datasets",
    "download_gleam_monthly_datasets",
    "download_grace_dataset",
    "download_public_datasets",
    "ensure_dataset_directories",
    "format_bytes",
    "load_auth_config",
    "remaining_public_download_bytes",
    "storage_report_lines",
]
