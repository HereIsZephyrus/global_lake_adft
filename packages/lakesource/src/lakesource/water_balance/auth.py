"""Environment-driven auth configuration for water-balance downloads."""

from __future__ import annotations

from collections.abc import Mapping
import os

from .models import DEFAULT_GRACE_URL, DownloadAuthConfig, GraceAuthConfig, GleamAuthConfig


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
