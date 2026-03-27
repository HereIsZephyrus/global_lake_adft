"""Parse Hydrofetch log files for dashboard alerts and summaries."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

LOG_PATTERN = re.compile(
    r"^(?P<ts>\S+)\s+(?P<level>[A-Z]+)\s+(?P<logger>[\w\.]+):\s+(?P<message>.*)$"
)


def parse_logs(log_dir: str | Path, limit_files: int = 5) -> pd.DataFrame:
    """Parse the newest Hydrofetch logs into a DataFrame."""

    log_dir_path = Path(log_dir).expanduser().resolve()
    if not log_dir_path.exists():
        return pd.DataFrame(columns=["timestamp", "level", "logger", "message", "log_file"])

    files = sorted(log_dir_path.glob("hydrofetch_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    rows: list[dict[str, str]] = []
    for path in files[:limit_files]:
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                match = LOG_PATTERN.match(line.strip())
                if not match:
                    continue
                item = match.groupdict()
                item["log_file"] = path.name
                rows.append(item)
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["timestamp", "level", "logger", "message", "log_file"])

    logs_df = pd.DataFrame(rows)
    logs_df["timestamp"] = pd.to_datetime(logs_df["ts"], utc=True, errors="coerce")
    logs_df = logs_df.drop(columns=["ts"]).sort_values("timestamp", ascending=False).reset_index(drop=True)
    return logs_df


def build_log_alerts(logs_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return recent error and warning views for the alert page."""

    if logs_df.empty:
        empty = pd.DataFrame()
        return {"errors": empty, "warnings": empty, "writes": empty}

    errors = logs_df.loc[logs_df["level"] == "ERROR"].copy()
    warnings = logs_df.loc[logs_df["level"] == "WARNING"].copy()
    writes = logs_df.loc[logs_df["message"].str.contains("upserted", case=False, na=False)].copy()

    return {
        "errors": errors.head(100),
        "warnings": warnings.head(100),
        "writes": writes.head(50),
    }


__all__ = ["build_log_alerts", "parse_logs"]
