"""Parse Hydrofetch log files."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

_LOG_PATTERN = re.compile(
    r"^(?P<ts>\S+)\s+(?P<level>[A-Z]+)\s+(?P<logger>[\w\.]+):\s+(?P<message>.*)$"
)
_EMPTY_COLS = ["timestamp", "level", "logger", "message", "log_file"]


def parse_logs(log_dir: str | Path, limit_files: int = 5) -> pd.DataFrame:
    """Parse the newest Hydrofetch logs into a DataFrame."""

    log_dir_path = Path(log_dir).expanduser().resolve()
    if not log_dir_path.exists():
        return pd.DataFrame(columns=_EMPTY_COLS)

    files = sorted(
        log_dir_path.glob("hydrofetch_*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    rows: list[dict[str, str]] = []
    for path in files[:limit_files]:
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                m = _LOG_PATTERN.match(line.strip())
                if not m:
                    continue
                item = m.groupdict()
                item["log_file"] = path.name
                rows.append(item)
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=_EMPTY_COLS)

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.drop(columns=["ts"]).sort_values("timestamp", ascending=False).reset_index(drop=True)
    return df


def log_error_summary(log_dir: str | Path, limit_files: int = 5) -> dict[str, list[dict]]:
    """Return recent errors and writes from logs."""

    df = parse_logs(log_dir, limit_files=limit_files)
    if df.empty:
        return {"errors": [], "warnings": [], "writes": []}

    def _rows(mask: pd.Series, cols: list[str]) -> list[dict]:
        sub = df.loc[mask, cols].copy()
        sub["timestamp"] = sub["timestamp"].astype(str)
        return sub.head(100).to_dict("records")

    err_cols = ["timestamp", "level", "logger", "message", "log_file"]
    return {
        "errors": _rows(df["level"] == "ERROR", err_cols),
        "warnings": _rows(df["level"] == "WARNING", err_cols),
        "writes": _rows(df["message"].str.contains("upserted", case=False, na=False), err_cols),
    }


__all__ = ["log_error_summary", "parse_logs"]
