"""Summary-cache builders and plotters for monthly transition batch outputs."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

from lakeanalysis.plot_config import setup_chinese_font

from lakesource.monthly_transition.schema import MonthlyTransitionResult

from .plot import (
    plot_transition_count_summary_from_cache,
    plot_transition_seasonality_summary_from_cache,
)


@dataclass
class SummaryAccumulator:
    """Accumulate summary statistics during batch execution."""

    transition_counts: Counter[str] = field(default_factory=Counter)
    transition_seasonality: Counter[int] = field(default_factory=Counter)
    lake_transition_counts: dict[int, int] = field(default_factory=dict)
    lake_extreme_counts: dict[int, int] = field(default_factory=dict)
    done_count: int = 0
    error_count: int = 0

    def update_success(self, result: MonthlyTransitionResult) -> None:
        """Update counters from one successful lake result."""
        if result.hylak_id is None:
            raise ValueError("SummaryAccumulator requires integer hylak_id values")
        hylak_id = int(result.hylak_id)
        self.transition_counts.update(result.transitions_df["transition_type"].tolist())
        if not result.transitions_df.empty:
            self.transition_seasonality.update(
                result.transitions_df["to_month"].astype(int).tolist()
            )
        self.lake_transition_counts[hylak_id] = int(len(result.transitions_df))
        self.lake_extreme_counts[hylak_id] = int(len(result.extremes_df))
        self.done_count += 1

    def update_error(self) -> None:
        """Record one failed lake."""
        self.error_count += 1

    def merge(self, other: "SummaryAccumulator") -> None:
        """Merge another accumulator into this one."""
        self.transition_counts.update(other.transition_counts)
        self.transition_seasonality.update(other.transition_seasonality)
        self.lake_transition_counts.update(other.lake_transition_counts)
        self.lake_extreme_counts.update(other.lake_extreme_counts)
        self.done_count += other.done_count
        self.error_count += other.error_count

    def to_cache_payload(self) -> dict[str, pd.DataFrame | dict[str, Any]]:
        """Convert the accumulator to cache payload tables."""
        return {
            "transition_counts": pd.DataFrame(
                [
                    {"transition_type": key, "count": int(value)}
                    for key, value in sorted(self.transition_counts.items())
                ]
            ),
            "transition_seasonality": pd.DataFrame(
                [
                    {"to_month": key, "count": int(value)}
                    for key, value in sorted(self.transition_seasonality.items())
                ]
            ),
            "lake_transition_counts": pd.DataFrame(
                [
                    {"hylak_id": key, "transition_count": int(value)}
                    for key, value in sorted(self.lake_transition_counts.items())
                ]
            ),
            "lake_extreme_counts": pd.DataFrame(
                [
                    {"hylak_id": key, "extreme_count": int(value)}
                    for key, value in sorted(self.lake_extreme_counts.items())
                ]
            ),
            "run_metadata": {
                "done_count": int(self.done_count),
                "error_count": int(self.error_count),
            },
        }

    @classmethod
    def load(cls, output_root: Path) -> "SummaryAccumulator":
        """Load an existing cache from output_root if present."""
        cache_root = cache_root_for(output_root)
        accumulator = cls()
        if not cache_root.exists():
            return accumulator
        cache = load_summary_cache(cache_root)
        accumulator.transition_counts.update(
            {
                str(row["transition_type"]): int(row["count"])
                for _, row in cache["transition_counts"].iterrows()
            }
        )
        accumulator.transition_seasonality.update(
            {
                int(row["to_month"]): int(row["count"])
                for _, row in cache["transition_seasonality"].iterrows()
                if int(row["count"]) > 0
            }
        )
        accumulator.lake_transition_counts = {
            int(row["hylak_id"]): int(row["transition_count"])
            for _, row in cache["lake_transition_counts"].iterrows()
        }
        accumulator.lake_extreme_counts = {
            int(row["hylak_id"]): int(row["extreme_count"])
            for _, row in cache["lake_extreme_counts"].iterrows()
        }
        accumulator.done_count = int(cache["run_metadata"].get("done_count", 0))
        accumulator.error_count = int(cache["run_metadata"].get("error_count", 0))
        return accumulator


def cache_root_for(output_root: Path) -> Path:
    """Return the summary-cache directory for a batch output root."""
    return output_root / "summary_cache"


def _normalize_transition_counts(df: pd.DataFrame) -> pd.DataFrame:
    base = pd.DataFrame({"transition_type": ["low_to_high", "high_to_low"]})
    if df.empty:
        base["count"] = 0
        return base
    merged = base.merge(df, on="transition_type", how="left")
    merged["count"] = merged["count"].fillna(0).astype(int)
    return merged


def _normalize_transition_seasonality(df: pd.DataFrame) -> pd.DataFrame:
    base = pd.DataFrame({"to_month": list(range(1, 13))})
    if df.empty:
        base["count"] = 0
        return base
    normalized = df.rename(columns={"month": "to_month"})
    merged = base.merge(normalized, on="to_month", how="left")
    merged["count"] = merged["count"].fillna(0).astype(int)
    return merged


def write_summary_cache(
    cache_root: Path,
    *,
    transition_counts: pd.DataFrame,
    transition_seasonality: pd.DataFrame,
    lake_transition_counts: pd.DataFrame,
    lake_extreme_counts: pd.DataFrame,
    run_metadata: dict[str, Any] | None = None,
    run_status: pd.DataFrame | None = None,
) -> dict[str, Path]:
    """Write local summary cache files."""
    if run_metadata is not None and run_status is not None:
        raise ValueError("Use either run_metadata or run_status when writing summary cache")
    if run_metadata is None and run_status is None:
        raise ValueError("run_metadata or run_status is required when writing summary cache")

    cache_root.mkdir(parents=True, exist_ok=True)

    transition_counts_path = cache_root / "transition_counts.csv"
    transition_seasonality_path = cache_root / "transition_seasonality.csv"
    lake_transition_counts_path = cache_root / "lake_transition_counts.csv"
    lake_extreme_counts_path = cache_root / "lake_extreme_counts.csv"
    run_metadata_path = cache_root / "run_metadata.json"

    _normalize_transition_counts(transition_counts).to_csv(transition_counts_path, index=False)
    _normalize_transition_seasonality(transition_seasonality).to_csv(
        transition_seasonality_path,
        index=False,
    )
    lake_transition_counts.to_csv(lake_transition_counts_path, index=False)
    lake_extreme_counts.to_csv(lake_extreme_counts_path, index=False)

    normalized_run_metadata = dict(run_metadata or {})
    if run_status is not None:
        status_map = {
            str(row["status"]): int(row["count"])
            for _, row in run_status.iterrows()
        }
        normalized_run_metadata.update(
            {
                "done_count": status_map.get("done", 0),
                "error_count": status_map.get("error", 0),
            }
        )

    metadata_payload = {
        **normalized_run_metadata,
        "cache_generated_at": datetime.now(timezone.utc).isoformat(),
    }
    run_metadata_path.write_text(
        json.dumps(metadata_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "transition_counts": transition_counts_path,
        "transition_seasonality": transition_seasonality_path,
        "lake_transition_counts": lake_transition_counts_path,
        "lake_extreme_counts": lake_extreme_counts_path,
        "run_metadata": run_metadata_path,
    }


def load_summary_cache(cache_root: Path) -> dict[str, Any]:
    """Load local summary cache files."""
    transition_seasonality = _normalize_transition_seasonality(
        pd.read_csv(cache_root / "transition_seasonality.csv")
    )
    return {
        "transition_counts": pd.read_csv(cache_root / "transition_counts.csv"),
        "transition_seasonality": transition_seasonality,
        "lake_transition_counts": pd.read_csv(cache_root / "lake_transition_counts.csv"),
        "lake_extreme_counts": pd.read_csv(cache_root / "lake_extreme_counts.csv"),
        "run_metadata": json.loads((cache_root / "run_metadata.json").read_text(encoding="utf-8")),
    }


def save_summary_plots_from_cache(cache_root: Path, output_root: Path) -> dict[str, Path]:
    """Generate global summary figures from cached summary files."""
    cache = load_summary_cache(cache_root)
    counts_df = _normalize_transition_counts(cache["transition_counts"])
    seasonality_df = cache["transition_seasonality"]

    output_root.mkdir(parents=True, exist_ok=True)
    count_path = output_root / "transition_count_summary.png"
    seasonality_path = output_root / "transition_seasonality.png"

    setup_chinese_font()

    fig = plot_transition_count_summary_from_cache(counts_df)
    fig.savefig(count_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_transition_seasonality_summary_from_cache(seasonality_df)
    fig.savefig(seasonality_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    return {
        "transition_count_summary": count_path,
        "transition_seasonality": seasonality_path,
    }
