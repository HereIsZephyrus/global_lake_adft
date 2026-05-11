"""Apportionment Entropy pipeline service.

Orchestrates the AE computation pipeline: chunk loading, compute, and
persistence.  Separated from ``runner.py`` which now handles only
visualisation/display concerns.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider

from .compute import compute_annual_ae, compute_overall_ae, compute_trend

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntropyRunConfig:
    data_dir: Path
    limit_id: int | None = None
    chunk_size: int = 10_000
    show_plot: bool = False


def chunk_path(data_dir: Path, chunk_start: int, chunk_end: int) -> Path:
    return data_dir / f"chunk_{chunk_start:08d}_{chunk_end:08d}.parquet"


def run_entropy(config: EntropyRunConfig) -> None:
    """Execute the AE pipeline in resumable chunks.

    This is the main production pipeline: fetch lake area data in chunks,
    compute AE and trend metrics, persist to parquet and database.
    """
    log.info(
        "Starting entropy pipeline, limit_id=%s, chunk_size=%d",
        config.limit_id,
        config.chunk_size,
    )

    provider = create_provider(SourceConfig())
    config.data_dir.mkdir(parents=True, exist_ok=True)

    provider.ensure_table("entropy")

    max_id = provider.fetch_max_hylak_id()
    if config.limit_id is not None:
        max_id = min(max_id, config.limit_id - 1)

    chunk_ranges = [
        (start, min(start + config.chunk_size, max_id + 1))
        for start in range(0, max_id + 1, config.chunk_size)
    ]

    for chunk_start, chunk_end in chunk_ranges:
        done_ids = provider.fetch_done_ids("entropy", chunk_start, chunk_end)
        lake_frames = provider.fetch_lake_area_chunk(chunk_start, chunk_end)
        amplitudes = provider.fetch_seasonal_amplitude_chunk(chunk_start, chunk_end)

        rows: list[dict[str, int | float | str | bool | None]] = []
        for hylak_id, df in lake_frames.items():
            if hylak_id in done_ids:
                continue
            ae_overall = compute_overall_ae(df)
            annual_df = compute_annual_ae(df)
            trend = compute_trend(annual_df)
            rows.append({
                "hylak_id": hylak_id,
                "ae_overall": ae_overall,
                "sens_slope": trend["sens_slope"],
                "change_per_decade_pct": trend["change_per_decade_pct"],
                "mk_trend": trend["mk_trend"],
                "mk_p": trend["mk_p"],
                "mk_z": trend["mk_z"],
                "mk_significant": trend["mk_significant"],
                "mean_seasonal_amplitude": amplitudes.get(hylak_id),
            })

        out_path = chunk_path(config.data_dir, chunk_start, chunk_end)
        pd.DataFrame(rows).to_parquet(out_path, index=False)
        log.debug("Persisted %d rows to %s", len(rows), out_path.name)
        provider.upsert_rows("entropy", rows)

    if config.show_plot:
        from .runner import show_entropy_plots
        show_entropy_plots(config.data_dir, limit_id=config.limit_id)


def run_update_amplitude_only(data_dir: Path, show_plot: bool = False) -> None:
    """Refresh mean_seasonal_amplitude for existing entropy chunks.

    Reads existing chunk parquet files, re-fetches amplitude data from
    the provider, updates parquet and database in-place.  No AE recompute.
    """
    log.info("Update amplitude only (no AE recompute)")
    data_dir.mkdir(parents=True, exist_ok=True)

    paths = sorted(data_dir.glob("chunk_*.parquet"))
    if not paths:
        log.warning("No chunk parquet files in %s", data_dir)
        if show_plot:
            from .runner import show_entropy_plots
            show_entropy_plots(data_dir, limit_id=None)
        return

    provider = create_provider(SourceConfig())
    for path in paths:
        _, start_str, end_str = path.stem.split("_", 2)
        chunk_start = int(start_str)
        chunk_end = int(end_str)

        amplitudes = provider.fetch_seasonal_amplitude_chunk(chunk_start, chunk_end)

        df = pd.read_parquet(path)
        df["mean_seasonal_amplitude"] = df["hylak_id"].map(amplitudes)
        df.to_parquet(path, index=False)
        log.debug("Updated amplitude in %s", path.name)

        provider.upsert_rows("entropy", df.to_dict("records"))

    log.info("Updated amplitude for %d chunk(s)", len(paths))
    if show_plot:
        from .runner import show_entropy_plots
        show_entropy_plots(data_dir, limit_id=None)


def load_entropy_summary(data_dir: Path, limit_id: int | None) -> pd.DataFrame:
    """Load entropy summary from persisted parquet files."""
    parts: list[pd.DataFrame] = []
    for path in sorted(data_dir.glob("chunk_*.parquet")):
        _, start_str, _ = path.stem.split("_", 2)
        chunk_start = int(start_str)
        if limit_id is not None and chunk_start >= limit_id:
            continue
        df = pd.read_parquet(path)
        if limit_id is not None:
            df = df[df["hylak_id"] < limit_id]
        parts.append(df)

    if not parts:
        log.warning("No parquet files found in %s", data_dir)
        return pd.DataFrame()

    summary = pd.concat(parts, ignore_index=True)
    if "mean_seasonal_amplitude" not in summary.columns and "annual_means_std" in summary.columns:
        summary["mean_seasonal_amplitude"] = summary["annual_means_std"]
    return summary
