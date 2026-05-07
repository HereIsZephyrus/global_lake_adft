"""Application runner for interpolation detection."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from lakesource.config import Backend, SourceConfig
from lakesource.provider.factory import create_provider

from .interpolation import InterpolationConfig, detect_interpolation

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class InterpolationRunConfig:
    data_dir: Path
    chunk_size: int = 10_000
    limit_id: int | None = None
    id_start: int = 0
    id_end: int | None = None
    min_collinear_points: int = 4
    no_db: bool = False
    output_suffix: str = ""


def run_interpolation_detect(config: InterpolationRunConfig) -> pd.DataFrame:
    interpolation_config = InterpolationConfig(
        min_collinear_points=config.min_collinear_points
    )
    parquet_dir = config.data_dir / "parquet"
    if not parquet_dir.exists():
        raise FileNotFoundError(f"Parquet data directory not found: {parquet_dir}")

    provider = create_provider(SourceConfig(backend=Backend.PARQUET, data_dir=parquet_dir))

    max_id = provider.fetch_max_hylak_id()
    if config.limit_id is not None:
        max_id = min(max_id, config.limit_id)
    if config.id_end is not None:
        max_id = min(max_id, config.id_end)

    log.info(
        "Starting interpolation detection: hylak_id range [%d, %d), chunk_size=%d, min_collinear_points=%d",
        config.id_start,
        max_id,
        config.chunk_size,
        interpolation_config.min_collinear_points,
    )

    if not config.no_db:
        provider.ensure_table("interpolation_detect")

    all_rows: list[dict[str, int | float | bool | None]] = []
    db_rows: list[dict[str, int | float | None]] = []
    n_total = 0
    n_linear = 0
    n_flat_only = 0

    chunk_start = config.id_start
    while chunk_start < max_id:
        chunk_end = min(chunk_start + config.chunk_size, max_id)
        log.info("Processing chunk [%d, %d)...", chunk_start, chunk_end)

        lake_frames = provider.fetch_lake_area_chunk(chunk_start, chunk_end)
        frozen_map = provider.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

        chunk_linear = 0
        chunk_flat = 0
        for hylak_id, df in lake_frames.items():
            result = detect_interpolation(
                df,
                frozen_year_months=frozen_map.get(hylak_id),
                config=interpolation_config,
            )

            all_rows.append(
                {
                    "hylak_id": hylak_id,
                    "has_interpolation": result.has_interpolation,
                    "n_linear_segments": result.n_linear_segments,
                    "n_flat_segments": result.n_flat_segments,
                    "max_linear_len": result.max_linear_len,
                    "max_flat_len": result.max_flat_len,
                    "collinear_ratio": result.collinear_ratio,
                    "first_linear_ym": result.first_linear_ym,
                    "n_obs": result.n_obs,
                }
            )

            if result.n_linear_segments > 0:
                chunk_linear += 1
                db_rows.append(
                    {
                        "hylak_id": hylak_id,
                        "n_linear_segments": result.n_linear_segments,
                        "n_flat_segments": result.n_flat_segments,
                        "max_linear_len": result.max_linear_len,
                        "max_flat_len": result.max_flat_len,
                        "collinear_ratio": result.collinear_ratio,
                        "first_linear_ym": result.first_linear_ym,
                        "n_obs": result.n_obs,
                    }
                )
            elif result.n_flat_segments > 0:
                chunk_flat += 1

        n_total += len(lake_frames)
        n_linear += chunk_linear
        n_flat_only += chunk_flat
        log.info(
            "Chunk [%d, %d): %d lakes, %d true-linear, %d flat-only",
            chunk_start,
            chunk_end,
            len(lake_frames),
            chunk_linear,
            chunk_flat,
        )
        chunk_start = chunk_end

    result_df = pd.DataFrame(all_rows)
    if not result_df.empty:
        result_df = result_df.sort_values("hylak_id").reset_index(drop=True)

    output_dir = config.data_dir / "interpolation"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"interpolation_detect{config.output_suffix}.parquet"
    result_df.to_parquet(out_path, index=False)
    log.info("Wrote %d rows to %s", len(result_df), out_path)

    if not config.no_db and db_rows:
        provider.upsert_rows("interpolation_detect", db_rows)
        log.info("Wrote %d true-linear lakes to PostgreSQL", len(db_rows))

    if n_total > 0:
        log.info(
            "Summary: %d total, %d true-linear (%.2f%%), %d flat-only (%.2f%%)",
            n_total,
            n_linear,
            100.0 * n_linear / n_total,
            n_flat_only,
            100.0 * n_flat_only / n_total,
        )

    return result_df
