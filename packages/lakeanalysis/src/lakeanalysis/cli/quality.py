"""CLI commands for data quality assessment & anomaly detection."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, DATA_DIR, DryRunOpt, LimitIdOpt, setup_logging

app = typer.Typer(help="Data quality assessment & anomaly detection", no_args_is_help=True)


@app.command()
def run(
    limit_id: LimitIdOpt = None,
    chunk_size: ChunkSizeOpt = 10_000,
    zero_quantile: float = typer.Option(0.80, help="Quantile at which zero area is flagged"),
    flat_ratio: float = typer.Option(0.8, "--flat-ratio", help="Flatness dominant ratio threshold"),
    area_ratio_min: float = typer.Option(0.1, "--area-ratio-min", help="Minimum acceptable rs/atlas ratio"),
    area_ratio_max: float = typer.Option(10.0, "--area-ratio-max", help="Maximum acceptable rs/atlas ratio"),
    pv_threshold: float = typer.Option(0.001, "--pv-threshold", help="Penalized volatility anomaly threshold"),
    range_tolerance: float = typer.Option(0.5, "--range-tolerance", help="Outside-range tolerance"),
    reset: bool = typer.Option(False, "--reset", help="Truncate tables before running"),
) -> None:
    """Run the full quality pipeline (all anomaly filters)."""
    setup_logging("quality")
    from lakeanalysis.quality.runner import run_quality
    from lakeanalysis.quality.batch import QualityRunConfig
    from lakeanalysis.quality.filters.flatness import FlatnessFilterConfig
    from lakeanalysis.quality.filters.area_ratio import AreaRatioConfig
    from lakeanalysis.quality.filters.penalized_volatility import PenalizedVolatilityConfig
    from lakeanalysis.quality.filters.outside_range import OutsideRangeConfig
    from lakeanalysis.quality.filters.shift import ShiftConfig

    config = QualityRunConfig(
        limit_id=limit_id,
        chunk_size=chunk_size,
        zero_quantile=zero_quantile,
        flat_config=FlatnessFilterConfig(dominant_ratio_threshold=flat_ratio),
        ratio_config=AreaRatioConfig(min_ratio=area_ratio_min, max_ratio=area_ratio_max),
        pv_config=PenalizedVolatilityConfig(pv_threshold=pv_threshold),
        outside_range_config=OutsideRangeConfig(tolerance=range_tolerance),
        shift_config=ShiftConfig(),
        reset=reset,
    )
    run_quality(config)


@app.command()
def interpolation(
    chunk_size: ChunkSizeOpt = 10_000,
    limit_id: LimitIdOpt = None,
    id_start: int = typer.Option(0, "--id-start"),
    id_end: int | None = typer.Option(None, "--id-end"),
    min_collinear: int = typer.Option(4, "--min-collinear", help="Minimum consecutive collinear points"),
    no_db: bool = typer.Option(False, "--no-db", help="Skip DB write, parquet only"),
) -> None:
    """Detect linear interpolation in lake area time series."""
    setup_logging("interpolation-detect")
    from lakeanalysis.quality.interpolation_runner import InterpolationRunConfig, run_interpolation_detect

    config = InterpolationRunConfig(
        data_dir=DATA_DIR,
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        min_collinear_points=min_collinear,
        no_db=no_db,
    )
    run_interpolation_detect(config)


@app.command()
def recheck_zero_quantile(
    zero_quantile: float = typer.Option(0.80, help="Quantile for zero check"),
    batch_size: int = typer.Option(10_000, "--batch-size"),
    dry_run: DryRunOpt = False,
) -> None:
    """Recheck lakes previously flagged as zero-quantile."""
    setup_logging("recheck-zero-quantile")
    from lakeanalysis.quality.maintenance_runner import RecheckZeroQuantileConfig, run_recheck_zero_quantile

    run_recheck_zero_quantile(RecheckZeroQuantileConfig(
        zero_quantile=zero_quantile, batch_size=batch_size, dry_run=dry_run,
    ))


@app.command()
def recompute_pv(
    chunk_size: int = typer.Option(5_000, "--chunk-size"),
    start_id: int = typer.Option(0, "--start-id"),
    limit_id: LimitIdOpt = None,
    dry_run: DryRunOpt = False,
) -> None:
    """Recompute penalized volatility (H*CV) for existing quality records."""
    setup_logging("recompute-pv")
    from lakeanalysis.quality.maintenance_runner import RecomputePvConfig, run_recompute_pv

    run_recompute_pv(RecomputePvConfig(
        chunk_size=chunk_size, start_id=start_id, limit_id=limit_id, dry_run=dry_run,
    ))
