"""CLI commands for experiment workflows.

Consolidates multi-step research / benchmark workflows into a small set of
top-level entry points.
"""

from __future__ import annotations

import typer

from ._common import setup_logging

app = typer.Typer(help="Experiment workflows", no_args_is_help=True)


@app.command()
def artificial(
    limit_id: int | None = typer.Option(None, "--limit-id", help="Limit lake id for preparation stages"),
    max_area_ratio: float = typer.Option(10.0, "--max-area-ratio", help="Max area ratio between matched lakes"),
    limit_pairs: int | None = typer.Option(None, "--limit-pairs", help="Limit similarity/impact pairs"),
    z_threshold: float = typer.Option(3.0, "--z-threshold", help="Z-score anomaly threshold"),
    plot: bool = typer.Option(False, "--plot", help="Render plots after compute"),
    plot_only: bool = typer.Option(False, "--plot-only", help="Render plots only"),
) -> None:
    """Run the full artificial lake experiment pipeline: pfaf -> nearest -> similarity -> impact."""
    setup_logging("experiment-artificial")
    from lakeanalysis.artificial.pfaf.runner import PfafRunConfig, run_pfaf
    from lakeanalysis.artificial.pfaf.nearest_runner import NearestRunConfig, run_nearest
    from lakeanalysis.artificial.similarity.runner import SimilarityRunConfig, run_similarity, show_similarity_plots
    from lakeanalysis.artificial.impact.runner import ImpactRunConfig, run_impact, load_impact_csv, show_impact_plots
    from lakesource.config import SourceConfig

    source = SourceConfig()
    if limit_id is not None:
        run_pfaf(PfafRunConfig(limit_id=limit_id))
        run_nearest(NearestRunConfig(limit_id=limit_id, max_area_ratio=max_area_ratio))
    else:
        run_pfaf(PfafRunConfig())
        run_nearest(NearestRunConfig(max_area_ratio=max_area_ratio))

    data_dir = source.data_dir.parent / "experiments" / "artificial" / "similarity"
    impact_dir = source.data_dir.parent / "experiments" / "artificial" / "impact"

    if plot_only:
        show_similarity_plots(data_dir)
        impact_df = load_impact_csv(impact_dir)
        if not impact_df.empty:
            show_impact_plots(impact_dir, rows=impact_df.to_dict("records"))
        return

    run_similarity(SimilarityRunConfig(data_dir=data_dir, limit_pairs=limit_pairs, show_plot=plot))
    run_impact(ImpactRunConfig(data_dir=impact_dir, limit_pairs=limit_pairs, z_threshold=z_threshold, show_plot=plot))


@app.command()
def entropy(
    limit_id: int | None = typer.Option(None, "--limit-id", help="Limit lake id"),
    chunk_size: int = typer.Option(10_000, "--chunk-size", help="Chunk size"),
    plot: bool = typer.Option(False, "--plot", help="Render plots after compute"),
    update_amplitude_only: bool = typer.Option(False, "--update-amplitude-only", help="Refresh amplitudes only"),
    plot_only: bool = typer.Option(False, "--plot-only", help="Plot only from existing parquet"),
) -> None:
    """Run the entropy experiment end-to-end."""
    setup_logging("experiment-entropy")
    from lakeanalysis.entropy.runner import show_entropy_plots
    from lakeanalysis.entropy.service import EntropyRunConfig, run_entropy, run_update_amplitude_only
    from lakesource.config import SourceConfig

    data_dir = SourceConfig().data_dir.parent / "experiments" / "entropy"
    if plot_only:
        show_entropy_plots(data_dir, limit_id=None)
        return
    if update_amplitude_only:
        run_update_amplitude_only(data_dir, show_plot=plot)
        return
    run_entropy(EntropyRunConfig(data_dir=data_dir, limit_id=limit_id, chunk_size=chunk_size, show_plot=plot))


@app.command("benchmark-algorithms")
def benchmark_algorithms(
    n_samples: int = typer.Option(50_000, "--n-samples", help="Number of benchmark sample lakes"),
    chunk_size: int = typer.Option(5_000, "--chunk-size", help="Batch chunk size"),
    io_budget: int = typer.Option(4, "--io-budget", help="DB IO budget"),
    resolution: float = typer.Option(0.5, "--resolution", help="Grid resolution"),
    refresh: bool = typer.Option(False, "--refresh", help="Force recompute grid cache"),
    compare_gt10: bool = typer.Option(True, "--gt10/--no-gt10", help="Include gt10 comparison plots"),
) -> None:
    """Run the algorithm benchmark suite on full and gt10 datasets."""
    setup_logging("experiment-benchmark-algorithms")
    from lakesource.config import SourceConfig
    from . import comparison as comparison_cli
    from . import plot as plot_cli

    source = SourceConfig()
    sample_dir = source.data_dir.parent / "comparison" / "benchmarks" / "algorithms" / "full"
    sample_file = sample_dir / "sample_lakes.parquet"

    comparison_cli.sample_lakes(n_samples=n_samples, output_dir=str(sample_dir), seed=42)
    comparison_cli.run(
        sample_file=str(sample_file),
        chunk_size=chunk_size,
        io_budget=io_budget,
        output_dir=str(sample_dir),
    )
    comparison_cli.grid_agg(sample_file=str(sample_file), resolution=resolution, refresh=refresh)
    plot_cli.comparison_global(
        refresh=refresh,
        resolution=resolution,
        gt10_dir=source.data_dir.parent / "parquet_gt10",
        full_dir=source.data_dir,
    )
    plot_cli.comparison_zonal(
        data_dir=source.data_dir,
    )
    if compare_gt10:
        plot_cli.quantile_global(refresh=refresh, resolution=resolution, data_dir=source.data_dir.parent / "parquet_gt10")
        plot_cli.pwm_global(refresh=refresh, resolution=resolution, data_dir=source.data_dir.parent / "parquet_gt10")
        plot_cli.eot_global(refresh=refresh, resolution=resolution, data_dir=source.data_dir.parent / "parquet_gt10")


@app.command("interpolation")
def interpolation(
    limit_id: int | None = typer.Option(None, "--limit-id", help="Limit lake id"),
    chunk_size: int = typer.Option(10_000, "--chunk-size", help="Chunk size"),
    id_start: int = typer.Option(0, "--id-start"),
    id_end: int | None = typer.Option(None, "--id-end"),
    min_collinear: int = typer.Option(4, "--min-collinear", help="Minimum consecutive collinear points"),
    no_db: bool = typer.Option(False, "--no-db", help="Skip DB write, parquet only"),
) -> None:
    """Detect linear interpolation in lake area time series."""
    setup_logging("experiment-interpolation")
    from lakeanalysis.quality.interpolation_runner import InterpolationRunConfig, run_interpolation_detect
    from lakesource.config import SourceConfig

    config = InterpolationRunConfig(
        data_dir=SourceConfig().data_dir.parent / "experiments" / "interpolation",
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        min_collinear_points=min_collinear,
        no_db=no_db,
    )
    run_interpolation_detect(config)


@app.command("area-vs-atlas")
def area_vs_atlas(
    data_dir: str | None = typer.Option(None, help="Parquet data directory"),
    good_threshold: float = typer.Option(2.0, help="Good agreement: ratio in [1/G, G]"),
    moderate_threshold: float = typer.Option(5.0, help="Moderate agreement threshold"),
    poor_threshold: float = typer.Option(10.0, help="Poor agreement threshold"),
    sample: int = typer.Option(120, help="Number of extreme-ratio lakes to sample for plotting"),
) -> None:
    """Compare rs_area vs atlas_area from area_quality."""
    setup_logging("experiment-area-vs-atlas")
    from . import comparison as comparison_cli
    from lakesource.config import SourceConfig

    output_dir = SourceConfig().data_dir.parent / "comparison" / "benchmarks" / "area_vs_atlas"
    comparison_cli.area(
        data_dir=data_dir,
        output_dir=str(output_dir),
        good_threshold=good_threshold,
        moderate_threshold=moderate_threshold,
        poor_threshold=poor_threshold,
        sample=sample,
    )


@app.command("hawkes-mining")
def hawkes_mining(
    input_summary: str | None = typer.Option(None, "--input-summary", help="Path to summary CSV"),
    p_threshold: float = typer.Option(0.05, help="LRT p-value threshold"),
    alpha_min: float = typer.Option(1e-3, "--alpha-min", help="Minimum cross-alpha magnitude"),
    min_events: int = typer.Option(12, "--min-events", help="Minimum events"),
    quarter_window: float = typer.Option(0.25, "--quarter-window", help="Window length in years"),
    quarterly_min_mass: float = typer.Option(0.50, "--quarterly-min-mass", help="Min excitation mass in window"),
    max_case_plots: int = typer.Option(500, "--max-case-plots", help="Max case plot count"),
) -> None:
    """Mine batch outputs for short-memory transition lakes."""
    setup_logging("experiment-hawkes-mining")
    from . import hawkes as hawkes_cli

    hawkes_cli.mining(
        input_summary=input_summary,
        p_threshold=p_threshold,
        alpha_min=alpha_min,
        min_events=min_events,
        quarter_window=quarter_window,
        quarterly_min_mass=quarterly_min_mass,
        max_case_plots=max_case_plots,
    )
