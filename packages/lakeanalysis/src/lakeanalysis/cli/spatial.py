"""CLI commands for spatial / topological pipelines."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, LimitIdOpt, setup_logging

app = typer.Typer(help="Spatial / topological pipelines", no_args_is_help=True)


@app.command()
def pfaf(
    limit_id: LimitIdOpt = None,
    chunk_size: ChunkSizeOpt = 10_000,
) -> None:
    """Run Pfafstetter basin ID lookup."""
    setup_logging("pfaf")
    from lakeanalysis.artificial.pfaf.runner import PfafRunConfig, run_pfaf

    run_pfaf(PfafRunConfig(limit_id=limit_id, chunk_size=chunk_size))


@app.command()
def nearest(
    limit_id: LimitIdOpt = None,
    max_area_ratio: float = typer.Option(10.0, "--max-area-ratio", help="Max area ratio between matched lakes"),
) -> None:
    """Search nearest natural lake for each artificial lake."""
    setup_logging("nearest")
    from lakeanalysis.artificial.pfaf.nearest_runner import NearestRunConfig, run_nearest

    run_nearest(NearestRunConfig(limit_id=limit_id, max_area_ratio=max_area_ratio))


@app.command()
def similarity(
    limit_pairs: int | None = typer.Option(None, "--limit-pairs", help="Only first N pairs"),
    plot: bool = typer.Option(False, "--plot", help="Show plots after computation"),
    plot_only: bool = typer.Option(False, "--plot-only", help="Load from CSV and plot only"),
) -> None:
    """Compute Pearson + ACF cosine similarity for lake pairs."""
    setup_logging("similarity")
    from lakesource.config import SourceConfig
    from lakeanalysis.artificial.similarity.runner import SimilarityRunConfig, run_similarity

    data_dir = SourceConfig().data_dir.parent / "experiments" / "artificial" / "similarity"
    if plot_only:
        from lakeanalysis.artificial.similarity.runner import show_similarity_plots
        show_similarity_plots(data_dir)
    else:
        run_similarity(SimilarityRunConfig(data_dir=data_dir, limit_pairs=limit_pairs, show_plot=plot))


@app.command()
def impact(
    limit_pairs: int | None = typer.Option(None, "--limit-pairs", help="Only first N pairs"),
    z_threshold: float = typer.Option(3.0, "--z-threshold", help="Z-score anomaly threshold"),
    plot: bool = typer.Option(False, "--plot", help="Show plots after computation"),
    plot_only: bool = typer.Option(False, "--plot-only", help="Load from CSV and plot only"),
) -> None:
    """Compute human-impact metrics for artificial-natural lake pairs."""
    setup_logging("impact")
    from lakesource.config import SourceConfig
    from lakeanalysis.artificial.impact.runner import ImpactRunConfig, run_impact

    data_dir = SourceConfig().data_dir.parent / "experiments" / "artificial" / "impact"
    if plot_only:
        from lakeanalysis.artificial.impact.runner import load_impact_csv, show_impact_plots
        rows = load_impact_csv(data_dir)
        show_impact_plots(data_dir, rows=rows)
    else:
        run_impact(ImpactRunConfig(data_dir=data_dir, limit_pairs=limit_pairs, z_threshold=z_threshold, show_plot=plot))
