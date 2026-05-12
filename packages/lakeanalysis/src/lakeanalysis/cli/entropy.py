"""CLI commands for Apportionment Entropy pipeline."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, LimitIdOpt, setup_logging

app = typer.Typer(help="Apportionment Entropy computation pipeline", no_args_is_help=True)


@app.command()
def run(
    limit_id: LimitIdOpt = None,
    chunk_size: ChunkSizeOpt = 10_000,
    plot: bool = typer.Option(False, "--plot", help="Show matplotlib plots after computation"),
) -> None:
    """Run the AE pipeline in resumable chunks."""
    setup_logging("entropy")
    from lakeanalysis.entropy.service import EntropyRunConfig, run_entropy
    from lakesource.config import SourceConfig

    run_entropy(
        EntropyRunConfig(
            data_dir=SourceConfig().data_dir.parent / "entropy",
            limit_id=limit_id,
            chunk_size=chunk_size,
            show_plot=plot,
        )
    )


@app.command()
def update_amplitude(
    plot: bool = typer.Option(False, "--plot", help="Show plots after update"),
) -> None:
    """Refresh mean_seasonal_amplitude only (no AE recompute)."""
    setup_logging("entropy-amplitude")
    from lakeanalysis.entropy.service import run_update_amplitude_only
    from lakesource.config import SourceConfig

    run_update_amplitude_only(SourceConfig().data_dir.parent / "entropy", show_plot=plot)


@app.command()
def plot_only() -> None:
    """Load existing parquet data and generate plots (no computation)."""
    setup_logging("entropy-plot")
    from lakeanalysis.entropy.runner import show_entropy_plots
    from lakesource.config import SourceConfig

    show_entropy_plots(SourceConfig().data_dir.parent / "entropy", limit_id=None)
