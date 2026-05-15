"""CLI commands for algorithm comparison."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, IoBudgetOpt, setup_logging

app = typer.Typer(help="Algorithm comparison pipelines", no_args_is_help=True)


@app.command()
def run(
    sample_file: str = typer.Option(..., "--sample-file", help="Path to sample_lakes.parquet"),
    chunk_size: ChunkSizeOpt = 5_000,
    io_budget: IoBudgetOpt = 4,
    min_valid_per_month: int | None = typer.Option(None, help="Min valid obs per month"),
    min_valid_observations: int | None = typer.Option(None, help="Min total valid obs"),
    output_dir: str = typer.Option("data/comparison", help="Post-processing output dir"),
) -> None:
    """Run algorithm comparison (Quantile vs PWM) on sampled lakes."""
    setup_logging("comparison")
    import pandas as pd
    from lakeanalysis.batch import Engine, IdSetFilter, build_provider_batch_reader, build_provider_batch_writer
    from lakeanalysis.batch.calculator.factory import CalculatorFactory
    from lakesource.config import SourceConfig

    sample = pd.read_parquet(sample_file)
    sample_ids = set(int(v) for v in sample["hylak_id"].dropna().tolist())

    config = SourceConfig()
    reader = build_provider_batch_reader(config, done_table="comparison_run_status", done_requires_status=True)
    writer = build_provider_batch_writer(config, ensure_tables=["comparison"])
    calculator = CalculatorFactory.create("comparison",
        min_valid_per_month=min_valid_per_month,
        min_valid_observations=min_valid_observations)
    engine = Engine(reader=reader, writer=writer, calculator=calculator,
                    algorithm="comparison", lake_filter=IdSetFilter(sample_ids),
                    chunk_size=chunk_size, io_budget=io_budget)
    engine.run()
    typer.echo(f"Comparison complete. Output: {output_dir}")


@app.command()
def area(
    data_dir: str | None = typer.Option(None, help="Parquet data directory"),
    output_dir: str = typer.Option("data/comparison/benchmarks/area_vs_atlas", help="Output directory"),
    good_threshold: float = typer.Option(2.0, help="Good agreement: ratio in [1/G, G]"),
    moderate_threshold: float = typer.Option(5.0, help="Moderate agreement threshold"),
    poor_threshold: float = typer.Option(10.0, help="Poor agreement threshold"),
    sample: int = typer.Option(120, help="Number of extreme-ratio lakes to sample for plotting"),
) -> None:
    """Compare rs_area vs atlas_area from area_quality."""
    setup_logging("area-comparison")
    from lakesource.config import SourceConfig
    if data_dir is None:
        data_dir = str(SourceConfig().data_dir)
    import duckdb
    from pathlib import Path
    from lakeanalysis.quality.comparison import enrich_comparison_df, summarize_comparison
    from lakeanalysis.quality.metrics import AgreementConfig
    import matplotlib.pyplot as plt

    parquet_dir = Path(data_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{parquet_dir}/area_quality/*.parquet'").df()
    df = enrich_comparison_df(df, config=AgreementConfig(good=good_threshold, moderate=moderate_threshold, poor=poor_threshold))

    summary_m = summarize_comparison(df, rs_col="rs_area_mean", config=AgreementConfig(good=good_threshold, moderate=moderate_threshold, poor=poor_threshold))
    summary_med = summarize_comparison(df, rs_col="rs_area_median", config=AgreementConfig(good=good_threshold, moderate=moderate_threshold, poor=poor_threshold))

    typer.echo(f"\nTotal valid lakes: {summary_m.get('n_total', 0)}")
    typer.echo(f"Agreement (mean): {summary_m.get('n_by_agreement', {})}")
    typer.echo(f"Over/under/agree (mean): {summary_m.get('n_overestimate', 0)}/{summary_m.get('n_underestimate', 0)}/{summary_m.get('n_agree', 0)}")
    typer.echo(f"Median ratio (median): {summary_med.get('median_ratio', 'N/A')}")

    # Scatter plot
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(df["atlas_area"], df["ratio_median"], s=1, alpha=0.5)
    ax.set_xscale("log"); ax.set_xlabel("Atlas area (km²)"); ax.set_ylabel("Ratio (RS/Atlas)")
    ax.set_title("RS Median Area vs Atlas Area")
    fig.savefig(out / "scatter_ratio_vs_atlas.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    typer.echo(f"Saved plots to {out}")


@app.command()
def grid_agg(
    sample_file: str | None = typer.Option(None, help="Sample file path"),
    resolution: float = typer.Option(0.5, help="Grid resolution in degrees"),
    refresh: bool = typer.Option(False, "--refresh", help="Force recompute even if cached"),
) -> None:
    """Grid-level exceedance rate aggregation."""
    setup_logging("comparison-grid")
    from lakesource.config import SourceConfig
    if sample_file is None:
        sample_file = str(SourceConfig().data_dir.parent / "comparison" / "sample_lakes.parquet")
    import pandas as pd
    from lakesource.provider.factory import create_provider

    sample = pd.read_parquet(sample_file)
    sample_ids = set(int(v) for v in sample["hylak_id"].dropna().tolist())
    provider = create_provider(SourceConfig())
    result = provider.fetch_grid_agg("comparison.exceedance", resolution, refresh=refresh, sample_ids=sample_ids)
    typer.echo(f"Grid aggregation complete: {len(result) if result is not None else 0} cells")


@app.command()
def sample_lakes(
    n_samples: int = typer.Option(50_000, help="Target total sampled lakes"),
    output_dir: str | None = typer.Option(None, help="Output directory"),
    seed: int = typer.Option(42, help="Random seed"),
) -> None:
    """Geographic stratified lake sampling for comparison benchmarks."""
    setup_logging("sample-lakes")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = str(SourceConfig().data_dir.parent / "comparison" / "benchmarks" / "algorithms" / "full" / "sample")
    from pathlib import Path
    from lakesource.postgres import series_db  # pylint: disable=no-name-in-module
    import pandas as pd
    import numpy as np

    with series_db.connection_context() as conn:
        df = pd.read_sql(
            "SELECT li.hylak_id, li.latitude, li.longitude FROM lake_info li "
            "JOIN area_quality aq ON li.hylak_id = aq.hylak_id",
            conn,
        )
    rng = np.random.default_rng(seed)
    sampled = df.sample(n=min(n_samples, len(df)), random_state=rng, replace=False)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    sampled.to_parquet(out / "sample_lakes.parquet", index=False)
    typer.echo(f"Sampled {len(sampled)} lakes → {out / 'sample_lakes.parquet'}")
