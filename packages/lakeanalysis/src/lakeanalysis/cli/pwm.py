"""CLI commands for PWM extreme & PWM-Hawkes analysis."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, FilterNameOpt, IdEndOpt, IdStartOpt, LimitIdOpt, run_batch_engine

app = typer.Typer(help="PWM extreme & PWM-Hawkes analysis", no_args_is_help=True)


@app.command()
def run(
    chunk_size: ChunkSizeOpt = 10_000,
    filter_name: FilterNameOpt = "full",
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    threshold_quantiles: list[float] = typer.Option([0.95, 0.99], "--threshold-quantile", help="PWM threshold quantiles"),
    min_valid_per_month: int | None = typer.Option(None, help="Min valid obs per month"),
    min_valid_observations: int | None = typer.Option(None, help="Min total valid obs"),
    method: str = typer.Option("stl", help="Decomposition method: stl | legacy"),
) -> None:
    """Run batch PWM extreme computation."""
    run_batch_engine(
        "pwm_extreme",
        algorithm="pwm_extreme",
        done_table="pwm_extreme_run_status",
        ensure_tables=("pwm_extreme",),
        chunk_size=chunk_size,
        filter_name=filter_name,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        calculator_kwargs=dict(
            threshold_quantiles=threshold_quantiles,
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
            method=method,
        ),
    )


@app.command()
def hawkes(
    chunk_size: ChunkSizeOpt = 10_000,
    filter_name: FilterNameOpt = "full",
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    threshold_quantiles: list[float] = typer.Option([0.95, 0.99], "--threshold-quantile", help="PWM threshold quantiles"),
    decay_rate: float = typer.Option(0.8, help="Exponential decay rate λ for S_k strength"),
    hawkes_window_months: float = typer.Option(4.0, help="Hawkes kernel window in months"),
    monthly_significance_quantile: float = typer.Option(0.95, help="Monthly significance quantile"),
) -> None:
    """Run batch PWM-Hawkes computation."""
    run_batch_engine(
        "pwm_hawkes",
        algorithm="pwm_hawkes",
        done_table="pwm_hawkes_run_status",
        ensure_tables=("pwm_extreme", "pwm_hawkes"),
        chunk_size=chunk_size,
        filter_name=filter_name,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        calculator_kwargs=dict(
            threshold_quantiles=threshold_quantiles,
            decay_rate=decay_rate,
            hawkes_window_months=hawkes_window_months,
            monthly_significance_quantile=monthly_significance_quantile,
        ),
    )


@app.command()
def diag(
    limit_id: int = typer.Option(200, "--limit-id", help="Upper hylak_id bound"),
    chunk_size: ChunkSizeOpt = 10_000,
    filter_name: FilterNameOpt = "full",
    skip_run: bool = typer.Option(False, "--skip-run", help="Skip batch run, only fetch diagnosis"),
) -> None:
    """Run PWM-Hawkes diagnostic on small sample, print summary."""
    from ._common import setup_logging
    setup_logging("pwm-hawkes-diag")

    if not skip_run:
        run_batch_engine(
            "pwm_hawkes_diag",
            algorithm="pwm_hawkes",
            done_table="pwm_hawkes_run_status",
            ensure_tables=("pwm_extreme", "pwm_hawkes"),
            chunk_size=chunk_size,
            filter_name=filter_name,
            limit_id=limit_id,
            calculator_kwargs=dict(
                threshold_quantiles=[0.95, 0.99],
                decay_rate=0.8, hawkes_window_months=4.0,
                monthly_significance_quantile=0.95,
            ),
        )

    import pandas as pd
    from lakesource.config import SourceConfig

    config = SourceConfig()
    parquet_path = config.data_dir / "pwm_hawkes_results.parquet"
    if not parquet_path.exists():
        typer.echo(f"No results found at {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    typer.echo(f"\nTotal rows: {len(df)}")
    typer.echo(f"Converged: {df['converged'].sum()}")
    typer.echo(f"QC pass: {df['qc_pass'].sum()}")
    typer.echo(f"Median n_events: {df['n_events'].median()}")
