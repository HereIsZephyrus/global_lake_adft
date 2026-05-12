"""CLI commands for PWM extreme & PWM-Hawkes analysis."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, IdEndOpt, IdStartOpt, IoBudgetOpt, LimitIdOpt, run_batch_engine

app = typer.Typer(help="PWM extreme & PWM-Hawkes analysis", no_args_is_help=True)


@app.command()
def run(
    chunk_size: ChunkSizeOpt = 10_000,
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    io_budget: IoBudgetOpt = 4,
    min_valid_per_month: int | None = typer.Option(None, help="Min valid obs per month"),
    min_valid_observations: int | None = typer.Option(None, help="Min total valid obs"),
) -> None:
    """Run batch PWM extreme computation."""
    run_batch_engine(
        "pwm_extreme",
        algorithm="pwm_extreme",
        done_table="pwm_extreme_run_status",
        ensure_tables=("pwm_extreme",),
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        io_budget=io_budget,
        calculator_kwargs=dict(
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
        ),
    )


@app.command()
def hawkes(
    chunk_size: ChunkSizeOpt = 10_000,
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    io_budget: IoBudgetOpt = 4,
    decluster_run_length: int = typer.Option(1, help="Declustering run length"),
    hawkes_window_months: float = typer.Option(4.0, help="Hawkes kernel window in months"),
    min_events: int = typer.Option(10, help="Minimum events for Hawkes fitting"),
    min_event_rate: float = typer.Option(0.01, help="Minimum event rate"),
    max_event_rate: float = typer.Option(0.30, help="Maximum event rate"),
    min_relative_amplitude: float = typer.Option(0.05, help="Minimum relative amplitude"),
    min_median_severity: float = typer.Option(1.0, help="Minimum median severity"),
    monthly_significance_quantile: float = typer.Option(0.95, help="Monthly significance quantile"),
) -> None:
    """Run batch PWM-Hawkes computation."""
    run_batch_engine(
        "pwm_hawkes",
        algorithm="pwm_hawkes",
        done_table="pwm_hawkes_run_status",
        ensure_tables=("pwm_extreme", "hawkes"),
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        io_budget=io_budget,
        calculator_kwargs=dict(
            decluster_run_length=decluster_run_length,
            hawkes_window_months=hawkes_window_months,
            min_events=min_events,
            min_event_rate=min_event_rate,
            max_event_rate=max_event_rate,
            min_relative_amplitude=min_relative_amplitude,
            min_median_severity=min_median_severity,
            monthly_significance_quantile=monthly_significance_quantile,
        ),
    )


@app.command()
def diag(
    limit_id: int = typer.Option(200, "--limit-id", help="Upper hylak_id bound"),
    chunk_size: ChunkSizeOpt = 10_000,
    io_budget: IoBudgetOpt = 4,
    skip_run: bool = typer.Option(False, "--skip-run", help="Skip batch run, only fetch diagnosis"),
    min_events: int = typer.Option(5, help="Minimum events for Hawkes fitting"),
    min_event_rate: float = typer.Option(0.005, help="Minimum event rate"),
    max_event_rate: float = typer.Option(0.50, help="Maximum event rate"),
) -> None:
    """Run PWM-Hawkes diagnostic on small sample, print summary."""
    from ._common import setup_logging
    setup_logging("pwm-hawkes-diag")

    if not skip_run:
        run_batch_engine(
            "pwm_hawkes_diag",
            algorithm="pwm_hawkes",
            done_table="pwm_hawkes_run_status",
            ensure_tables=("pwm_extreme", "hawkes"),
            chunk_size=chunk_size,
            limit_id=limit_id,
            io_budget=io_budget,
            calculator_kwargs=dict(
                decluster_run_length=1, hawkes_window_months=4.0,
                min_events=min_events, min_event_rate=min_event_rate,
                max_event_rate=max_event_rate, min_relative_amplitude=0.05,
                min_median_severity=1.0, monthly_significance_quantile=0.95,
            ),
        )

    import pandas as pd
    from lakesource.config import SourceConfig

    config = SourceConfig()
    parquet_path = config.data_dir / "hawkes" / "hawkes_results.parquet"
    if not parquet_path.exists():
        typer.echo(f"No results found at {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    typer.echo(f"\nTotal rows: {len(df)}")
    typer.echo(f"Converged: {df['converged'].sum()}")
    typer.echo(f"QC pass: {df['qc_pass'].sum()}")
    typer.echo(f"Median n_events: {df['n_events'].median()}")
