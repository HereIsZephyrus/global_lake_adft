"""CLI commands for extremes-over-threshold analysis."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, IdEndOpt, IdStartOpt, IoBudgetOpt, LimitIdOpt, run_batch_engine

app = typer.Typer(help="Extremes-over-threshold analysis", no_args_is_help=True)


@app.command()
def run(
    chunk_size: ChunkSizeOpt = 10_000,
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    io_budget: IoBudgetOpt = 4,
    tail: str = typer.Option("both", help="Tail: high, low, or both"),
    threshold_quantiles: list[float] = typer.Option([0.95, 0.98], "--threshold", help="Quantile thresholds"),
) -> None:
    """Run batch EOT (excess-over-threshold) computation."""
    tails = ["high", "low"] if tail == "both" else [tail]
    run_batch_engine(
        "eot",
        algorithm="eot",
        done_table="eot_run_status",
        ensure_tables=("eot",),
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        io_budget=io_budget,
        calculator_kwargs={"tails": tails, "quantiles": threshold_quantiles},
    )


@app.command()
def basemodel(
    limit_id: LimitIdOpt = None,
    hylak_id: int | None = typer.Option(None, "--hylak-id", help="Single lake ID"),
    all_lakes: bool = typer.Option(False, "--all", help="Process all lakes"),
    criterion: str = typer.Option("aic", help="Model selection criterion (aic/bic)"),
    max_rmse: float = typer.Option(1.0, "--max-rmse", help="Max relative RMSE"),
    no_trend: bool = typer.Option(False, "--no-trend", help="Disable trend candidates"),
    plot: bool = typer.Option(False, "--plot", help="Show matplotlib plots"),
) -> None:
    """Select baseline model (seasonal/trend) via AIC/BIC."""
    from ._common import setup_logging
    setup_logging("basemodel")

    from lakeanalysis.eot import BasisSelector

    selector = BasisSelector(criterion=criterion, include_trend=not no_trend, max_relative_rmse=max_rmse)

    if hylak_id is not None:
        from lakesource.postgres import fetch_lake_area, series_db
        from lakeanalysis.eot import MonthlyTimeSeries

        with series_db.connection_context() as conn:
            df = fetch_lake_area(conn, hylak_id)
        series = MonthlyTimeSeries.from_frame(df)
        result = selector.select_result(series.times, series.values)
        base_name = result.selected_record.basis_name if result.selected_record else "none"
        print(f"Selected: {base_name}")
        print(f"AIC: {result.selected_record.aic:.2f}" if result.selected_record else "AIC: n/a")
        if plot:
            from lakeanalysis.eot import plot_candidate_scores, plot_basis_fit, plot_residuals
            fit = selector.fit_basis(series.times, series.values, result.selected_basis)
            plot_candidate_scores(result.candidate_records, result.criterion, base_name)
            plot_basis_fit(fit, base_name, result.criterion, result.relative_rmse)
            plot_residuals(fit)
    elif all_lakes or limit_id is not None:
        from lakesource.postgres import fetch_lake_area_by_ids, series_db

        with series_db.connection_context() as conn:
            lake_map = fetch_lake_area_by_ids(conn, list(range(1, limit_id or 100)))
        for hid, df in lake_map.items():
            series = MonthlyTimeSeries.from_frame(df)
            result = selector.select_result(series.times, series.values)
            base_name = result.selected_record.basis_name if result.selected_record else "none"
            print(f"hylak_id={hid}: {base_name}")
    else:
        typer.echo("Specify --hylak-id, --limit-id, or --all")


@app.command()
def quantile(
    chunk_size: ChunkSizeOpt = 10_000,
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    io_budget: IoBudgetOpt = 4,
    min_valid_per_month: int | None = typer.Option(None, help="Min valid obs per month"),
    min_valid_observations: int | None = typer.Option(None, help="Min total valid obs"),
    method: str = typer.Option("stl", help="Decomposition method: stl | legacy"),
) -> None:
    """Run batch quantile computation."""
    run_batch_engine(
        "quantile",
        algorithm="quantile",
        done_table="quantile_run_status",
        ensure_tables=("quantile",),
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        io_budget=io_budget,
        calculator_kwargs=dict(
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
            method=method,
        ),
    )
