"""CLI commands for Hawkes process modelling."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, IdEndOpt, IdStartOpt, IoBudgetOpt, LimitIdOpt, run_batch_engine, setup_logging

app = typer.Typer(help="Hawkes process modelling", no_args_is_help=True)


@app.command()
def run(
    hylak_id: int = typer.Option(..., "--hylak-id", help="Single lake ID (required)"),
    threshold_quantile: float = typer.Option(0.90, help="EOT threshold quantile"),
    hawkes_window_months: float = typer.Option(4.0, help="Kernel window in months"),
    plot: bool = typer.Option(False, "--plot", help="Show diagnostic plots"),
    no_decluster: bool = typer.Option(False, "--no-decluster", help="Disable Runs declustering (keep all exceedances)"),
) -> None:
    """Fit bivariate Hawkes model for a single lake."""
    setup_logging("hawkes")
    from lakesource.postgres import fetch_lake_area_by_ids, fetch_frozen_year_months_by_ids, series_db
    from lakeanalysis.hawkes import (
        LikelihoodRatioTest, TYPE_DRY, TYPE_WET,
        build_events_from_eot, evaluate_intensity_decomposition,
        fit_full_model, fit_restricted_model, run_model_comparison,
    )
    from lakeanalysis.eot import NoDeclustering, RunsDeclustering
    import numpy as np

    with series_db.connection_context() as conn:
        lake_map = fetch_lake_area_by_ids(conn, [hylak_id])
        frozen_map = fetch_frozen_year_months_by_ids(conn, [hylak_id])
    df = lake_map.get(hylak_id)
    if df is None:
        typer.echo(f"Lake {hylak_id} not found.")
        raise typer.Exit(1)
    frozen = set(frozen_map.get(hylak_id, []))

    strategy = NoDeclustering() if no_decluster else RunsDeclustering(run_length=1)
    event_series = build_events_from_eot(
        df,
        threshold_quantile=threshold_quantile,
        frozen_year_months=frozen or None,
        declustering_strategy=strategy,
    )
    typer.echo(f"Events: {len(event_series.times)} dry={int((event_series.event_types==TYPE_DRY).sum())} wet={int((event_series.event_types==TYPE_WET).sum())}")

    full_fit = fit_full_model(event_series, window_months=hawkes_window_months)
    restricted_wd = fit_restricted_model(event_series, disabled_edges=[(TYPE_WET, TYPE_DRY)], window_months=hawkes_window_months)
    restricted_dw = fit_restricted_model(event_series, disabled_edges=[(TYPE_DRY, TYPE_WET)], window_months=hawkes_window_months)

    strategy = LikelihoodRatioTest(significance_level=0.05)
    lrt_wd = run_model_comparison("D_to_W", restricted_wd, full_fit, df=1, test_strategy=strategy)
    lrt_dw = run_model_comparison("W_to_D", restricted_dw, full_fit, df=1, test_strategy=strategy)

    typer.echo(f"\nFull fit converged: {full_fit.converged}")
    typer.echo(f"Log-likelihood: {full_fit.log_likelihood:.2f}")
    typer.echo(f"Spectral radius: {full_fit.spectral_radius:.4f}")
    typer.echo(f"LRT D→W p={lrt_wd.p_value:.4f}  W→D p={lrt_dw.p_value:.4f}")

    if plot:
        from lakeanalysis.hawkes import (
            plot_event_timeline, plot_intensity_decomposition,
            plot_kernel_matrix, plot_lrt_summary,
        )
        import matplotlib.pyplot as plt

        plot_event_timeline(event_series.events_table)
        plot_kernel_matrix(full_fit)

        eval_times = event_series.timeline["time"].to_numpy(dtype=float) if event_series.timeline is not None else np.array([event_series.start_time, event_series.end_time])
        decomp = evaluate_intensity_decomposition(event_series, full_fit, eval_times, window_years=hawkes_window_months/12)
        plot_intensity_decomposition(decomp)

        plt.show()


@app.command()
def eot_batch(
    chunk_size: ChunkSizeOpt = 10_000,
    limit_id: LimitIdOpt = None,
    id_start: IdStartOpt = 0,
    id_end: IdEndOpt = None,
    io_budget: IoBudgetOpt = 4,
    threshold_quantile: float = typer.Option(0.90, help="EOT threshold quantile"),
    hawkes_window_months: float = typer.Option(4.0, help="Kernel window in months"),
    min_event_rate: float = typer.Option(0.01, help="Minimum event rate"),
    max_event_rate: float = typer.Option(0.30, help="Maximum event rate"),
    min_relative_amplitude: float = typer.Option(0.05, help="Minimum relative amplitude"),
    min_median_severity: float = typer.Option(1.0, help="Minimum median severity"),
    monthly_significance_quantile: float = typer.Option(0.95, help="Monthly significance quantile"),
    decluster_run_length: int | None = typer.Option(1, help="Declustering run length (None=NoDeclustering)"),
) -> None:
    """Run batch EOT-Hawkes computation (EOT events → Hawkes fit)."""
    run_batch_engine(
        "eot_hawkes",
        algorithm="eot_hawkes",
        done_table="eot_hawkes_run_status",
        ensure_tables=("eot_hawkes",),
        chunk_size=chunk_size,
        limit_id=limit_id,
        id_start=id_start,
        id_end=id_end,
        io_budget=io_budget,
        calculator_kwargs=dict(
            threshold_quantile=threshold_quantile,
            hawkes_window_months=hawkes_window_months,
            min_event_rate=min_event_rate,
            max_event_rate=max_event_rate,
            min_relative_amplitude=min_relative_amplitude,
            min_median_severity=min_median_severity,
            monthly_significance_quantile=monthly_significance_quantile,
            decluster_run_length=decluster_run_length,
        ),
    )


@app.command()
def qc(
    output_dir: str | None = typer.Option(None, "--output-dir", help="Output directory"),
    threshold_quantile: float | None = typer.Option(None, help="Filter by quantile"),
    results_limit: int = typer.Option(200_000, help="Results sample size"),
    errors_top_n: int = typer.Option(30, help="Top N error messages"),
    no_plots: bool = typer.Option(False, "--no-plots", help="Skip plot generation"),
) -> None:
    """Run production QA on Hawkes batch results."""
    setup_logging("hawkes-qc")
    from pathlib import Path
    from lakesource.config import SourceConfig
    from lakesource.postgres import (
        fetch_hawkes_qc_summary_by_quantile, fetch_hawkes_error_message_counts,
        fetch_hawkes_lrt_summary_by_test, fetch_hawkes_results, series_db,
    )

    if output_dir is None:
        output_dir = str(SourceConfig().data_dir.parent / "visualize" / "qr")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        summary = fetch_hawkes_qc_summary_by_quantile(conn)
        summary.to_csv(out_dir / "hawkes_summary_by_quantile.csv")
        errors = fetch_hawkes_error_message_counts(conn, limit=errors_top_n)
        errors.to_csv(out_dir / "hawkes_error_message_counts.csv")
        lrt = fetch_hawkes_lrt_summary_by_test(conn, threshold_quantile=threshold_quantile)
        lrt.to_csv(out_dir / "hawkes_lrt_summary_by_test.csv")
        results = fetch_hawkes_results(conn, threshold_quantile=threshold_quantile, limit=results_limit)
        results.to_csv(out_dir / "hawkes_results_sample.csv")

    typer.echo(f"Saved QC tables to {out_dir}")


@app.command()
def mining(
    input_summary: str | None = typer.Option(None, "--input-summary", help="Path to summary CSV"),
    output_dir: str | None = typer.Option(None, "--output-dir", help="Output directory"),
    p_threshold: float = typer.Option(0.05, help="LRT p-value threshold"),
    alpha_min: float = typer.Option(1e-3, "--alpha-min", help="Minimum cross-alpha magnitude"),
    min_events: int = typer.Option(12, "--min-events", help="Minimum events"),
    quarter_window: float = typer.Option(0.25, "--quarter-window", help="Window length in years"),
    quarterly_min_mass: float = typer.Option(0.50, "--quarterly-min-mass", help="Min excitation mass in window"),
    max_case_plots: int = typer.Option(500, "--max-case-plots", help="Max case plot count"),
) -> None:
    """Mine batch outputs for short-memory transition lakes."""
    setup_logging("hawkes-mining")
    from pathlib import Path
    import json
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt

    from lakesource.config import SourceConfig
    source = SourceConfig()
    if input_summary is None:
        input_summary = str(source.data_dir.parent / "hawkes" / "batch" / "summary.csv")
    if output_dir is None:
        output_dir = str(source.data_dir.parent / "experiments" / "hawkes" / "mining")

    from lakeanalysis.hawkes.mining import (
        load_summary, safe_series_divide,
        select_transition_lakes, build_overall_stats, load_events_from_case,
    )
    from lakesource.postgres import fetch_lake_area_by_ids, series_db
    from lakeanalysis.eot import plot_eot_extremes

    summary_path = Path(input_summary)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    summary = load_summary(summary_path)
    d_to_w, w_to_d, union = select_transition_lakes(
        summary, p_threshold=p_threshold, alpha_min=alpha_min,
        min_events=min_events, quarter_window_years=quarter_window,
        quarterly_min_mass=quarterly_min_mass,
    )

    stats = build_overall_stats(summary)
    stats.update(dict(p_threshold=p_threshold, alpha_min=alpha_min, min_events=min_events,
                       n_transition_d_to_w=len(d_to_w), n_transition_w_to_d=len(w_to_d),
                       n_transition_union_lakes=union["hylak_id"].nunique() if not union.empty else 0))
    d_to_w.to_csv(out / "transition_lakes_D_to_W.csv", index=False)
    w_to_d.to_csv(out / "transition_lakes_W_to_D.csv", index=False)
    union.to_csv(out / "transition_lakes_union.csv", index=False)
    (out / "overall_stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    # Plot beta histogram
    fig, ax = plt.subplots(figsize=(8, 5))
    data = pd.to_numeric(union["beta_WD"], errors="coerce").dropna()
    ax.hist(data, bins=30) if not data.empty else ax.text(0.5, 0.5, "No transitions", ha="center")
    ax.set_xlabel("beta_WD"); ax.set_title("beta_WD for Transition Lakes")
    fig.tight_layout(); fig.savefig(out / "hist_beta_WD_transition_lakes.png", dpi=300, bbox_inches="tight"); plt.close(fig)

    # Plot short-memory cases (top ranked)
    ranked = union.copy()
    ranked["effect_strength"] = np.where(
        ranked["transition_direction"] == "D_to_W",
        pd.to_numeric(ranked["alpha_WD"], errors="coerce") * pd.to_numeric(ranked["mass_q_D_to_W"], errors="coerce"),
        pd.to_numeric(ranked["alpha_DW"], errors="coerce") * pd.to_numeric(ranked["mass_q_W_to_D"], errors="coerce"),
    )
    ranked = ranked.sort_values("effect_strength", ascending=False).head(max_case_plots)
    if not ranked.empty:
        case_dir = out / "short_memory_cases"
        case_dir.mkdir(exist_ok=True)
        ids = sorted({int(v) for v in ranked["hylak_id"].dropna().tolist()})
        with series_db.connection_context() as conn:
            lake_map = fetch_lake_area_by_ids(conn, ids)
        for _, row in ranked.iterrows():
            hid = int(row["hylak_id"])
            df = lake_map.get(hid)
            if df is None:
                continue
            events = load_events_from_case(Path(str(row["output_dir"])))
            if events.empty:
                continue
            fig = plot_eot_extremes(hylak_id=hid, series_df=df, extremes_df=events, annotate_top_n_each_tail=6)
            fig.savefig(case_dir / f"hylak_{hid}_combined_extremes.png", dpi=300, bbox_inches="tight")
            plt.close(fig)

    typer.echo(f"Done: D→W={len(d_to_w)} W→D={len(w_to_d)} union_lakes={stats['n_transition_union_lakes']}")
