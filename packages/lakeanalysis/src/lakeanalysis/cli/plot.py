"""CLI commands for visualisation figure generation.

All plot commands follow the pattern:
  lake_adft plot <figure-name> [--output-dir ...] [--options ...]

Global maps accept --data-dir to switch between full and gt10 subsets.
Single-lake plots require --hylak-id.
"""

from __future__ import annotations

from pathlib import Path

import typer

from ._common import setup_logging

app = typer.Typer(help="Visualisation figure generation", no_args_is_help=True)


# ── Global distribution maps ────────────────────────────────────────────────


@app.command()
def eot_global(
    tail: str | None = typer.Option(None, help="Filter tail: high or low"),
    quantile: float | None = typer.Option(None, help="Filter threshold quantile"),
    refresh: bool = typer.Option(False, "--refresh", help="Force recompute grid cache"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    resolution: float = typer.Option(0.5, help="Grid resolution in degrees"),
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Parquet data dir (for gt10 subset)"),
) -> None:
    """Generate global EOT distribution maps (convergence, xi, sigma, frequency)."""
    setup_logging("plot-eot-global")
    import matplotlib.pyplot as plt
    from lakesource.config import SourceConfig
    from lakesource.eot.reader import fetch_available_quantiles
    from lakesource.provider import create_provider
    from lakeviz.config import GlobalGridConfig
    from lakeviz.eot import (
        plot_eot_convergence_map, plot_eot_extremes_density_map,
        plot_eot_extremes_frequency_map, plot_eot_sigma_map,
        plot_eot_threshold_map, plot_eot_xi_map,
    )
    from lakeviz.style.presets import Theme
    Theme.apply()

    source = SourceConfig(data_dir=data_dir) if data_dir else SourceConfig()

    if output_dir is None:
        output_dir = source.figures_dir

    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=resolution, output_dir=output_dir)

    combos = fetch_available_quantiles(source)
    if combos.empty:
        typer.echo("No EOT results found"); return
    if tail:
        combos = combos[combos["tail"] == tail]
    if quantile:
        combos = combos[combos["threshold_quantile"].astype(float) == quantile]

    plot_fns = [
        ("convergence", plot_eot_convergence_map), ("xi", plot_eot_xi_map),
        ("sigma", plot_eot_sigma_map), ("frequency", plot_eot_extremes_frequency_map),
        ("density", plot_eot_extremes_density_map), ("threshold", plot_eot_threshold_map),
    ]
    for _, row in combos.iterrows():
        t, q = row["tail"], float(row["threshold_quantile"])
        for name, fn in plot_fns:
            try:
                fn(grid_config, t, q, refresh=refresh)
            except Exception as e:
                typer.echo(f"  Failed {name} tail={t} q={q}: {e}")
            plt.close("all")
    typer.echo(f"EOT global maps saved to {output_dir}")


@app.command()
def pwm_global(
    refresh: bool = typer.Option(False, "--refresh"),
    monthly_only: bool = typer.Option(False, "--monthly-only", help="Only monthly threshold maps"),
    exceedance_only: bool = typer.Option(False, "--exceedance-only", help="Only exceedance maps"),
    p_values: list[float] = typer.Option([0.01, 0.025, 0.05, 0.10], "--p-values", help="Exceedance p-values"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    resolution: float = typer.Option(0.5, help="Grid resolution"),
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Parquet data dir"),
) -> None:
    """Generate global PWM extreme distribution maps."""
    setup_logging("plot-pwm-global")
    import matplotlib.pyplot as plt
    from lakesource.config import SourceConfig
    from lakesource.provider import create_provider
    from lakeviz.config import GlobalGridConfig
    from lakeviz.pwm import (
        plot_pwm_convergence_map, plot_pwm_exceedance_maps,
        plot_pwm_monthly_exceedance_maps, plot_pwm_monthly_threshold_maps,
        plot_pwm_threshold_high_map, plot_pwm_threshold_low_map,
    )
    from lakeviz.style.presets import Theme
    Theme.apply()

    source = SourceConfig(data_dir=data_dir) if data_dir else SourceConfig()

    if output_dir is None:
        output_dir = source.figures_dir

    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=resolution, output_dir=output_dir)

    if not exceedance_only:
        plot_pwm_convergence_map(grid_config, refresh=refresh)
        plot_pwm_threshold_high_map(grid_config, refresh=refresh)
        plot_pwm_threshold_low_map(grid_config, refresh=refresh)
        plot_pwm_monthly_threshold_maps(grid_config, refresh=refresh)
    if not monthly_only:
        plot_pwm_exceedance_maps(grid_config, p_values=p_values, refresh=refresh)
        plot_pwm_monthly_exceedance_maps(grid_config, p_values=p_values, refresh=refresh)
    plt.close("all")
    typer.echo(f"PWM global maps saved to {output_dir}")


@app.command()
def quantile_global(
    refresh: bool = typer.Option(False, "--refresh"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    resolution: float = typer.Option(0.5, help="Grid resolution"),
    data_dir: Path | None = typer.Option(None, "--data-dir"),
) -> None:
    """Generate global quantile distribution maps."""
    setup_logging("plot-quantile-global")
    import matplotlib.pyplot as plt
    from lakesource.config import SourceConfig
    from lakesource.provider import create_provider
    from lakeviz.config import GlobalGridConfig
    from lakeviz.quantile import plot_quantile_global_maps
    from lakeviz.style.presets import Theme
    Theme.apply()

    source = SourceConfig(data_dir=data_dir) if data_dir else SourceConfig()

    if output_dir is None:
        output_dir = source.figures_dir

    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=resolution, output_dir=output_dir)
    plot_quantile_global_maps(grid_config, refresh=refresh)
    plt.close("all")
    typer.echo(f"Quantile global maps saved to {output_dir}")


@app.command()
def comparison_global(
    refresh: bool = typer.Option(False, "--refresh"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    resolution: float = typer.Option(0.5, help="Grid resolution"),
    gt10_dir: Path | None = typer.Option(None, "--gt10-dir", help="gt10 parquet directory"),
    full_dir: Path | None = typer.Option(None, "--full-dir", help="Full parquet directory"),
    pwm_p1: float = typer.Option(0.01, "--pwm-p1"),
    pwm_p2: float = typer.Option(0.05, "--pwm-p2"),
    eot_q1: float = typer.Option(0.95, "--eot-q1"),
    eot_q2: float = typer.Option(0.98, "--eot-q2"),
) -> None:
    """Generate comparison density panels (incl. gt10 vs full)."""
    setup_logging("plot-comparison-global")
    import matplotlib.pyplot as plt
    from lakesource.config import SourceConfig
    from lakesource.provider import create_provider
    from lakeviz.config import GlobalGridConfig
    from lakeviz.comparison import (
        plot_eot_quantile_panels, plot_gt10_vs_full_panels,
        plot_pwm_pvalue_panels, plot_pwm_vs_eot_panels, plot_quantile_vs_pwm_panels,
    )
    from lakeviz.style.presets import Theme
    Theme.apply()

    source = SourceConfig()

    if output_dir is None:
        output_dir = source.figures_dir
    if gt10_dir is None:
        gt10_dir = source.data_dir.parent / "parquet_gt10"
    if full_dir is None:
        full_dir = source.data_dir

    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=resolution, output_dir=output_dir)

    plot_pwm_pvalue_panels(grid_config, p1=pwm_p1, p2=pwm_p2, refresh=refresh)
    plot_eot_quantile_panels(grid_config, q1=eot_q1, q2=eot_q2, refresh=refresh)
    plot_quantile_vs_pwm_panels(grid_config, refresh=refresh)
    plot_pwm_vs_eot_panels(grid_config, refresh=refresh)
    plot_gt10_vs_full_panels(grid_config, refresh=refresh, gt10_dir=gt10_dir, full_dir=full_dir)
    plt.close("all")
    typer.echo(f"Comparison panels saved to {output_dir}")


@app.command()
def comparison_zonal(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    data_dir: Path | None = typer.Option(None, "--data-dir"),
) -> None:
    """Generate comparison latitude-profile figures."""
    setup_logging("plot-comparison-zonal")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_comparison_zonal import main as _main
    import sys as _sys
    _sys.argv = ["plot_comparison_zonal", "--output-dir", str(output_dir)]
    if data_dir:
        _sys.argv += ["--data-dir", str(data_dir)]
    _main()


# ── Scatter / histogram ─────────────────────────────────────────────────────


@app.command()
def extremes_scatter(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    parquet_dir: Path | None = typer.Option(None, "--parquet-dir", help="Parquet data dir (use parquet_gt10 for gt10)"),
    dpi: int = typer.Option(300, help="Figure DPI"),
    point_size: float = typer.Option(0.8, "--point-size"),
    alpha: float = typer.Option(0.4),
) -> None:
    """Scatter plot of extreme event counts on global Robinson projection."""
    setup_logging("plot-extremes-scatter")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    if parquet_dir is None:
        parquet_dir = SourceConfig().data_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_extremes_scatter import main as _main
    import sys as _sys
    _sys.argv = ["plot_extremes_scatter", "--output-dir", str(output_dir),
                 "--parquet-dir", str(parquet_dir), "--dpi", str(dpi),
                 "--point-size", str(point_size), "--alpha", str(alpha)]
    _main()


@app.command()
def area_histogram(
    limit: int | None = typer.Option(None, "--limit", help="Limit number of lakes"),
    output_dir: str = typer.Option("figure", help="Output directory"),
) -> None:
    """Plot KDE of lake area distribution (lake_info vs area_quality)."""
    setup_logging("plot-area-histogram")
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_area_histogram import main as _main
    import sys as _sys
    _sys.argv = ["plot_area_histogram", "--output-dir", output_dir]
    if limit:
        _sys.argv += ["--limit", str(limit)]
    _main()


@app.command()
def upset(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    min_size: int = typer.Option(5, "--min-size", help="Minimum intersection size to display"),
    limit: int | None = typer.Option(None, "--limit", help="Limit number of lakes"),
) -> None:
    """Generate UpSet + donut anomaly intersection plot."""
    setup_logging("plot-upset")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_anomaly_upset import main as _main
    import sys as _sys
    _sys.argv = ["plot_anomaly_upset", "--output-dir", str(output_dir), "--min-size", str(min_size)]
    if limit:
        _sys.argv += ["--limit", str(limit)]
    _main()


# ── Interpolation ───────────────────────────────────────────────────────────


@app.command()
def interpolation_sample(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    n_samples: int = typer.Option(20, "--n-samples", help="Number of sample lakes"),
) -> None:
    """Plot sample lakes with detected interpolation segments."""
    setup_logging("plot-interpolation-sample")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_interpolation_sample import main as _main
    import sys as _sys
    _sys.argv = ["plot_interpolation_sample", "--output-dir", str(output_dir), "--n-samples", str(n_samples)]
    _main()


@app.command()
def interpolation_hq(
    hylak_ids: str = typer.Option(..., "--hylak-ids", help="Comma-separated lake IDs"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
) -> None:
    """High-quality interpolation figures for specified lakes."""
    setup_logging("plot-interpolation-hq")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_interpolation_hq import main as _main
    import sys as _sys
    _sys.argv = ["plot_interpolation_hq", "--hylak-ids", hylak_ids, "--output-dir", str(output_dir)]
    _main()


# ── Single-lake plots ───────────────────────────────────────────────────────


@app.command()
def eot_extremes(
    hylak_id: int = typer.Option(..., "--hylak-id", help="Lake ID"),
    threshold_quantile: float = typer.Option(0.95, help="Threshold quantile"),
    tail: str | None = typer.Option(None, help="Tail: high or low"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
) -> None:
    """Plot single-lake EOT extremes timeline from database."""
    setup_logging("plot-eot-extremes")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import matplotlib.pyplot as plt
    from lakesource.postgres import fetch_eot_extremes_by_id, fetch_lake_area_by_ids, series_db
    from lakeanalysis.eot import plot_eot_extremes
    from lakeviz.style.presets import Theme
    Theme.apply()

    output_dir.mkdir(parents=True, exist_ok=True)
    with series_db.connection_context() as conn:
        lake_map = fetch_lake_area_by_ids(conn, [hylak_id])
        extremes = fetch_eot_extremes_by_id(conn, hylak_id, threshold_quantile=threshold_quantile, tail=tail)
    series_df = lake_map.get(hylak_id)
    if series_df is None:
        typer.echo(f"Lake {hylak_id} not found"); return
    fig = plot_eot_extremes(hylak_id=hylak_id, series_df=series_df, extremes_df=extremes)
    out_path = output_dir / f"eot_extremes_{hylak_id}.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    typer.echo(f"Saved: {out_path}")


@app.command()
def pwm_hawkes_lake(
    hylak_id: int = typer.Option(..., "--hylak-id", help="Lake ID"),
    annotate_top_n: int = typer.Option(8, help="Top N events to annotate"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
) -> None:
    """Plot single-lake PWM-Hawkes timeline with events and transitions."""
    setup_logging("plot-pwm-hawkes-lake")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_pwm_hawkes_lake import main as _main
    import sys as _sys
    _sys.argv = ["plot_pwm_hawkes_lake", "--hylak-id", str(hylak_id),
                 "--annotate-top-n", str(annotate_top_n), "--output-dir", str(output_dir)]
    _main()


@app.command()
def pwm_hawkes_summary(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
) -> None:
    """Plot PWM-Hawkes summary statistics (QC pie, event histograms, parameters)."""
    setup_logging("plot-pwm-hawkes-summary")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_pwm_hawkes_summary import main as _main
    import sys as _sys
    _sys.argv = ["plot_pwm_hawkes_summary", "--output-dir", str(output_dir)]
    _main()


# ── Shift candidates ────────────────────────────────────────────────────────


@app.command()
def shift_candidates(
    candidate_file: Path = typer.Option(..., "--candidate-file", help="CSV or Parquet with candidate IDs"),
    top_n: int = typer.Option(20, "--top-n", help="Number of candidates to plot"),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
) -> None:
    """Plot time series for structural shift candidate lakes."""
    setup_logging("plot-shift-candidates")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_shift_candidate_lakes import main as _main
    import sys as _sys
    _sys.argv = ["plot_shift_candidate_lakes", "--candidate-file", str(candidate_file),
                 "--top-n", str(top_n), "--output-dir", str(output_dir)]
    _main()


# ── Series quality plots ────────────────────────────────────────────────────


@app.command()
def flat_series(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    top_n: int = typer.Option(20, "--top-n", help="Number of lakes to plot"),
) -> None:
    """Plot time series for flat-flagged lakes."""
    setup_logging("plot-flat-series")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_flat_lake_series import main as _main
    import sys as _sys
    _sys.argv = ["plot_flat_lake_series", "--output-dir", str(output_dir), "--top-n", str(top_n)]
    _main()


@app.command()
def hcv_series(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    top_n: int = typer.Option(20, "--top-n", help="Number of lakes to plot"),
) -> None:
    """Plot time series for low H*CV lakes."""
    setup_logging("plot-hcv-series")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_hcv_lake_series import main as _main
    import sys as _sys
    _sys.argv = ["plot_hcv_lake_series", "--output-dir", str(output_dir), "--top-n", str(top_n)]
    _main()


@app.command()
def hcv_mid_series(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    top_n: int = typer.Option(20, "--top-n", help="Number of lakes to plot"),
) -> None:
    """Plot time series for mid-range H*CV lakes."""
    setup_logging("plot-hcv-mid-series")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_hcv_mid_lake_series import main as _main
    import sys as _sys
    _sys.argv = ["plot_hcv_mid_lake_series", "--output-dir", str(output_dir), "--top-n", str(top_n)]
    _main()


@app.command()
def pv_series(
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Output directory"),
    top_n: int = typer.Option(20, "--top-n", help="Number of lakes to plot"),
) -> None:
    """Plot time series for PV-anomalous lakes."""
    setup_logging("plot-pv-series")
    from lakesource.config import SourceConfig
    if output_dir is None:
        output_dir = SourceConfig().figures_dir
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from plot_pv_lake_series import main as _main
    import sys as _sys
    _sys.argv = ["plot_pv_lake_series", "--output-dir", str(output_dir), "--top-n", str(top_n)]
    _main()
