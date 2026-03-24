"""Run Hawkes modelling on EOT events for one lake."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakeanalysis.dbconnect import (
    fetch_frozen_year_months_by_ids,
    fetch_lake_area_by_ids,
    series_db,
)
from lakeanalysis.eot import (
    EOTEstimator,
    NoDeclustering,
    plot_extremes_timeline,
    plot_location_model,
)
from lakeanalysis.hawkes import (
    LikelihoodRatioTest,
    TYPE_DRY,
    TYPE_WET,
    build_events_from_eot,
    evaluate_intensity_decomposition,
    fit_full_model,
    fit_restricted_model,
    plot_event_timeline,
    plot_intensity_decomposition,
    plot_kernel_matrix,
    plot_lrt_summary,
    run_model_comparison,
)
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "hawkes"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Hawkes modelling from NoDeclustering EOT events.",
    )
    parser.add_argument(
        "--hylak-id",
        type=int,
        required=True,
        help="Target lake id.",
    )
    parser.add_argument(
        "--threshold-quantile",
        type=float,
        default=0.90,
        help="Quantile for EOT event extraction.",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate Hawkes diagnostic plots.",
    )
    parser.add_argument(
        "--hawkes-window-months",
        type=float,
        default=4.0,
        help="Hard support window in months for Hawkes excitation kernels.",
    )
    return parser.parse_args()


def _fetch_series(hylak_id: int) -> pd.DataFrame:
    """Fetch one lake monthly area series."""
    with series_db.connection_context() as conn:
        mapping = fetch_lake_area_by_ids(conn, [hylak_id])
    if hylak_id not in mapping:
        raise ValueError(f"No lake_area series found for hylak_id={hylak_id}")
    return mapping[hylak_id]


def _fetch_frozen_year_months(hylak_id: int) -> set[int]:
    """Fetch frozen month keys for one lake."""
    with series_db.connection_context() as conn:
        mapping = fetch_frozen_year_months_by_ids(conn, [hylak_id])
    return mapping.get(hylak_id, set())


def _save_plot(fig: plt.Figure, path: Path) -> None:
    """Save and close a matplotlib figure."""
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def run(args: argparse.Namespace) -> dict:
    """Run Hawkes fitting workflow and persist tabular outputs."""
    series_df = _fetch_series(args.hylak_id)
    frozen_year_months = _fetch_frozen_year_months(args.hylak_id)
    event_series = build_events_from_eot(
        data=series_df,
        threshold_quantile=args.threshold_quantile,
        frozen_year_months=frozen_year_months,
    )

    full_fit = fit_full_model(event_series, window_months=args.hawkes_window_months)
    restricted_d_to_w = fit_restricted_model(
        event_series=event_series,
        disabled_edges=[(TYPE_WET, TYPE_DRY)],
        window_months=args.hawkes_window_months,
    )
    restricted_w_to_d = fit_restricted_model(
        event_series=event_series,
        disabled_edges=[(TYPE_DRY, TYPE_WET)],
        window_months=args.hawkes_window_months,
    )

    strategy = LikelihoodRatioTest(significance_level=0.05)
    lrt_d_to_w = run_model_comparison(
        test_name="D_to_W",
        restricted_fit=restricted_d_to_w,
        full_fit=full_fit,
        df=1,
        test_strategy=strategy,
    )
    lrt_w_to_d = run_model_comparison(
        test_name="W_to_D",
        restricted_fit=restricted_w_to_d,
        full_fit=full_fit,
        df=1,
        test_strategy=strategy,
    )

    if event_series.timeline is not None and not event_series.timeline.empty:
        evaluation_times = event_series.timeline["time"].to_numpy(dtype=float)
    else:
        evaluation_times = pd.Series(
            [event_series.start_time, event_series.end_time],
            dtype=float,
        ).to_numpy()
    decomposition = evaluate_intensity_decomposition(
        event_series=event_series,
        fit_result=full_fit,
        evaluation_times=evaluation_times,
        window_years=args.hawkes_window_months / 12.0,
    )

    output_dir = DATA_DIR / str(args.hylak_id)
    plots_dir = output_dir / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.plot:
        plots_dir.mkdir(parents=True, exist_ok=True)

    events_table = (
        event_series.events_table
        if event_series.events_table is not None
        else pd.DataFrame(columns=["time", "event_type", "event_label"])
    )
    lrt_frame = pd.DataFrame(
        [
            {
                "test_name": lrt_d_to_w.test_name,
                "lr_statistic": lrt_d_to_w.lr_statistic,
                "df": lrt_d_to_w.df,
                "p_value": lrt_d_to_w.p_value,
                "significance_level": lrt_d_to_w.significance_level,
                "reject_null": lrt_d_to_w.reject_null,
                "restricted_log_likelihood": lrt_d_to_w.restricted_log_likelihood,
                "full_log_likelihood": lrt_d_to_w.full_log_likelihood,
            },
            {
                "test_name": lrt_w_to_d.test_name,
                "lr_statistic": lrt_w_to_d.lr_statistic,
                "df": lrt_w_to_d.df,
                "p_value": lrt_w_to_d.p_value,
                "significance_level": lrt_w_to_d.significance_level,
                "reject_null": lrt_w_to_d.reject_null,
                "restricted_log_likelihood": lrt_w_to_d.restricted_log_likelihood,
                "full_log_likelihood": lrt_w_to_d.full_log_likelihood,
            },
        ]
    )

    summary = {
        "hylak_id": int(args.hylak_id),
        "threshold_quantile": float(args.threshold_quantile),
        "n_events": int(len(event_series.times)),
        "n_dry_events": int(int((event_series.event_types == TYPE_DRY).sum())),
        "n_wet_events": int(int((event_series.event_types == TYPE_WET).sum())),
        "fit": {
            "converged": bool(full_fit.converged),
            "message": full_fit.message,
            "log_likelihood": float(full_fit.log_likelihood),
            "objective_value": float(full_fit.objective_value),
            "mu": full_fit.mu.tolist(),
            "alpha": full_fit.alpha.tolist(),
            "beta": full_fit.beta.tolist(),
            "spectral_radius": float(full_fit.spectral_radius),
        },
        "lrt_tests": lrt_frame.to_dict(orient="records"),
    }

    (output_dir / "fit_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    events_table.to_csv(output_dir / "events.csv", index=False)
    decomposition.to_csv(output_dir / "intensity_decomposition.csv", index=False)
    lrt_frame.to_csv(output_dir / "lrt.csv", index=False)

    if args.plot:
        _save_plot(plot_event_timeline(events_table), plots_dir / "event_timeline.png")
        _save_plot(
            plot_intensity_decomposition(decomposition),
            plots_dir / "intensity_decomposition.png",
        )
        _save_plot(plot_kernel_matrix(full_fit), plots_dir / "kernel_matrix.png")
        _save_plot(plot_lrt_summary(lrt_frame), plots_dir / "lrt_summary.png")
        # Also export EOT plots in the same folder for side-by-side interpretation.
        eot_estimator = EOTEstimator(declustering_strategy=NoDeclustering())
        for tail in ("high", "low"):
            eot_fit = eot_estimator.fit(
                series_df,
                tail=tail,
                threshold_quantile=args.threshold_quantile,
                frozen_year_months=frozen_year_months,
            )
            _save_plot(
                plot_extremes_timeline(
                    eot_fit.series,
                    eot_fit.extremes,
                    eot_fit.threshold,
                    fit_result=eot_fit,
                ),
                plots_dir / f"eot_{tail}_extremes_timeline.png",
            )
            _save_plot(
                plot_location_model(eot_fit),
                plots_dir / f"eot_{tail}_location_model.png",
            )

    return summary


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    Logger("run_hawkes")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    summary = run(args)
    log.info(
        "hylak_id=%d converged=%s n_events=%d ll=%.4f",
        summary["hylak_id"],
        summary["fit"]["converged"],
        summary["n_events"],
        summary["fit"]["log_likelihood"],
    )


if __name__ == "__main__":
    main()

