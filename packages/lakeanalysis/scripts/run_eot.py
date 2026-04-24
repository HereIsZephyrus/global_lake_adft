"""Debug and explore EOT fits for one lake_area monthly series.

Usage examples:
    uv run python scripts/run_eot.py --hylak-id 1023
    uv run python scripts/run_eot.py --hylak-id 1023 --tail low --plot
    uv run python scripts/run_eot.py --hylak-id 1023 --no-decluster --plot
    uv run python scripts/run_eot.py --hylak-id 1023 --threshold 123.4 --run-length 2
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakeanalysis.basemodel import BasisSelectionResult, HarmonicBasis
from lakesource.postgres import (
    fetch_frozen_year_months_by_ids,
    fetch_lake_area_by_ids,
    series_db,
)
from lakeviz.plot_config import setup_chinese_font
from lakeanalysis.eot import (
    BasisSelector,
    EOTEstimator,
    ModelChecker,
    MonthlyTimeSeries,
    NHPPFitter,
    NoDeclustering,
    ReturnLevelEstimator,
    RunsDeclustering,
    plot_extremes_timeline,
    plot_location_model,
    plot_mrl,
    plot_parameter_stability,
    plot_pp,
    plot_qq,
    plot_return_levels,
)
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "eot"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for single-lake EOT debugging."""
    parser = argparse.ArgumentParser(
        description="Fit the EOT NHPP model for one lake and optionally save diagnostics."
    )
    parser.add_argument(
        "--hylak-id",
        type=int,
        required=True,
        help="Target lake ID to fit.",
    )
    parser.add_argument(
        "--tail",
        choices=["high", "low", "both"],
        default="both",
        help="Tail to fit: high, low, or both (default: both).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Manual threshold on the working tail scale. If omitted, use threshold-quantile.",
    )
    parser.add_argument(
        "--threshold-quantile",
        type=float,
        default=0.90,
        help="Automatic threshold quantile if --threshold is omitted (default: 0.90).",
    )
    parser.add_argument(
        "--run-length",
        type=int,
        default=1,
        help="Runs declustering gap length (default: 1).",
    )
    parser.add_argument(
        "--no-decluster",
        action="store_true",
        help="Disable declustering and keep all exceedances.",
    )
    parser.add_argument(
        "--return-periods",
        nargs="+",
        type=float,
        default=[10.0, 25.0, 50.0, 100.0],
        help="Return periods in years for return-level estimation.",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate diagnostic plots under data/eot/<hylak_id>/<tail>/.",
    )
    parser.add_argument(
        "--basis-criterion",
        choices=["aic", "bic"],
        default="aic",
        help="Criterion used to select the baseline model (default: aic).",
    )
    parser.add_argument(
        "--basis-max-relative-rmse",
        type=float,
        default=1.0,
        help="Fallback threshold for baseline relative RMSE (default: 1.0).",
    )
    parser.add_argument(
        "--basis-no-trend",
        action="store_true",
        help="Disable the linear trend term when selecting the baseline model.",
    )
    parser.add_argument(
        "--eot-integration-points",
        type=int,
        default=256,
        help="Integration grid size used by NHPP likelihood (default: 256).",
    )
    parser.add_argument(
        "--eot-max-restarts",
        type=int,
        default=4,
        help="Maximum number of NHPP initial points to try (default: 4).",
    )
    parser.add_argument(
        "--eot-disable-powell-fallback",
        action="store_true",
        help="Disable Powell fallback and use only L-BFGS-B for NHPP fit.",
    )
    return parser.parse_args()


def _build_estimator(args: argparse.Namespace) -> EOTEstimator:
    """Create an estimator with the requested declustering strategy."""
    if args.no_decluster:
        strategy = NoDeclustering()
    else:
        strategy = RunsDeclustering(run_length=args.run_length)
    basis_selector = BasisSelector(
        candidates=(
            HarmonicBasis(n_harmonics=1),
            HarmonicBasis(n_harmonics=2),
            HarmonicBasis(n_harmonics=3),
        ),
        criterion=args.basis_criterion,
        include_trend=not args.basis_no_trend,
        max_relative_rmse=args.basis_max_relative_rmse,
    )
    return EOTEstimator(
        declustering_strategy=strategy,
        basis_selector=basis_selector,
        fitter=NHPPFitter(
            integration_points=args.eot_integration_points,
            max_restarts=args.eot_max_restarts,
            enable_powell_fallback=not args.eot_disable_powell_fallback,
        ),
    )


def _fetch_series(hylak_id: int) -> pd.DataFrame:
    """Fetch a single lake monthly series from SERIES_DB."""
    with series_db.connection_context() as conn:
        mapping = fetch_lake_area_by_ids(conn, [hylak_id])
    if hylak_id not in mapping:
        raise ValueError(f"No lake_area series found for hylak_id={hylak_id}")
    return mapping[hylak_id]


def _fetch_frozen_year_months(hylak_id: int) -> set[int]:
    """Fetch frozen YYYYMM keys for one lake from the anomaly table."""
    with series_db.connection_context() as conn:
        mapping = fetch_frozen_year_months_by_ids(conn, [hylak_id])
    return mapping.get(hylak_id, set())


def _tail_output_dir(hylak_id: int, tail: str) -> Path:
    return DATA_DIR / str(hylak_id) / tail


def _write_summary(
    output_dir: Path,
    summary: dict,
    threshold_diagnostics: dict[str, pd.DataFrame],
    return_levels: pd.DataFrame,
) -> None:
    """Persist fit summary and tabular diagnostics."""
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    threshold_diagnostics["mrl"].to_csv(output_dir / "mrl.csv", index=False)
    threshold_diagnostics["stability"].to_csv(
        output_dir / "stability.csv",
        index=False,
    )
    return_levels.to_csv(output_dir / "return_levels.csv", index=False)


def _save_plot(fig: plt.Figure, path: Path) -> None:
    """Save and close a matplotlib figure."""
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _select_basis(
    df: pd.DataFrame,
    tail: str,
    estimator: EOTEstimator,
    frozen_year_months: set[int] | None = None,
) -> BasisSelectionResult | None:
    """Run basis selection after defrozen preprocessing for summary/logging."""
    if estimator.basis_selector is None:
        return None
    series = estimator._prepare_series(  # pylint: disable=protected-access
        df,
        tail,
        frozen_year_months=frozen_year_months,
    )
    return estimator.basis_selector.select_result(
        series.data["time"].to_numpy(dtype=float),
        series.values,
    )


def _fit_tail(
    df: pd.DataFrame,
    hylak_id: int,
    tail: str,
    args: argparse.Namespace,
) -> dict:
    """Fit one tail and optionally persist diagnostics."""
    estimator = _build_estimator(args)
    frozen_year_months = _fetch_frozen_year_months(hylak_id)
    n_raw_observations = MonthlyTimeSeries.from_frame(df).n_obs
    basis_selection = _select_basis(
        df,
        tail,
        estimator,
        frozen_year_months=frozen_year_months,
    )
    threshold_diagnostics = estimator.threshold_diagnostics(
        df,
        tail=tail,
        frozen_year_months=frozen_year_months,
    )
    output_dir = _tail_output_dir(hylak_id, tail)
    if args.plot:
        setup_chinese_font()
        output_dir.mkdir(parents=True, exist_ok=True)
        _save_plot(plot_mrl(threshold_diagnostics["mrl"]), output_dir / "mrl.png")
        _save_plot(
            plot_parameter_stability(threshold_diagnostics["stability"]),
            output_dir / "stability.png",
        )
    fit_result = estimator.fit(
        df,
        tail=tail,
        threshold=args.threshold,
        threshold_quantile=args.threshold_quantile,
        frozen_year_months=frozen_year_months,
    )
    n_filtered_observations = fit_result.series.n_obs
    if args.plot:
        output_dir.mkdir(parents=True, exist_ok=True)
        _save_plot(
            plot_extremes_timeline(
                fit_result.series,
                fit_result.extremes,
                fit_result.threshold,
                fit_result=fit_result,
            ),
            output_dir / "extremes_timeline.png",
        )
        _save_plot(plot_location_model(fit_result), output_dir / "location_model.png")
        checker = ModelChecker(fit_result)
        _save_plot(plot_pp(checker), output_dir / "probability_plot.png")
        _save_plot(plot_qq(checker), output_dir / "quantile_plot.png")
    return_levels = ReturnLevelEstimator(fit_result).estimate(args.return_periods)
    if args.plot:
        output_dir.mkdir(parents=True, exist_ok=True)
        _save_plot(plot_return_levels(return_levels), output_dir / "return_levels.png")

    summary = {
        "hylak_id": hylak_id,
        "tail": tail,
        "converged": fit_result.converged,
        "message": fit_result.message,
        "threshold": fit_result.threshold,
        "n_extremes": int(len(fit_result.extremes)),
        "defrozen_applied": True,
        "n_frozen_months_total": int(len(frozen_year_months)),
        "n_observations_before_defrozen": int(n_raw_observations),
        "n_observations_after_defrozen": int(n_filtered_observations),
        "n_removed_frozen": int(n_raw_observations - n_filtered_observations),
        "log_likelihood": fit_result.log_likelihood,
        "declustering": (
            "NoDeclustering"
            if args.no_decluster
            else f"RunsDeclustering(run_length={args.run_length})"
        ),
        "basis_model": fit_result.location_model.basis_model.model_name,
        "basis_selection": (
            None
            if basis_selection is None
            else {
                "criterion": basis_selection.criterion,
                "value_scale": float(basis_selection.value_scale),
                "relative_rmse": (
                    None
                    if basis_selection.relative_rmse is None
                    else float(basis_selection.relative_rmse)
                ),
                "used_fallback": basis_selection.used_fallback,
                "fallback_reason": basis_selection.fallback_reason,
                "candidate_records": [
                    {
                        "basis_name": record.basis_name,
                        "rmse": float(record.rmse),
                        "aic": float(record.aic),
                        "bic": float(record.bic),
                        "n_params": int(record.n_params),
                        "converged": bool(record.converged),
                        "message": record.message,
                    }
                    for record in basis_selection.candidate_records
                ],
            }
        ),
        "params": fit_result.params,
    }
    selected_basis_name = (
        fit_result.location_model.basis_model.model_name
        if fit_result.location_model.basis_model is not None
        else "unknown"
    )
    log.info(
        "hylak_id=%d tail=%s basis=%s converged=%s n_obs=%d->%d frozen_total=%d "
        "n_extremes=%d threshold=%.6f",
        hylak_id,
        tail,
        selected_basis_name,
        fit_result.converged,
        n_raw_observations,
        n_filtered_observations,
        len(frozen_year_months),
        len(fit_result.extremes),
        fit_result.threshold,
    )
    if basis_selection is not None:
        log.info(
            "basis_selection criterion=%s relative_rmse=%s fallback=%s",
            basis_selection.criterion,
            (
                "N/A"
                if basis_selection.relative_rmse is None
                else f"{basis_selection.relative_rmse:.4f}"
            ),
            basis_selection.fallback_reason if basis_selection.used_fallback else "none",
        )
    log.info("params=%s", summary["params"])

    _write_summary(output_dir, summary, threshold_diagnostics, return_levels)

    return summary


def run(args: argparse.Namespace) -> list[dict]:
    """Run EOT fitting for one lake on one or both tails."""
    df = _fetch_series(args.hylak_id)
    tails = ["high", "low"] if args.tail == "both" else [args.tail]
    return [_fit_tail(df, args.hylak_id, tail, args) for tail in tails]


def main() -> None:
    """Entry point for CLI execution."""
    args = parse_args()
    Logger("run_eot")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
