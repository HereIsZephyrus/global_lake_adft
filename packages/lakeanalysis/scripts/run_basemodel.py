"""Select baseline models for lake monthly time series.

Usage examples:
    uv run python scripts/run_basemodel.py --hylak-id 176595 --plot
    uv run python scripts/run_basemodel.py --limit 5000
    uv run python scripts/run_basemodel.py --all
    uv run python scripts/run_basemodel.py --hylak-id 176595 --criterion bic --plot
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeanalysis.basemodel import (
    BasisSelectionResult,
    BasisSelector,
    plot_basis_fit,
    plot_candidate_scores,
    plot_residuals,
)
from lakeanalysis.dbconnect import fetch_lake_area, fetch_lake_area_by_ids, series_db
from lakeanalysis.eot.preprocess import MonthlyTimeSeries
from lakeanalysis.logger import Logger
from lakeanalysis.plot_config import setup_chinese_font

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "basemodel"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for baseline model selection."""
    parser = argparse.ArgumentParser(
        description="Select baseline models for one, partial, or all lake monthly time series."
    )
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--hylak-id",
        type=int,
        help="Target lake ID to analyze.",
    )
    target_group.add_argument(
        "--limit-id",
        "--limit",
        dest="limit_id",
        type=int,
        default=None,
        help="Process all lakes with hylak_id < N.",
    )
    target_group.add_argument(
        "--all",
        action="store_true",
        help="Process all lakes in lake_area.",
    )
    parser.add_argument(
        "--criterion",
        choices=["aic", "bic"],
        default="aic",
        help="Model selection criterion (default: aic).",
    )
    parser.add_argument(
        "--max-relative-rmse",
        type=float,
        default=1.0,
        help="Fallback threshold for relative RMSE (default: 1.0).",
    )
    parser.add_argument(
        "--no-trend",
        action="store_true",
        help="Disable linear trend in basis-model design matrices.",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate plots (single hylak_id mode only).",
    )
    return parser.parse_args()


def _fetch_series(hylak_id: int) -> pd.DataFrame:
    """Fetch a single lake monthly series from SERIES_DB."""
    with series_db.connection_context() as conn:
        mapping = fetch_lake_area_by_ids(conn, [hylak_id])
    if hylak_id not in mapping:
        raise ValueError(f"No lake_area series found for hylak_id={hylak_id}")
    return mapping[hylak_id]


def _fetch_series_many(limit_id: int | None = None) -> dict[int, pd.DataFrame]:
    """Fetch multiple lake monthly series from SERIES_DB."""
    with series_db.connection_context() as conn:
        return fetch_lake_area(conn, limit_id=limit_id)


def _save_plot(fig: plt.Figure, path: Path) -> None:
    """Save and close a matplotlib figure."""
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _fit_frame(
    series: MonthlyTimeSeries,
    fitted: np.ndarray,
    residuals: np.ndarray,
) -> pd.DataFrame:
    """Build a dataframe with observed values, fitted values and residuals."""
    frame = series.data.loc[:, ["year", "month", "time"]].copy()
    frame["value"] = series.values
    frame["fitted"] = np.asarray(fitted, dtype=float)
    frame["residual"] = np.asarray(residuals, dtype=float)
    return frame


def _summary_payload(
    hylak_id: int,
    selector: BasisSelector,
    selection_result: BasisSelectionResult,
    fit_frame: pd.DataFrame,
) -> dict:
    """Build a serializable summary payload."""
    residual_abs = np.abs(fit_frame["residual"].to_numpy(dtype=float))
    payload = {
        "hylak_id": int(hylak_id),
        "criterion": selector.criterion,
        "include_trend": selector.include_trend,
        "max_relative_rmse": selector.max_relative_rmse,
        "selected_basis": selection_result.selected_basis.model_name,
        "used_fallback": selection_result.used_fallback,
        "fallback_reason": selection_result.fallback_reason,
        "relative_rmse": (
            None if selection_result.relative_rmse is None else float(selection_result.relative_rmse)
        ),
        "n_observations": int(len(fit_frame)),
        "bias_mean": float(fit_frame["residual"].mean()),
        "bias_std": float(fit_frame["residual"].std(ddof=1)),
        "bias_mae": float(np.mean(residual_abs)),
        "bias_p95_abs": float(np.percentile(residual_abs, 95)),
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
            for record in selection_result.candidate_records
        ],
    }
    return payload


def _fit_one_series(
    hylak_id: int,
    df: pd.DataFrame,
    selector: BasisSelector,
) -> tuple[dict, pd.DataFrame, BasisSelectionResult]:
    """Fit one lake series and return summary, fit frame and selection details."""
    series = MonthlyTimeSeries.from_frame(df)
    times = series.data["time"].to_numpy(dtype=float)
    values = series.values
    selection_result = selector.select_result(times, values)
    _, fitted, residuals = selector.fit_basis(times, values, selection_result.selected_basis)
    fit_frame = _fit_frame(series, fitted, residuals)
    summary = _summary_payload(hylak_id, selector, selection_result, fit_frame)
    return summary, fit_frame, selection_result


def _run_single(args: argparse.Namespace, selector: BasisSelector) -> dict:
    """Run baseline-model selection for a single lake."""
    if args.hylak_id is None:
        raise ValueError("hylak_id is required for single-run mode")
    df = _fetch_series(args.hylak_id)
    summary, fit_frame, selection_result = _fit_one_series(args.hylak_id, df, selector)

    output_dir = DATA_DIR / str(args.hylak_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    pd.DataFrame([record.__dict__ for record in selection_result.candidate_records]).to_csv(
        output_dir / "candidates.csv",
        index=False,
    )
    fit_frame.to_csv(output_dir / "fit_series.csv", index=False)

    if args.plot:
        setup_chinese_font()
        _save_plot(
            plot_candidate_scores(
                selection_result.candidate_records,
                selector.criterion,
                selection_result.selected_basis.model_name,
            ),
            output_dir / "candidate_scores.png",
        )
        _save_plot(
            plot_basis_fit(
                fit_frame,
                selected_basis_name=selection_result.selected_basis.model_name,
                criterion=selector.criterion,
                relative_rmse=selection_result.relative_rmse,
            ),
            output_dir / "fit_vs_observed.png",
        )
        _save_plot(plot_residuals(fit_frame), output_dir / "residuals.png")

    if selection_result.used_fallback:
        log.warning(
            "hylak_id=%d basis selection fallback: %s",
            args.hylak_id,
            selection_result.fallback_reason,
        )
    log.info(
        "hylak_id=%d selected_basis=%s criterion=%s relative_rmse=%s",
        args.hylak_id,
        selection_result.selected_basis.model_name,
        selector.criterion,
        (
            "N/A"
            if selection_result.relative_rmse is None
            else f"{selection_result.relative_rmse:.4f}"
        ),
    )
    return summary


def _run_batch(args: argparse.Namespace, selector: BasisSelector) -> dict:
    """Run baseline-model selection for partial/all database data."""
    if args.plot:
        log.warning("Batch mode ignores --plot to avoid creating excessive figures.")
    mode_name = "all" if args.limit_id is None else f"limit_{args.limit_id}"
    output_dir = DATA_DIR / mode_name
    output_dir.mkdir(parents=True, exist_ok=True)

    mapping = _fetch_series_many(limit_id=args.limit_id)
    summaries: list[dict] = []
    residual_rows: list[pd.DataFrame] = []
    failed_rows: list[dict[str, str | int]] = []

    for index, (hylak_id, frame) in enumerate(sorted(mapping.items()), start=1):
        try:
            summary, fit_frame, _ = _fit_one_series(hylak_id, frame, selector)
            summaries.append(summary)
            residual_rows.append(
                fit_frame.loc[:, ["time", "residual"]].assign(hylak_id=hylak_id)
            )
        except ValueError as exc:
            failed_rows.append({"hylak_id": int(hylak_id), "error": str(exc)})
            log.warning("Skip hylak_id=%d due to value error: %s", hylak_id, exc)
        except np.linalg.LinAlgError as exc:
            failed_rows.append({"hylak_id": int(hylak_id), "error": str(exc)})
            log.warning("Skip hylak_id=%d due to linear algebra error: %s", hylak_id, exc)

        if index % 1000 == 0:
            log.info("Processed %d / %d lakes", index, len(mapping))

    summary_frame = pd.DataFrame(summaries)
    summary_cols = [
        "hylak_id",
        "selected_basis",
        "used_fallback",
        "fallback_reason",
        "relative_rmse",
        "bias_mean",
        "bias_std",
        "bias_mae",
        "bias_p95_abs",
        "n_observations",
    ]
    if summary_frame.empty:
        pd.DataFrame(columns=summary_cols).to_csv(
            output_dir / "hylak_basemodel.csv",
            index=False,
        )
    else:
        summary_frame.loc[:, summary_cols].to_csv(output_dir / "hylak_basemodel.csv", index=False)

    if residual_rows:
        residual_frame = pd.concat(residual_rows, ignore_index=True)
    else:
        residual_frame = pd.DataFrame(columns=["hylak_id", "time", "residual"])
    residual_frame.to_csv(output_dir / "hylak_residuals.csv", index=False)

    pd.DataFrame(failed_rows).to_csv(output_dir / "failed_hylak.csv", index=False)
    (output_dir / "batch_summary.json").write_text(
        json.dumps(
            {
                "mode": mode_name,
                "n_total": int(len(mapping)),
                "n_success": int(len(summaries)),
                "n_failed": int(len(failed_rows)),
                "criterion": selector.criterion,
                "include_trend": selector.include_trend,
                "max_relative_rmse": selector.max_relative_rmse,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info(
        "Batch run complete mode=%s success=%d failed=%d",
        mode_name,
        len(summaries),
        len(failed_rows),
    )
    return {
        "mode": mode_name,
        "n_total": len(mapping),
        "n_success": len(summaries),
        "n_failed": len(failed_rows),
    }


def run(args: argparse.Namespace) -> dict:
    """Run baseline-model selection in single or batch mode."""
    selector = BasisSelector(
        criterion=args.criterion,
        include_trend=not args.no_trend,
        max_relative_rmse=args.max_relative_rmse,
    )
    if args.hylak_id is not None:
        return _run_single(args, selector)
    if args.limit_id is not None:
        return _run_batch(args, selector)
    if args.all:
        return _run_batch(args, selector)
    raise ValueError("One of --hylak-id, --limit-id/--limit, or --all must be provided")


def main() -> None:
    """Entry point for CLI execution."""
    args = parse_args()
    Logger("run_basemodel")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
