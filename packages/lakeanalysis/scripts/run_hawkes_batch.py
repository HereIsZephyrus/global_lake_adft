"""Batch Hawkes fitting with chunk-level parallelism and DB persistence.

This workflow:
  1. Reads monthly lake series and frozen-month marks by chunk.
  2. Runs EOT (NoDeclustering) and quality controls.
  3. Runs Hawkes only for QC-passed tasks.
  4. Persists DB rows (EOT + Hawkes), and optionally file artifacts.

Usage examples:
    uv run python scripts/run_hawkes_batch.py
    uv run python scripts/run_hawkes_batch.py --workers 8 --chunk-size 5000
    uv run python scripts/run_hawkes_batch.py --threshold-quantiles 0.95 0.98
    uv run python scripts/run_hawkes_batch.py --to-file --plot-mode all --limit-id 50000
"""

from __future__ import annotations

import argparse
import concurrent.futures
from decimal import Decimal
import json
import logging
import math
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakesource.postgres import (
    ensure_eot_results_table,
    ensure_hawkes_results_table,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_eot_extremes,
    upsert_eot_results,
    upsert_hawkes_lrt,
    upsert_hawkes_results,
    upsert_hawkes_transition_monthly,
)
from lakeanalysis.eot import (
    EOTEstimator,
    NHPPFitter,
    NoDeclustering,
    plot_extremes_timeline,
    plot_location_model,
)
from lakeanalysis.hawkes import (
    HawkesEventSeries,
    LikelihoodRatioTest,
    TYPE_DRY,
    TYPE_LABELS,
    TYPE_WET,
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

matplotlib.use("Agg")

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "hawkes" / "batch"


def _save_plot(fig: plt.Figure, path: Path) -> None:
    """Save and close a matplotlib figure."""
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _split_evenly(items: list, n: int) -> list[list]:
    """Split items into at most n roughly equal groups."""
    if not items:
        return []
    n = min(n, len(items))
    sub_size = math.ceil(len(items) / n)
    return [items[i : i + sub_size] for i in range(0, len(items), sub_size)]


def _iter_chunks(chunk_size: int, limit_id: int | None) -> list[tuple[int, int]]:
    """Return [start, end) chunk ranges over hylak_id."""
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(hylak_id) FROM lake_info")
            row = cur.fetchone()
    max_id = int(row[0]) if row and row[0] is not None else 0
    if limit_id is not None:
        max_id = min(max_id, limit_id - 1)
    chunks: list[tuple[int, int]] = []
    for start in range(0, max_id + 1, chunk_size):
        end = start + chunk_size
        if limit_id is not None:
            end = min(end, limit_id)
        chunks.append((start, end))
    return chunks


def _quantile_string(quantile: float) -> str:
    """Return deterministic decimal string for quantile keys."""
    return str(Decimal(str(quantile)))


def _quantile_tag(quantile: float) -> str:
    """Return filesystem-safe quantile tag."""
    return f"q_{quantile:.4f}"


def _load_completed_keys(manifest_path: Path) -> set[tuple[int, str]]:
    """Load completed task keys from manifest JSONL."""
    done: set[tuple[int, str]] = set()
    if not manifest_path.exists():
        return done
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("status") != "done":
            continue
        hid = item.get("hylak_id")
        q = item.get("threshold_quantile")
        if hid is None or q is None:
            continue
        done.add((int(hid), str(q)))
    return done


def _append_jsonl(path: Path, records: list[dict]) -> None:
    """Append records to JSONL."""
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _append_csv(path: Path, rows: list[dict]) -> None:
    """Append dict rows to CSV with automatic header handling."""
    if not rows:
        return
    frame = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    frame.to_csv(path, mode="a", header=write_header, index=False)


def _build_hawkes_events_from_eot_fits(
    fit_high,
    fit_low,
) -> tuple[HawkesEventSeries, pd.DataFrame]:
    """Convert two EOT fits (high/low) to one dual-type Hawkes event series."""
    wet_events = fit_high.extremes.loc[
        :,
        ["time", "year", "month", "value", "original_value", "threshold"],
    ].copy()
    wet_events["event_label"] = TYPE_LABELS[TYPE_WET]
    wet_events["event_type"] = TYPE_WET

    dry_events = fit_low.extremes.loc[
        :,
        ["time", "year", "month", "value", "original_value", "threshold"],
    ].copy()
    dry_events["event_label"] = TYPE_LABELS[TYPE_DRY]
    dry_events["event_type"] = TYPE_DRY

    events_table = pd.concat([dry_events, wet_events], ignore_index=True)
    if not events_table.empty:
        events_table = events_table.sort_values(
            ["time", "event_type"],
            ascending=[True, True],
        ).reset_index(drop=True)
    timeline = fit_high.series.data.loc[:, ["year", "month", "time"]].copy().reset_index(drop=True)
    start_time = float(timeline["time"].min())
    end_time = float(timeline["time"].max() + 1.0 / 12.0)
    event_series = HawkesEventSeries(
        times=events_table["time"].to_numpy(dtype=float),
        event_types=events_table["event_type"].to_numpy(dtype=int),
        start_time=start_time,
        end_time=end_time,
        timeline=timeline,
        events_table=events_table,
    )
    return event_series, events_table


def _eot_result_row(
    hylak_id: int,
    tail: str,
    threshold_quantile: float,
    frozen_year_months: set[int],
    fit_result,
) -> dict:
    """Build one eot_results row from a fit result."""
    params = fit_result.params
    ll = fit_result.log_likelihood
    return {
        "hylak_id": int(hylak_id),
        "tail": tail,
        "threshold_quantile": _quantile_string(threshold_quantile),
        "converged": bool(fit_result.converged),
        "log_likelihood": float(ll) if np.isfinite(ll) else None,
        "threshold": float(fit_result.threshold),
        "n_extremes": int(len(fit_result.extremes)),
        "n_observations": int(fit_result.series.n_obs),
        "n_frozen_months": int(len(frozen_year_months)),
        "beta0": params.get("beta0"),
        "beta1": params.get("beta1"),
        "sin_1": params.get("sin_1"),
        "cos_1": params.get("cos_1"),
        "sigma": params.get("sigma"),
        "xi": params.get("xi"),
        "error_message": None,
    }


def _eot_extreme_rows(
    hylak_id: int,
    tail: str,
    threshold_quantile: float,
    extremes: pd.DataFrame,
) -> list[dict]:
    """Build eot_extremes rows from fit_result.extremes."""
    q_str = _quantile_string(threshold_quantile)
    return [
        {
            "hylak_id": int(hylak_id),
            "tail": tail,
            "threshold_quantile": q_str,
            "cluster_id": int(row["cluster_id"]),
            "cluster_size": int(row["cluster_size"]),
            "year": int(row["year"]),
            "month": int(row["month"]),
            "water_area": float(row["original_value"]),
            "threshold_at_event": float(row["threshold"]),
        }
        for _, row in extremes.iterrows()
    ]


def _qc_metrics(
    series_df: pd.DataFrame,
    event_series: HawkesEventSeries,
    events_table: pd.DataFrame,
    min_exceedance_rate: float,
    max_exceedance_rate: float,
    min_relative_amplitude: float,
    min_median_excess: float,
) -> tuple[dict, bool]:
    """Compute and evaluate Hawkes-entry quality metrics."""
    n_obs = len(series_df)
    n_events = len(event_series.times)
    exceedance_rate = float(n_events / n_obs) if n_obs > 0 else float("nan")
    values = pd.to_numeric(series_df["water_area"], errors="coerce").dropna().to_numpy(dtype=float)
    if values.size > 0:
        p95 = float(np.quantile(values, 0.95))
        p05 = float(np.quantile(values, 0.05))
        median = float(np.median(values))
        relative_amplitude = float((p95 - p05) / max(abs(median), 1.0))
    else:
        relative_amplitude = float("nan")

    if events_table.empty:
        median_excess = float("nan")
    else:
        excess = np.abs(
            pd.to_numeric(events_table["value"], errors="coerce").to_numpy(dtype=float)
            - pd.to_numeric(events_table["threshold"], errors="coerce").to_numpy(dtype=float)
        )
        median_excess = float(np.median(excess)) if excess.size > 0 else float("nan")

    qc = {
        "qc_exceedance_rate": exceedance_rate,
        "qc_relative_amplitude": relative_amplitude,
        "qc_median_excess": median_excess,
        "qc_pass_exceedance_rate": bool(
            np.isfinite(exceedance_rate)
            and min_exceedance_rate <= exceedance_rate <= max_exceedance_rate
        ),
        "qc_pass_relative_amplitude": bool(
            np.isfinite(relative_amplitude) and relative_amplitude >= min_relative_amplitude
        ),
        "qc_pass_median_excess": bool(
            np.isfinite(median_excess) and median_excess >= min_median_excess
        ),
    }
    qc_pass = bool(
        qc["qc_pass_exceedance_rate"]
        and qc["qc_pass_relative_amplitude"]
        and qc["qc_pass_median_excess"]
    )
    return qc, qc_pass


def _hawkes_result_row(summary: dict) -> dict:
    """Map summary dict to hawkes_results upsert schema."""
    return {
        "hylak_id": int(summary["hylak_id"]),
        "threshold_quantile": _quantile_string(float(summary["threshold_quantile"])),
        "converged": summary.get("converged"),
        "log_likelihood": summary.get("log_likelihood"),
        "objective_value": summary.get("objective_value"),
        "n_events": summary.get("n_events"),
        "n_dry_events": summary.get("n_dry_events"),
        "n_wet_events": summary.get("n_wet_events"),
        "mu_d": summary.get("mu_D"),
        "mu_w": summary.get("mu_W"),
        "alpha_dd": summary.get("alpha_DD"),
        "alpha_dw": summary.get("alpha_DW"),
        "alpha_wd": summary.get("alpha_WD"),
        "alpha_ww": summary.get("alpha_WW"),
        "beta_dd": summary.get("beta_DD"),
        "beta_dw": summary.get("beta_DW"),
        "beta_wd": summary.get("beta_WD"),
        "beta_ww": summary.get("beta_WW"),
        "spectral_radius": summary.get("spectral_radius"),
        "lrt_p_d_to_w": summary.get("lrt_p_D_to_W"),
        "lrt_p_w_to_d": summary.get("lrt_p_W_to_D"),
        "qc_pass": summary.get("qc_pass"),
        "qc_exceedance_rate": summary.get("qc_exceedance_rate"),
        "qc_relative_amplitude": summary.get("qc_relative_amplitude"),
        "qc_median_excess": summary.get("qc_median_excess"),
        "error_message": summary.get("error_message"),
    }


def _hawkes_transition_monthly_rows(
    hylak_id: int,
    threshold_quantile: float,
    decomposition: pd.DataFrame,
    timeline: pd.DataFrame,
    significance_quantile: float,
) -> list[dict]:
    """Build monthly D->W/W->D significance rows from intensity decomposition."""
    if decomposition.empty or timeline.empty:
        return []
    if not (0.0 < significance_quantile < 1.0):
        raise ValueError("significance_quantile must be in (0, 1)")

    joined = timeline.loc[:, ["year", "month", "time"]].merge(
        decomposition.loc[:, ["time", "cross_D", "cross_W", "lambda_D", "lambda_W"]],
        on="time",
        how="inner",
    )
    if joined.empty:
        return []

    joined["score_raw_D_to_W"] = pd.to_numeric(joined["cross_W"], errors="coerce")
    joined["score_raw_W_to_D"] = pd.to_numeric(joined["cross_D"], errors="coerce")
    joined["score_norm_D_to_W"] = joined["score_raw_D_to_W"] / pd.to_numeric(
        joined["lambda_W"], errors="coerce"
    ).replace(0.0, np.nan)
    joined["score_norm_W_to_D"] = joined["score_raw_W_to_D"] / pd.to_numeric(
        joined["lambda_D"], errors="coerce"
    ).replace(0.0, np.nan)

    rows: list[dict] = []
    q_str = _quantile_string(threshold_quantile)
    for direction, score_raw_col, score_norm_col in (
        ("D_to_W", "score_raw_D_to_W", "score_norm_D_to_W"),
        ("W_to_D", "score_raw_W_to_D", "score_norm_W_to_D"),
    ):
        valid_norm = joined[score_norm_col].dropna().to_numpy(dtype=float)
        threshold = (
            float(np.quantile(valid_norm, significance_quantile))
            if valid_norm.size > 0
            else float("nan")
        )
        for _, row in joined.iterrows():
            score_raw = row[score_raw_col]
            score_norm = row[score_norm_col]
            is_significant = bool(
                np.isfinite(score_norm)
                and np.isfinite(threshold)
                and float(score_norm) >= threshold
            )
            rows.append(
                {
                    "hylak_id": int(hylak_id),
                    "threshold_quantile": q_str,
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "direction": direction,
                    "score_raw": float(score_raw) if np.isfinite(score_raw) else None,
                    "score_norm": float(score_norm) if np.isfinite(score_norm) else None,
                    "significance_quantile": float(significance_quantile),
                    "significance_threshold": float(threshold) if np.isfinite(threshold) else None,
                    "significant": is_significant,
                }
            )
    return rows


def _fit_one_task(
    hylak_id: int,
    series_df: pd.DataFrame,
    frozen_year_months: set[int],
    threshold_quantile: float,
    output_root: str,
    plot_mode: str,
    min_exceedance_rate: float,
    max_exceedance_rate: float,
    min_relative_amplitude: float,
    min_median_excess: float,
    to_file: bool,
    hawkes_window_months: float,
    eot_integration_points: int,
    eot_max_restarts: int,
    eot_enable_powell_fallback: bool,
    monthly_significance_quantile: float,
) -> tuple[dict, list[dict], dict, list[dict], list[dict], list[dict], list[dict]]:
    """Run one (lake, quantile) task and save per-lake artifacts."""
    output_base = Path(output_root)
    q_str = _quantile_string(threshold_quantile)
    lake_dir = output_base / "lakes" / str(hylak_id) / _quantile_tag(threshold_quantile)
    plots_dir = lake_dir / "plots"
    if to_file:
        lake_dir.mkdir(parents=True, exist_ok=True)
        if plot_mode != "none":
            plots_dir.mkdir(parents=True, exist_ok=True)

    manifest_record = {
        "hylak_id": int(hylak_id),
        "threshold_quantile": q_str,
        "status": "done",
        "error_message": None,
    }
    try:
        eot_estimator = EOTEstimator(
            declustering_strategy=NoDeclustering(),
            fitter=NHPPFitter(
                integration_points=eot_integration_points,
                max_restarts=eot_max_restarts,
                enable_powell_fallback=eot_enable_powell_fallback,
            ),
        )
        fit_high, fit_low = eot_estimator.fit_both_tails(
            series_df,
            threshold_quantile=threshold_quantile,
            frozen_year_months=frozen_year_months,
        )
        eot_result_rows = [
            _eot_result_row(
                hylak_id=hylak_id,
                tail="high",
                threshold_quantile=threshold_quantile,
                frozen_year_months=frozen_year_months,
                fit_result=fit_high,
            ),
            _eot_result_row(
                hylak_id=hylak_id,
                tail="low",
                threshold_quantile=threshold_quantile,
                frozen_year_months=frozen_year_months,
                fit_result=fit_low,
            ),
        ]
        eot_extreme_rows = _eot_extreme_rows(
            hylak_id=hylak_id,
            tail="high",
            threshold_quantile=threshold_quantile,
            extremes=fit_high.extremes,
        ) + _eot_extreme_rows(
            hylak_id=hylak_id,
            tail="low",
            threshold_quantile=threshold_quantile,
            extremes=fit_low.extremes,
        )
        event_series, events_table = _build_hawkes_events_from_eot_fits(fit_high, fit_low)
        qc, qc_pass = _qc_metrics(
            series_df=series_df,
            event_series=event_series,
            events_table=events_table,
            min_exceedance_rate=min_exceedance_rate,
            max_exceedance_rate=max_exceedance_rate,
            min_relative_amplitude=min_relative_amplitude,
            min_median_excess=min_median_excess,
        )
        if not qc_pass:
            message = (
                "QC failed before Hawkes fit: "
                f"rate={qc['qc_exceedance_rate']:.4f}, "
                f"rel_amp={qc['qc_relative_amplitude']:.6f}, "
                f"median_excess={qc['qc_median_excess']:.6f}"
            )
            summary = {
                "hylak_id": int(hylak_id),
                "threshold_quantile": float(threshold_quantile),
                "converged": False,
                "message": message,
                "n_events": int(len(event_series.times)),
                "n_dry_events": int((event_series.event_types == TYPE_DRY).sum()),
                "n_wet_events": int((event_series.event_types == TYPE_WET).sum()),
                "log_likelihood": None,
                "objective_value": None,
                "mu_D": None,
                "mu_W": None,
                "alpha_DD": None,
                "alpha_DW": None,
                "alpha_WD": None,
                "alpha_WW": None,
                "beta_DD": None,
                "beta_DW": None,
                "beta_WD": None,
                "beta_WW": None,
                "spectral_radius": None,
                "lrt_p_D_to_W": None,
                "lrt_p_W_to_D": None,
                "qc_pass": False,
                **qc,
                "output_dir": str(lake_dir),
                "error_message": message,
            }
            summary_json = {
                "hylak_id": summary["hylak_id"],
                "threshold_quantile": summary["threshold_quantile"],
                "qc": qc,
                "message": message,
            }
            if to_file:
                (lake_dir / "fit_summary.json").write_text(
                    json.dumps(summary_json, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                events_table.to_csv(lake_dir / "events.csv", index=False)
            manifest_record["status"] = "done"
            manifest_record["error_message"] = message
            return (
                summary,
                [],
                manifest_record,
                eot_result_rows,
                eot_extreme_rows,
                [],
                [],
            )
        full_fit = fit_full_model(event_series, window_months=hawkes_window_months)
        restricted_d_to_w = fit_restricted_model(
            event_series=event_series,
            disabled_edges=[(TYPE_WET, TYPE_DRY)],
            window_months=hawkes_window_months,
        )
        restricted_w_to_d = fit_restricted_model(
            event_series=event_series,
            disabled_edges=[(TYPE_DRY, TYPE_WET)],
            window_months=hawkes_window_months,
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
            window_years=hawkes_window_months / 12.0,
        )
        lrt_frame = pd.DataFrame(
            [
                {
                    "hylak_id": int(hylak_id),
                    "threshold_quantile": q_str,
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
                    "hylak_id": int(hylak_id),
                    "threshold_quantile": q_str,
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
            "hylak_id": int(hylak_id),
            "threshold_quantile": float(threshold_quantile),
            "converged": bool(full_fit.converged),
            "message": full_fit.message,
            "n_events": int(len(event_series.times)),
            "n_dry_events": int((event_series.event_types == TYPE_DRY).sum()),
            "n_wet_events": int((event_series.event_types == TYPE_WET).sum()),
            "log_likelihood": float(full_fit.log_likelihood),
            "objective_value": float(full_fit.objective_value),
            "mu_D": float(full_fit.mu[TYPE_DRY]),
            "mu_W": float(full_fit.mu[TYPE_WET]),
            "alpha_DD": float(full_fit.alpha[TYPE_DRY, TYPE_DRY]),
            "alpha_DW": float(full_fit.alpha[TYPE_DRY, TYPE_WET]),
            "alpha_WD": float(full_fit.alpha[TYPE_WET, TYPE_DRY]),
            "alpha_WW": float(full_fit.alpha[TYPE_WET, TYPE_WET]),
            "beta_DD": float(full_fit.beta[TYPE_DRY, TYPE_DRY]),
            "beta_DW": float(full_fit.beta[TYPE_DRY, TYPE_WET]),
            "beta_WD": float(full_fit.beta[TYPE_WET, TYPE_DRY]),
            "beta_WW": float(full_fit.beta[TYPE_WET, TYPE_WET]),
            "spectral_radius": float(full_fit.spectral_radius),
            "lrt_p_D_to_W": float(lrt_d_to_w.p_value),
            "lrt_p_W_to_D": float(lrt_w_to_d.p_value),
            "qc_pass": True,
            **qc,
            "output_dir": str(lake_dir),
            "error_message": None,
        }
        monthly_rows = _hawkes_transition_monthly_rows(
            hylak_id=hylak_id,
            threshold_quantile=threshold_quantile,
            decomposition=decomposition,
            timeline=event_series.timeline if event_series.timeline is not None else pd.DataFrame(),
            significance_quantile=monthly_significance_quantile,
        )
        summary_json = {
            "hylak_id": summary["hylak_id"],
            "threshold_quantile": summary["threshold_quantile"],
            "n_events": summary["n_events"],
            "n_dry_events": summary["n_dry_events"],
            "n_wet_events": summary["n_wet_events"],
            "fit": {
                "converged": summary["converged"],
                "message": summary["message"],
                "log_likelihood": summary["log_likelihood"],
                "objective_value": summary["objective_value"],
                "mu": [summary["mu_D"], summary["mu_W"]],
                "alpha": [
                    [summary["alpha_DD"], summary["alpha_DW"]],
                    [summary["alpha_WD"], summary["alpha_WW"]],
                ],
                "beta": [
                    [summary["beta_DD"], summary["beta_DW"]],
                    [summary["beta_WD"], summary["beta_WW"]],
                ],
                "spectral_radius": summary["spectral_radius"],
            },
            "lrt_tests": lrt_frame.drop(
                columns=["hylak_id", "threshold_quantile"]
            ).to_dict(orient="records"),
            "monthly_significance_quantile": monthly_significance_quantile,
        }

        if to_file:
            (lake_dir / "fit_summary.json").write_text(
                json.dumps(summary_json, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            events_table.to_csv(lake_dir / "events.csv", index=False)
            decomposition.to_csv(lake_dir / "intensity_decomposition.csv", index=False)
            lrt_frame.to_csv(lake_dir / "lrt.csv", index=False)

        should_plot = to_file and plot_mode == "all"
        if should_plot:
            _save_plot(plot_event_timeline(events_table), plots_dir / "event_timeline.png")
            _save_plot(
                plot_intensity_decomposition(decomposition),
                plots_dir / "intensity_decomposition.png",
            )
            _save_plot(plot_kernel_matrix(full_fit), plots_dir / "kernel_matrix.png")
            _save_plot(plot_lrt_summary(lrt_frame), plots_dir / "lrt_summary.png")
            for tail, eot_fit in (("high", fit_high), ("low", fit_low)):
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
        return (
            summary,
            lrt_frame.to_dict(orient="records"),
            manifest_record,
            eot_result_rows,
            eot_extreme_rows,
            [_hawkes_result_row(summary)],
            monthly_rows,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        manifest_record["status"] = "failed"
        manifest_record["error_message"] = str(exc)[:500]
        error_summary = {
            "hylak_id": int(hylak_id),
            "threshold_quantile": float(threshold_quantile),
            "converged": False,
            "message": str(exc),
            "n_events": None,
            "n_dry_events": None,
            "n_wet_events": None,
            "log_likelihood": None,
            "objective_value": None,
            "mu_D": None,
            "mu_W": None,
            "alpha_DD": None,
            "alpha_DW": None,
            "alpha_WD": None,
            "alpha_WW": None,
            "beta_DD": None,
            "beta_DW": None,
            "beta_WD": None,
            "beta_WW": None,
            "spectral_radius": None,
            "lrt_p_D_to_W": None,
            "lrt_p_W_to_D": None,
            "qc_pass": None,
            "qc_exceedance_rate": None,
            "qc_relative_amplitude": None,
            "qc_median_excess": None,
            "qc_pass_exceedance_rate": None,
            "qc_pass_relative_amplitude": None,
            "qc_pass_median_excess": None,
            "output_dir": str(lake_dir),
            "error_message": manifest_record["error_message"],
        }
        if to_file:
            (lake_dir / "fit_summary.json").write_text(
                json.dumps(error_summary, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        return error_summary, [], manifest_record, [], [], [_hawkes_result_row(error_summary)], []


def _process_sub_batch(
    sub_batch: list[tuple[int, pd.DataFrame, set[int], float]],
    output_root: str,
    plot_mode: str,
    min_exceedance_rate: float,
    max_exceedance_rate: float,
    min_relative_amplitude: float,
    min_median_excess: float,
    to_file: bool,
    hawkes_window_months: float,
    eot_integration_points: int,
    eot_max_restarts: int,
    eot_enable_powell_fallback: bool,
    monthly_significance_quantile: float,
) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict], list[dict], list[dict]]:
    """Process one worker sub-batch."""
    summaries: list[dict] = []
    lrt_rows: list[dict] = []
    manifest_rows: list[dict] = []
    eot_result_rows: list[dict] = []
    eot_extreme_rows: list[dict] = []
    hawkes_rows: list[dict] = []
    monthly_rows: list[dict] = []
    for hylak_id, series_df, frozen_year_months, threshold_quantile in sub_batch:
        summary, lrt, manifest, eot_results, eot_extremes, hawkes_result_rows, monthly_transition_rows = _fit_one_task(
            hylak_id=hylak_id,
            series_df=series_df,
            frozen_year_months=frozen_year_months,
            threshold_quantile=threshold_quantile,
            output_root=output_root,
            plot_mode=plot_mode,
            min_exceedance_rate=min_exceedance_rate,
            max_exceedance_rate=max_exceedance_rate,
            min_relative_amplitude=min_relative_amplitude,
            min_median_excess=min_median_excess,
            to_file=to_file,
            hawkes_window_months=hawkes_window_months,
            eot_integration_points=eot_integration_points,
            eot_max_restarts=eot_max_restarts,
            eot_enable_powell_fallback=eot_enable_powell_fallback,
            monthly_significance_quantile=monthly_significance_quantile,
        )
        summaries.append(summary)
        lrt_rows.extend(lrt)
        manifest_rows.append(manifest)
        eot_result_rows.extend(eot_results)
        eot_extreme_rows.extend(eot_extremes)
        hawkes_rows.extend(hawkes_result_rows)
        monthly_rows.extend(monthly_transition_rows)
    return (
        summaries,
        lrt_rows,
        manifest_rows,
        eot_result_rows,
        eot_extreme_rows,
        hawkes_rows,
        monthly_rows,
    )


def _build_tasks(
    lake_map: dict[int, pd.DataFrame],
    frozen_map: dict[int, set[int]],
    quantiles: list[float],
    done_keys: set[tuple[int, str]],
) -> list[tuple[int, pd.DataFrame, set[int], float]]:
    """Build pending task tuples for one chunk."""
    tasks: list[tuple[int, pd.DataFrame, set[int], float]] = []
    for hylak_id, df in lake_map.items():
        frozen = frozen_map.get(hylak_id, set())
        for quantile in quantiles:
            key = (int(hylak_id), _quantile_string(quantile))
            if key in done_keys:
                continue
            tasks.append((int(hylak_id), df, frozen, quantile))
    return tasks


def run(args: argparse.Namespace) -> None:
    """Run batch Hawkes workflow."""
    if args.to_file:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    summary_csv = DATA_DIR / "summary.csv"
    lrt_csv = DATA_DIR / "lrt.csv"
    failures_csv = DATA_DIR / "failures.csv"
    manifest_jsonl = DATA_DIR / "manifest.jsonl"
    done_keys = _load_completed_keys(manifest_jsonl) if args.to_file else set()

    quantiles = args.threshold_quantiles
    with series_db.connection_context() as conn:
        ensure_eot_results_table(conn)
        ensure_hawkes_results_table(conn)
    chunks = _iter_chunks(args.chunk_size, args.limit_id)
    total_chunks = len(chunks)
    log.info(
        "Starting batch Hawkes: chunks=%d chunk_size=%d workers=%d quantiles=%s plot_mode=%s "
        "hawkes_window_months=%.2f eot_integration_points=%d eot_max_restarts=%d "
        "powell_fallback=%s monthly_significance_q=%.3f",
        total_chunks,
        args.chunk_size,
        args.workers,
        quantiles,
        args.plot_mode,
        args.hawkes_window_months,
        args.eot_integration_points,
        args.eot_max_restarts,
        not args.eot_disable_powell_fallback,
        args.monthly_significance_quantile,
    )

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        for idx, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            with series_db.connection_context() as conn:
                lake_map = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
                frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)
            if not lake_map:
                log.debug("[%d/%d] chunk %d-%d: empty, skipping", idx, total_chunks, chunk_start, chunk_end - 1)
                continue
            tasks = _build_tasks(lake_map, frozen_map, quantiles, done_keys)
            if not tasks:
                log.debug("[%d/%d] chunk %d-%d: already done", idx, total_chunks, chunk_start, chunk_end - 1)
                continue

            log.info(
                "[%d/%d] chunk %d-%d: processing %d task(s)",
                idx,
                total_chunks,
                chunk_start,
                chunk_end - 1,
                len(tasks),
            )
            sub_batches = _split_evenly(tasks, args.workers)
            futures = [
                executor.submit(
                    _process_sub_batch,
                    sub_batch,
                    str(DATA_DIR),
                    args.plot_mode,
                    args.min_exceedance_rate,
                    args.max_exceedance_rate,
                    args.min_relative_amplitude,
                    args.min_median_excess,
                    args.to_file,
                    args.hawkes_window_months,
                    args.eot_integration_points,
                    args.eot_max_restarts,
                    not args.eot_disable_powell_fallback,
                    args.monthly_significance_quantile,
                )
                for sub_batch in sub_batches
            ]
            concurrent.futures.wait(futures)

            chunk_summaries: list[dict] = []
            chunk_lrt_rows: list[dict] = []
            chunk_manifest_rows: list[dict] = []
            chunk_eot_result_rows: list[dict] = []
            chunk_eot_extreme_rows: list[dict] = []
            chunk_hawkes_rows: list[dict] = []
            chunk_monthly_rows: list[dict] = []
            for future in futures:
                (
                    summaries,
                    lrt_rows,
                    manifest_rows,
                    eot_result_rows,
                    eot_extreme_rows,
                    hawkes_rows,
                    monthly_rows,
                ) = future.result()
                chunk_summaries.extend(summaries)
                chunk_lrt_rows.extend(lrt_rows)
                chunk_manifest_rows.extend(manifest_rows)
                chunk_eot_result_rows.extend(eot_result_rows)
                chunk_eot_extreme_rows.extend(eot_extreme_rows)
                chunk_hawkes_rows.extend(hawkes_rows)
                chunk_monthly_rows.extend(monthly_rows)

            failures = [row for row in chunk_summaries if row.get("error_message")]
            if args.to_file:
                _append_csv(summary_csv, chunk_summaries)
                _append_csv(lrt_csv, chunk_lrt_rows)
                _append_csv(failures_csv, failures)
                _append_jsonl(manifest_jsonl, chunk_manifest_rows)
            with series_db.connection_context() as conn:
                if chunk_eot_result_rows:
                    upsert_eot_results(conn, chunk_eot_result_rows)
                if chunk_eot_extreme_rows:
                    upsert_eot_extremes(conn, chunk_eot_extreme_rows)
                if chunk_hawkes_rows:
                    upsert_hawkes_results(conn, chunk_hawkes_rows)
                if chunk_lrt_rows:
                    upsert_hawkes_lrt(conn, chunk_lrt_rows)
                if chunk_monthly_rows:
                    upsert_hawkes_transition_monthly(conn, chunk_monthly_rows)
            if args.to_file:
                for manifest in chunk_manifest_rows:
                    if manifest.get("status") == "done":
                        done_keys.add(
                            (int(manifest["hylak_id"]), str(manifest["threshold_quantile"]))
                        )

            log.info(
                "[%d/%d] chunk %d-%d: done (%d summary row(s), %d lrt row(s), %d failure(s))",
                idx,
                total_chunks,
                chunk_start,
                chunk_end - 1,
                len(chunk_summaries),
                len(chunk_lrt_rows),
                len(failures),
            )

    log.info("Batch Hawkes complete. Outputs under: %s", DATA_DIR)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for batch Hawkes."""
    parser = argparse.ArgumentParser(
        description="Batch Hawkes fitting with file outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of worker processes per chunk.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="Consecutive hylak_id count processed per chunk.",
    )
    parser.add_argument(
        "--threshold-quantiles",
        nargs="+",
        type=float,
        default=[0.95],
        help="One or more quantiles for EOT event extraction.",
    )
    parser.add_argument(
        "--plot-mode",
        choices=["none", "all"],
        default="none",
        help="Whether to render plots for each task.",
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        help="Only process hylak_id < limit_id for smoke tests.",
    )
    parser.add_argument(
        "--to-file",
        action="store_true",
        help="Persist file artifacts (CSV/JSON/plots). Default is DB-only mode.",
    )
    parser.add_argument(
        "--min-exceedance-rate",
        type=float,
        default=0.02,
        help="Minimum exceedance/event rate for Hawkes QC.",
    )
    parser.add_argument(
        "--max-exceedance-rate",
        type=float,
        default=0.25,
        help="Maximum exceedance/event rate for Hawkes QC.",
    )
    parser.add_argument(
        "--min-relative-amplitude",
        type=float,
        default=0.01,
        help="Minimum relative amplitude (p95-p05)/|median| for Hawkes QC.",
    )
    parser.add_argument(
        "--min-median-excess",
        type=float,
        default=0.0,
        help="Minimum median |value-threshold| across events for Hawkes QC.",
    )
    parser.add_argument(
        "--hawkes-window-months",
        type=float,
        default=4.0,
        help="Hard support window in months for Hawkes excitation kernels.",
    )
    parser.add_argument(
        "--eot-integration-points",
        type=int,
        default=256,
        help="Integration grid size used by NHPP likelihood.",
    )
    parser.add_argument(
        "--eot-max-restarts",
        type=int,
        default=4,
        help="Maximum number of NHPP initial points to try per fit.",
    )
    parser.add_argument(
        "--eot-disable-powell-fallback",
        action="store_true",
        help="Disable Powell fallback and use only L-BFGS-B in NHPP fitting.",
    )
    parser.add_argument(
        "--monthly-significance-quantile",
        type=float,
        default=0.95,
        help="Quantile threshold for monthly D->W/W->D significance (Scheme A).",
    )
    return parser.parse_args()


def main() -> None:
    """Entrypoint."""
    args = parse_args()
    if not (0.0 < args.monthly_significance_quantile < 1.0):
        raise ValueError("--monthly-significance-quantile must be in (0, 1)")
    Logger("run_hawkes_batch")
    if args.plot_mode != "none" and not args.to_file:
        log.warning("--plot-mode is ignored unless --to-file is enabled.")
    run(args)


if __name__ == "__main__":
    main()

