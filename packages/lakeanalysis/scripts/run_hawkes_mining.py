"""Mine Hawkes batch outputs for short-memory transition lakes.

This script reads file outputs created by ``scripts/run_hawkes_batch.py`` and
produces:
  1. Short-memory transition lake lists (D->W, W->D, either direction).
  2. Aggregate quality statistics.
  3. Diagnostic plots (beta histogram + fit quality + parameter summaries).
  4. Combined dry/wet event plots for selected short-memory cases only.

Usage examples:
    uv run python scripts/run_hawkes_mining.py
    uv run python scripts/run_hawkes_mining.py --p-threshold 0.01 --alpha-min 1e-4
    uv run python scripts/run_hawkes_mining.py --min-events 20
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeanalysis.dbconnect import fetch_lake_area_by_ids, series_db
from lakeanalysis.eot import plot_eot_extremes_from_db
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "hawkes" / "batch"
SUMMARY_CSV = DATA_DIR / "summary.csv"
OUTPUT_DIR = DATA_DIR / "mining"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for Hawkes data mining."""
    parser = argparse.ArgumentParser(
        description="Mine transition lakes and fit-quality metrics from Hawkes batch outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-summary",
        type=str,
        default=str(SUMMARY_CSV),
        help="Path to batch summary CSV.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="Directory for mined tables and figures.",
    )
    parser.add_argument(
        "--p-threshold",
        type=float,
        default=0.05,
        help="Significance threshold for LRT p-values.",
    )
    parser.add_argument(
        "--alpha-min",
        type=float,
        default=1e-3,
        help="Minimum cross-alpha magnitude for transition filtering.",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=12,
        help="Minimum number of events required for transition filtering.",
    )
    parser.add_argument(
        "--quarter-window-years",
        type=float,
        default=0.25,
        help="Window length (years) used to define short-memory transitions.",
    )
    parser.add_argument(
        "--quarterly-min-mass",
        type=float,
        default=0.50,
        help="Minimum excitation mass inside the quarter window.",
    )
    parser.add_argument(
        "--max-case-plots",
        type=int,
        default=500,
        help="Maximum number of short-memory case plots to export.",
    )
    return parser.parse_args()


def _load_summary(path: Path) -> pd.DataFrame:
    """Load summary CSV and validate required columns."""
    if not path.exists():
        raise FileNotFoundError(f"Summary CSV not found: {path}")
    frame = pd.read_csv(path)
    required = {
        "hylak_id",
        "threshold_quantile",
        "converged",
        "n_events",
        "log_likelihood",
        "spectral_radius",
        "alpha_DW",
        "alpha_WD",
        "beta_DW",
        "beta_WD",
        "lrt_p_D_to_W",
        "lrt_p_W_to_D",
        "error_message",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns in summary: {sorted(missing)}")
    return frame


def _safe_series_divide(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Element-wise divide while guarding zeros and NaNs."""
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numer / denom
    result = result.replace([np.inf, -np.inf], np.nan)
    return result


def _select_transition_lakes(
    summary: pd.DataFrame,
    p_threshold: float,
    alpha_min: float,
    min_events: int,
    quarter_window_years: float,
    quarterly_min_mass: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return D->W, W->D and union short-memory transition tables."""
    fit_ok = summary[
        summary["converged"].astype(bool)
        & summary["error_message"].isna()
        & (pd.to_numeric(summary["n_events"], errors="coerce") >= min_events)
    ].copy()
    fit_ok["ll_per_event"] = _safe_series_divide(fit_ok["log_likelihood"], fit_ok["n_events"])
    fit_ok["mass_q_D_to_W"] = 1.0 - np.exp(
        -quarter_window_years * pd.to_numeric(fit_ok["beta_WD"], errors="coerce")
    )
    fit_ok["mass_q_W_to_D"] = 1.0 - np.exp(
        -quarter_window_years * pd.to_numeric(fit_ok["beta_DW"], errors="coerce")
    )

    d_to_w = fit_ok[
        (pd.to_numeric(fit_ok["lrt_p_D_to_W"], errors="coerce") < p_threshold)
        & (pd.to_numeric(fit_ok["alpha_WD"], errors="coerce") >= alpha_min)
        & (pd.to_numeric(fit_ok["mass_q_D_to_W"], errors="coerce") >= quarterly_min_mass)
    ].copy()
    d_to_w["transition_direction"] = "D_to_W"

    w_to_d = fit_ok[
        (pd.to_numeric(fit_ok["lrt_p_W_to_D"], errors="coerce") < p_threshold)
        & (pd.to_numeric(fit_ok["alpha_DW"], errors="coerce") >= alpha_min)
        & (pd.to_numeric(fit_ok["mass_q_W_to_D"], errors="coerce") >= quarterly_min_mass)
    ].copy()
    w_to_d["transition_direction"] = "W_to_D"

    union = (
        pd.concat([d_to_w, w_to_d], ignore_index=True)
        .sort_values(["hylak_id", "threshold_quantile", "transition_direction"])
        .reset_index(drop=True)
    )
    return d_to_w, w_to_d, union


def _build_overall_stats(summary: pd.DataFrame) -> dict:
    """Compute overall fit statistics."""
    total = len(summary)
    converged = int(summary["converged"].fillna(False).astype(bool).sum())
    failed = int(summary["error_message"].notna().sum())
    ok = summary[
        summary["converged"].fillna(False).astype(bool)
        & summary["error_message"].isna()
    ].copy()
    ok["ll_per_event"] = _safe_series_divide(ok["log_likelihood"], ok["n_events"])
    stats = {
        "n_total_rows": int(total),
        "n_converged": converged,
        "n_failed": failed,
        "convergence_rate": float(converged / total) if total > 0 else float("nan"),
        "failure_rate": float(failed / total) if total > 0 else float("nan"),
        "median_n_events": float(pd.to_numeric(ok["n_events"], errors="coerce").median()),
        "p25_n_events": float(pd.to_numeric(ok["n_events"], errors="coerce").quantile(0.25)),
        "p75_n_events": float(pd.to_numeric(ok["n_events"], errors="coerce").quantile(0.75)),
        "median_ll_per_event": float(pd.to_numeric(ok["ll_per_event"], errors="coerce").median()),
        "median_spectral_radius": float(pd.to_numeric(ok["spectral_radius"], errors="coerce").median()),
        "stable_ratio_rho_lt_1": float(
            (pd.to_numeric(ok["spectral_radius"], errors="coerce") < 1.0).mean()
        ),
    }
    return stats


def _plot_beta_wd_hist(transition_union: pd.DataFrame, output_dir: Path) -> None:
    """Plot histogram of beta_WD for selected transition lakes."""
    fig, ax = plt.subplots(figsize=(8, 5))
    data = pd.to_numeric(transition_union["beta_WD"], errors="coerce").dropna()
    if data.empty:
        ax.text(0.5, 0.5, "No transition lakes selected", ha="center", va="center")
    else:
        ax.hist(data, bins=30)
    ax.set_xlabel("beta_WD")
    ax.set_ylabel("Count")
    ax.set_title("Histogram of beta_WD for Transition Lakes")
    fig.tight_layout()
    fig.savefig(output_dir / "hist_beta_WD_transition_lakes.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def _load_events_from_case(case_output_dir: Path) -> pd.DataFrame:
    """Load per-case events CSV and convert to EOT-style columns."""
    events_path = case_output_dir / "events.csv"
    if not events_path.exists():
        return pd.DataFrame(
            columns=["tail", "year", "month", "water_area", "threshold_at_event"]
        )
    events = pd.read_csv(events_path)
    if events.empty:
        return pd.DataFrame(
            columns=["tail", "year", "month", "water_area", "threshold_at_event"]
        )
    converted = pd.DataFrame(
        {
            "tail": np.where(events["event_label"] == "W", "high", "low"),
            "year": pd.to_numeric(events["year"], errors="coerce").astype("Int64"),
            "month": pd.to_numeric(events["month"], errors="coerce").astype("Int64"),
            "water_area": pd.to_numeric(events["original_value"], errors="coerce"),
            "threshold_at_event": pd.to_numeric(events["threshold"], errors="coerce"),
        }
    )
    return converted.dropna(subset=["year", "month", "water_area", "threshold_at_event"]).copy()


def _plot_short_memory_cases(
    transition_union: pd.DataFrame,
    output_dir: Path,
    max_case_plots: int,
) -> pd.DataFrame:
    """Plot selected short-memory cases as combined dry/wet timelines."""
    case_plot_dir = output_dir / "short_memory_cases"
    case_plot_dir.mkdir(parents=True, exist_ok=True)
    if transition_union.empty:
        return pd.DataFrame(columns=["hylak_id", "threshold_quantile", "transition_direction", "plot_path"])

    ranked = transition_union.copy()
    ranked["effect_strength"] = np.where(
        ranked["transition_direction"] == "D_to_W",
        pd.to_numeric(ranked["alpha_WD"], errors="coerce")
        * pd.to_numeric(ranked["mass_q_D_to_W"], errors="coerce"),
        pd.to_numeric(ranked["alpha_DW"], errors="coerce")
        * pd.to_numeric(ranked["mass_q_W_to_D"], errors="coerce"),
    )
    ranked = ranked.sort_values("effect_strength", ascending=False).head(max_case_plots)

    ids = sorted({int(v) for v in ranked["hylak_id"].dropna().tolist()})
    with series_db.connection_context() as conn:
        series_map = fetch_lake_area_by_ids(conn, ids)

    records: list[dict] = []
    for _, row in ranked.iterrows():
        hylak_id = int(row["hylak_id"])
        threshold_quantile = float(row["threshold_quantile"])
        transition_direction = str(row["transition_direction"])
        series_df = series_map.get(hylak_id)
        if series_df is None:
            continue
        case_output_dir = Path(str(row["output_dir"]))
        events_df = _load_events_from_case(case_output_dir)
        if events_df.empty:
            continue
        fig = plot_eot_extremes_from_db(
            hylak_id=hylak_id,
            series_df=series_df,
            extremes_df=events_df,
            annotate_top_n_each_tail=6,
        )
        quantile_tag = f"{threshold_quantile:.4f}"
        out_path = case_plot_dir / (
            f"hylak_{hylak_id}_q_{quantile_tag}_{transition_direction}_combined_extremes.png"
        )
        fig.savefig(out_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        records.append(
            {
                "hylak_id": hylak_id,
                "threshold_quantile": threshold_quantile,
                "transition_direction": transition_direction,
                "effect_strength": float(row["effect_strength"]),
                "plot_path": str(out_path),
            }
        )
    return pd.DataFrame(records)


def _plot_quality_metrics(summary: pd.DataFrame, output_dir: Path) -> None:
    """Plot convergence and quality metric distributions."""
    fit_ok = summary[
        summary["converged"].fillna(False).astype(bool)
        & summary["error_message"].isna()
    ].copy()
    fit_ok["ll_per_event"] = _safe_series_divide(fit_ok["log_likelihood"], fit_ok["n_events"])

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    conv_values = [
        int(fit_ok.shape[0]),
        int(summary["error_message"].notna().sum()),
        int((~summary["converged"].fillna(False).astype(bool) & summary["error_message"].isna()).sum()),
    ]
    axes[0, 0].bar(["Converged", "Failed", "NotConverged"], conv_values)
    axes[0, 0].set_title("Task Status Counts")

    axes[0, 1].hist(pd.to_numeric(fit_ok["ll_per_event"], errors="coerce").dropna(), bins=40)
    axes[0, 1].set_title("Log-likelihood per Event")
    axes[0, 1].set_xlabel("ll_per_event")

    axes[1, 0].hist(pd.to_numeric(fit_ok["n_events"], errors="coerce").dropna(), bins=40)
    axes[1, 0].set_title("Event Count per Task")
    axes[1, 0].set_xlabel("n_events")

    axes[1, 1].hist(pd.to_numeric(fit_ok["spectral_radius"], errors="coerce").dropna(), bins=40)
    axes[1, 1].axvline(1.0, linestyle="--", color="tomato")
    axes[1, 1].set_title("Spectral Radius Distribution")
    axes[1, 1].set_xlabel("spectral_radius")

    fig.tight_layout()
    fig.savefig(output_dir / "fit_quality_metrics.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def _plot_parameter_distributions(summary: pd.DataFrame, output_dir: Path) -> None:
    """Plot key Hawkes parameter histograms."""
    fit_ok = summary[
        summary["converged"].fillna(False).astype(bool)
        & summary["error_message"].isna()
    ].copy()
    columns = [
        ("alpha_WD", "alpha_WD"),
        ("alpha_DW", "alpha_DW"),
        ("beta_WD", "beta_WD"),
        ("beta_DW", "beta_DW"),
        ("alpha_WW", "alpha_WW"),
        ("alpha_DD", "alpha_DD"),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    for axis, (col, title) in zip(axes.flatten(), columns, strict=True):
        axis.hist(pd.to_numeric(fit_ok[col], errors="coerce").dropna(), bins=40)
        axis.set_title(title)
    fig.tight_layout()
    fig.savefig(output_dir / "hawkes_parameter_distributions.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def _plot_transition_pvalues(summary: pd.DataFrame, output_dir: Path) -> None:
    """Plot p-value distributions for both transition directions."""
    fit_ok = summary[
        summary["converged"].fillna(False).astype(bool)
        & summary["error_message"].isna()
    ].copy()
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    axes[0].hist(pd.to_numeric(fit_ok["lrt_p_D_to_W"], errors="coerce").dropna(), bins=40)
    axes[0].set_title("LRT p-values: D->W")
    axes[0].set_xlabel("p-value")
    axes[1].hist(pd.to_numeric(fit_ok["lrt_p_W_to_D"], errors="coerce").dropna(), bins=40)
    axes[1].set_title("LRT p-values: W->D")
    axes[1].set_xlabel("p-value")
    fig.tight_layout()
    fig.savefig(output_dir / "lrt_pvalue_distributions.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def run(args: argparse.Namespace) -> None:
    """Execute mining pipeline."""
    summary_path = Path(args.input_summary)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = _load_summary(summary_path)
    d_to_w, w_to_d, transition_union = _select_transition_lakes(
        summary=summary,
        p_threshold=args.p_threshold,
        alpha_min=args.alpha_min,
        min_events=args.min_events,
        quarter_window_years=args.quarter_window_years,
        quarterly_min_mass=args.quarterly_min_mass,
    )
    overall_stats = _build_overall_stats(summary)
    overall_stats.update(
        {
            "p_threshold": float(args.p_threshold),
            "alpha_min": float(args.alpha_min),
            "min_events": int(args.min_events),
            "quarter_window_years": float(args.quarter_window_years),
            "quarterly_min_mass": float(args.quarterly_min_mass),
            "n_transition_d_to_w": int(len(d_to_w)),
            "n_transition_w_to_d": int(len(w_to_d)),
            "n_transition_union_rows": int(len(transition_union)),
            "n_transition_union_lakes": int(transition_union["hylak_id"].nunique()) if not transition_union.empty else 0,
        }
    )

    d_to_w.to_csv(output_dir / "transition_lakes_D_to_W.csv", index=False)
    w_to_d.to_csv(output_dir / "transition_lakes_W_to_D.csv", index=False)
    transition_union.to_csv(output_dir / "transition_lakes_union.csv", index=False)
    (output_dir / "overall_stats.json").write_text(
        json.dumps(overall_stats, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    _plot_beta_wd_hist(transition_union=transition_union, output_dir=output_dir)
    _plot_quality_metrics(summary=summary, output_dir=output_dir)
    _plot_parameter_distributions(summary=summary, output_dir=output_dir)
    _plot_transition_pvalues(summary=summary, output_dir=output_dir)
    case_index = _plot_short_memory_cases(
        transition_union=transition_union,
        output_dir=output_dir,
        max_case_plots=args.max_case_plots,
    )
    case_index.to_csv(output_dir / "short_memory_case_plots_index.csv", index=False)

    log.info("Saved mining tables and plots to %s", output_dir)
    log.info(
        "Short-memory transitions: D->W=%d, W->D=%d, union_lakes=%d, case_plots=%d",
        len(d_to_w),
        len(w_to_d),
        overall_stats["n_transition_union_lakes"],
        len(case_index),
    )


def main() -> None:
    """Entrypoint."""
    args = parse_args()
    Logger("run_hawkes_mining")
    run(args)


if __name__ == "__main__":
    main()

