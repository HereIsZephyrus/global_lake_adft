"""Hawkes production QA: pull aggregates from SERIES_DB and optional plots.

Reads ``hawkes_results``, ``hawkes_lrt``, optionally ``eot_results`` coverage and
``hawkes_transition_monthly``. Writes CSV summaries and figures under ``--output-dir``.

Examples:
    uv run python scripts/run_hawkes_qc.py
    uv run python scripts/run_hawkes_qc.py --threshold-quantile 0.95 --no-plots
    uv run python scripts/run_hawkes_qc.py --output-dir data/hawkes/qc_run1
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg.errors

from lakeanalysis.dbconnect import (
    fetch_eot_hawkes_coverage,
    fetch_hawkes_error_message_counts,
    fetch_hawkes_lrt_summary_by_test,
    fetch_hawkes_qc_summary_by_quantile,
    fetch_hawkes_results,
    fetch_hawkes_transition_monthly,
    series_db,
)
from lakeanalysis.plot_config import setup_chinese_font

log = logging.getLogger(__name__)

DEFAULT_OUTPUT = Path(__file__).resolve().parent.parent / "data" / "hawkes" / "qc"


def parse_args() -> argparse.Namespace:
    """CLI for Hawkes DB QC."""
    parser = argparse.ArgumentParser(
        description="Summarize Hawkes tables in SERIES_DB and export CSV / figures.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT),
        help="Directory for CSV and PNG outputs.",
    )
    parser.add_argument(
        "--threshold-quantile",
        type=float,
        default=None,
        help="If set, restrict detailed pulls and some plots to this quantile.",
    )
    parser.add_argument(
        "--results-limit",
        type=int,
        default=200_000,
        help="Max hawkes_results rows loaded for histograms / scatter (safety cap).",
    )
    parser.add_argument(
        "--errors-top-n",
        type=int,
        default=30,
        help="Number of top error_message prefixes in the errors table.",
    )
    parser.add_argument(
        "--transition-limit",
        type=int,
        default=500_000,
        help="Max rows from hawkes_transition_monthly (0 to skip table entirely).",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Only print summaries and write CSV; skip matplotlib figures.",
    )
    parser.add_argument(
        "--skip-eot-coverage",
        action="store_true",
        help="Do not query eot_results (e.g. table missing in this DB).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG logging.",
    )
    return parser.parse_args()


def _savefig(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def plot_rates_by_quantile(summary: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart: QC pass rate and convergence rate by quantile."""
    if summary.empty:
        log.warning("Empty summary; skip plot_rates_by_quantile")
        return
    setup_chinese_font()
    x = np.arange(len(summary))
    w = 0.35
    _, ax = plt.subplots(figsize=(8, 4))
    ax.bar(x - w / 2, summary["qc_pass_rate"], width=w, label="QC 通过率")
    ax.bar(x + w / 2, summary["converged_rate"], width=w, label="收敛率")
    ax.set_xticks(x)
    ax.set_xticklabels([str(q) for q in summary["threshold_quantile"]], rotation=45, ha="right")
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("比例")
    ax.set_xlabel("分位数阈值")
    ax.legend()
    ax.set_title("Hawkes：按分位数的 QC / 收敛")
    _savefig(out_dir / "hawkes_qc_rates_by_quantile.png")


def plot_results_distributions(df: pd.DataFrame, out_dir: Path) -> None:
    """Histograms for n_events and spectral_radius."""
    if df.empty:
        log.warning("Empty hawkes_results sample; skip plot_results_distributions")
        return
    setup_chinese_font()
    _, axes = plt.subplots(1, 2, figsize=(10, 4))
    ne = df["n_events"].dropna()
    if not ne.empty:
        axes[0].hist(ne, bins=min(50, max(10, int(np.sqrt(len(ne))))), color="steelblue", alpha=0.85)
    axes[0].set_xlabel("事件数 n_events")
    axes[0].set_ylabel("频数")
    axes[0].set_title("事件数分布")
    sr = df["spectral_radius"].dropna()
    if not sr.empty:
        axes[1].hist(sr, bins=min(50, max(10, int(np.sqrt(len(sr))))), color="darkorange", alpha=0.85)
    axes[1].set_xlabel("谱半径 spectral_radius")
    axes[1].set_ylabel("频数")
    axes[1].set_title("谱半径分布")
    _savefig(out_dir / "hawkes_qc_event_spectral_hist.png")


def plot_lrt_scatter(df: pd.DataFrame, out_dir: Path) -> None:
    """Scatter lrt_p_d_to_w vs lrt_p_w_to_d (log scale when positive)."""
    if df.empty or "lrt_p_d_to_w" not in df.columns:
        return
    sub = df.dropna(subset=["lrt_p_d_to_w", "lrt_p_w_to_d"])
    if sub.empty:
        log.warning("No LRT p pairs; skip plot_lrt_scatter")
        return
    setup_chinese_font()
    _, ax = plt.subplots(figsize=(5.5, 5.5))
    px = np.clip(sub["lrt_p_d_to_w"].astype(float), 1e-16, 1.0)
    py = np.clip(sub["lrt_p_w_to_d"].astype(float), 1e-16, 1.0)
    ax.scatter(px, py, alpha=0.25, s=8, c="navy")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("LRT p (干→湿)")
    ax.set_ylabel("LRT p (湿→干)")
    ax.set_title("互激 LRT p 散点（对数坐标）")
    ax.axhline(0.05, color="gray", linestyle="--", linewidth=0.8)
    ax.axvline(0.05, color="gray", linestyle="--", linewidth=0.8)
    _savefig(out_dir / "hawkes_qc_lrt_p_scatter.png")


def plot_lrt_reject_by_test(lrt_summary: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart of reject_null_rate per test_name."""
    if lrt_summary.empty:
        log.warning("Empty LRT summary; skip plot_lrt_reject_by_test")
        return
    setup_chinese_font()
    _, ax = plt.subplots(figsize=(max(6, 0.4 * len(lrt_summary)), 4))
    names = lrt_summary["test_name"].astype(str).tolist()
    rates = lrt_summary["reject_null_rate"].astype(float).tolist()
    ax.barh(names, rates, color="teal", alpha=0.85)
    ax.set_xlim(0, 1.05)
    ax.set_xlabel("拒绝原假设比例")
    ax.set_title("hawkes_lrt：按检验名的 reject 率")
    _savefig(out_dir / "hawkes_qc_lrt_reject_by_test.png")


def plot_transition_significant_rate(
    monthly: pd.DataFrame, out_dir: Path, threshold_quantile: float | None
) -> None:
    """Fraction significant per direction (if monthly table has data)."""
    if monthly.empty or "significant" not in monthly.columns:
        return
    setup_chinese_font()
    g = monthly.groupby("direction", dropna=False)["significant"].mean().reset_index()
    g.columns = ["direction", "significant_rate"]
    _, ax = plt.subplots(figsize=(5, 4))
    ax.bar(g["direction"].astype(str), g["significant_rate"], color="slateblue", alpha=0.85)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("显著比例")
    ax.set_xlabel("方向")
    title = "月尺度急转：按方向的显著率"
    if threshold_quantile is not None:
        title += f" (q={threshold_quantile})"
    ax.set_title(title)
    _savefig(out_dir / "hawkes_qc_transition_significant_by_direction.png")


def main() -> int:
    """Entry point."""
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tq = args.threshold_quantile

    with series_db.connection_context() as conn:
        summary = fetch_hawkes_qc_summary_by_quantile(conn)
        print("=== hawkes_results 按分位数汇总 ===")
        print(summary.to_string(index=False))
        summary.to_csv(out_dir / "hawkes_summary_by_quantile.csv", index=False)

        errors = fetch_hawkes_error_message_counts(conn, limit=args.errors_top_n)
        print("\n=== 常见 error_message（截断）===")
        print(errors.to_string(index=False))
        errors.to_csv(out_dir / "hawkes_error_message_counts.csv", index=False)

        lrt_summary = fetch_hawkes_lrt_summary_by_test(conn, threshold_quantile=tq)
        print("\n=== hawkes_lrt 按 test_name 汇总 ===")
        print(lrt_summary.to_string(index=False))
        lrt_summary.to_csv(out_dir / "hawkes_lrt_summary_by_test.csv", index=False)

        if not args.skip_eot_coverage:
            try:
                eot_cov = fetch_eot_hawkes_coverage(conn, threshold_quantile=tq)
                print("\n=== EOT 与 Hawkes 覆盖（按分位数）===")
                print(eot_cov.to_string(index=False))
                eot_cov.to_csv(out_dir / "eot_hawkes_coverage.csv", index=False)
            except psycopg.errors.UndefinedTable as exc:
                log.warning("EOT coverage skipped (missing table): %s", exc)
            except psycopg.errors.Error as exc:
                log.warning("EOT coverage query failed (skip): %s", exc)

        results = fetch_hawkes_results(
            conn,
            threshold_quantile=tq,
            limit=args.results_limit if args.results_limit > 0 else None,
        )
        results.to_csv(out_dir / "hawkes_results_sample.csv", index=False)
        print(f"\n=== hawkes_results 样本已写入 ({len(results)} 行) ===")

        monthly = pd.DataFrame()
        if args.transition_limit and args.transition_limit > 0:
            monthly = fetch_hawkes_transition_monthly(
                conn,
                threshold_quantile=tq,
                limit=args.transition_limit,
            )
            monthly.to_csv(out_dir / "hawkes_transition_monthly_sample.csv", index=False)
            print(f"=== hawkes_transition_monthly 样本 ({len(monthly)} 行) ===")

    if not args.no_plots:
        plot_rates_by_quantile(summary, out_dir)
        plot_results_distributions(results, out_dir)
        plot_lrt_scatter(results, out_dir)
        plot_lrt_reject_by_test(lrt_summary, out_dir)
        plot_transition_significant_rate(monthly, out_dir, tq)
        log.info("Figures written under %s", out_dir)

    log.info("Done. CSV under %s", out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
