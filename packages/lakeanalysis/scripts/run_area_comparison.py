"""Run area comparison analysis: rs_area vs atlas_area from area_quality.

Reads area_quality data via DuckDB/Parquet, computes comparison metrics,
outputs summary statistics, CSV report, and visualization plots.

Usage:
    uv run python scripts/run_area_comparison.py
    uv run python scripts/run_area_comparison.py --data-dir /path/to/parquet
    uv run python scripts/run_area_comparison.py --output-dir results/comparison
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.parquet.client import DuckDBClient
from lakeanalysis.logger import Logger
from lakeanalysis.quality.comparison import (
    AgreementConfig,
    enrich_comparison_df,
    summarize_comparison,
)
from lakeviz.quality import (
    plot_area_ratio_distribution,
    plot_area_scatter,
    plot_lake_area_grid,
    plot_ratio_histogram,
)
from lakeviz.layout import save

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="比较遥感面积与HydroATLAS面积.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        metavar="DIR",
        help="Parquet数据目录路径 (默认从DATA_DIR环境变量读取).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results/area_comparison",
        metavar="DIR",
        help="输出目录 (默认: results/area_comparison).",
    )
    parser.add_argument(
        "--excellent-threshold",
        type=float,
        default=0.1,
        metavar="T",
        help="优秀一致性阈值: 比值在±T范围内 (默认: 0.1 = ±10%%).",
    )
    parser.add_argument(
        "--good-threshold",
        type=float,
        default=2.0,
        metavar="G",
        help="良好一致性阈值: 比值在[1/G, G]范围内 (默认: 2.0).",
    )
    parser.add_argument(
        "--moderate-threshold",
        type=float,
        default=5.0,
        metavar="M",
        help="中等一致性阈值: 比值在[1/M, M]范围内 (默认: 5.0).",
    )
    parser.add_argument(
        "--poor-threshold",
        type=float,
        default=10.0,
        metavar="P",
        help="较差一致性阈值: 比值在[1/P, P]范围内 (默认: 10.0).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=120,
        metavar="N",
        help="从极端比例湖泊中抽样的数量 (默认: 120).",
    )
    return parser.parse_args()


def load_area_quality(data_dir: Path) -> pd.DataFrame:
    """Load area_quality table via DuckDB from parquet files."""
    with DuckDBClient(data_dir=data_dir) as client:
        tables = client.list_registered_tables()
        if "area_quality" not in tables:
            raise FileNotFoundError(
                f"area_quality not found in {data_dir}. Available: {tables}"
            )
        df = client.query_df(
            "SELECT hylak_id, rs_area_mean, rs_area_median, "
            "atlas_area, computed_at "
            "FROM area_quality "
            "ORDER BY hylak_id"
        )
    log.info("从area_quality加载 %d 行数据", len(df))
    return df


def print_summary(summary: dict, label: str) -> None:
    """Print summary statistics to log."""
    n = summary["n_total"]
    if n == 0:
        log.warning("[%s] 无有效数据用于比较 (atlas_area > 0)", label)
        return

    log.info("=" * 60)
    log.info("[%s] 面积比较统计摘要 (n = %d)", label, n)
    log.info("=" * 60)

    counts = summary["n_by_agreement"]
    for level in ["excellent", "good", "moderate", "poor", "extreme"]:
        c = counts.get(level, 0)
        pct = c / n * 100
        log.info("  %-12s: %8d  (%5.1f%%)", level, c, pct)

    log.info("-" * 60)
    log.info("  比值中位数       : %.4f", summary["median_ratio"])
    log.info("  log2比值均值     : %.4f", summary["mean_log2_ratio"])
    log.info("  比值四分位距     : %.4f", summary["iqr_ratio"])
    log.info("  log2比值标准差   : %.4f", summary["std_log2_ratio"])
    log.info(
        "  p05 / p25 / p50 / p75 / p95 比值: "
        "%.4f / %.4f / %.4f / %.4f / %.4f",
        summary["p05_ratio"], summary["p25_ratio"],
        summary["p50_ratio"], summary["p75_ratio"],
        summary["p95_ratio"],
    )
    log.info("-" * 60)
    log.info(
        "  高估 (比值 > 1+T): %d (%.1f%%)",
        summary["n_overestimate"], summary["n_overestimate"] / n * 100,
    )
    log.info(
        "  低估 (比值 < 1-T): %d (%.1f%%)",
        summary["n_underestimate"], summary["n_underestimate"] / n * 100,
    )
    log.info(
        "  一致 (±T)        : %d (%.1f%%)",
        summary["n_agree"], summary["n_agree"] / n * 100,
    )
    log.info("=" * 60)


def config_to_dict(cfg: AgreementConfig) -> dict[str, float]:
    """Convert AgreementConfig to dict for plot labels."""
    return {
        "excellent": cfg.excellent,
        "good": cfg.good,
        "moderate": cfg.moderate,
        "poor": cfg.poor,
    }


def run(args: argparse.Namespace) -> None:
    """Execute the area comparison analysis pipeline."""
    data_dir = Path(args.data_dir) if args.data_dir else Path(
        os.environ.get("DATA_DIR", "data/parquet")
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    config = AgreementConfig(
        excellent=args.excellent_threshold,
        good=args.good_threshold,
        moderate=args.moderate_threshold,
        poor=args.poor_threshold,
    )
    log.info(
        "一致性阈值配置: excellent=±%.2f, good=%.1fx, "
        "moderate=%.1fx, poor=%.1fx",
        config.excellent, config.good, config.moderate, config.poor,
    )

    df = load_area_quality(data_dir)
    if df.empty:
        log.warning("area_quality表为空，无法比较")
        return

    enriched = enrich_comparison_df(df, config=config)

    summary_median = summarize_comparison(
        enriched, rs_col="rs_area_median", config=config,
    )
    print_summary(summary_median, "中位数")

    summary_mean = summarize_comparison(
        enriched, rs_col="rs_area_mean", config=config,
    )
    print_summary(summary_mean, "均值")

    csv_path = output_dir / "area_comparison.csv"
    enriched.to_csv(csv_path, index=False)
    log.info("已保存详细CSV到 %s", csv_path)

    config_dict = config_to_dict(config)

    fig_scatter_median = plot_area_scatter(
        enriched,
        rs_col="rs_area_median",
        agreement_col="agreement_median",
        title="遥感面积中位数与HydroATLAS面积对比",
        config=config_dict,
    )
    save(fig_scatter_median, output_dir / "area_scatter_median.png")

    fig_scatter_mean = plot_area_scatter(
        enriched,
        rs_col="rs_area_mean",
        agreement_col="agreement_mean",
        title="遥感面积均值与HydroATLAS面积对比",
        config=config_dict,
    )
    save(fig_scatter_mean, output_dir / "area_scatter_mean.png")

    fig_ratio_dist, highlight_indices = plot_area_ratio_distribution(enriched)
    save(fig_ratio_dist, output_dir / "area_ratio_distribution.png")

    _save_highlight_ids(enriched, highlight_indices, output_dir)

    fig_hist_median = plot_ratio_histogram(
        enriched,
        log2_ratio_col="log2_ratio_median",
        agreement_col="agreement_median",
        title="log₂(遥感面积中位数/HydroATLAS面积)分布",
        config=config_dict,
    )
    save(fig_hist_median, output_dir / "ratio_histogram_median.png")

    fig_hist_mean = plot_ratio_histogram(
        enriched,
        log2_ratio_col="log2_ratio_mean",
        agreement_col="agreement_mean",
        title="log₂(遥感面积均值/HydroATLAS面积)分布",
        config=config_dict,
    )
    save(fig_hist_mean, output_dir / "ratio_histogram_mean.png")

    _sample_and_plot(enriched, data_dir, output_dir, args.sample)

    log.info("完成。结果保存在 %s", output_dir)



def _save_highlight_ids(
    enriched: pd.DataFrame,
    highlight_indices: dict[str, np.ndarray],
    output_dir: Path,
) -> None:
    """Save 95% highlight hylak_ids to CSV."""
    mean_idx = highlight_indices.get("mean_95pct", np.array([], dtype=int))
    median_idx = highlight_indices.get("median_95pct", np.array([], dtype=int))

    if len(mean_idx) == 0 and len(median_idx) == 0:
        log.info("无95%高亮湖泊，跳过保存")
        return

    mean_ids = (
        enriched.iloc[mean_idx]["hylak_id"].astype(int).tolist()
        if len(mean_idx) > 0
        else []
    )
    median_ids = (
        enriched.iloc[median_idx]["hylak_id"].astype(int).tolist()
        if len(median_idx) > 0
        else []
    )

    records = []
    for hid in mean_ids:
        records.append({"hylak_id": hid, "type": "mean_95pct"})
    for hid in median_ids:
        records.append({"hylak_id": hid, "type": "median_95pct"})

    if records:
        df_out = pd.DataFrame(records)
        csv_path = output_dir / "highlight_95pct_ids.csv"
        df_out.to_csv(csv_path, index=False)
        log.info(
            "保存95%%高亮ID: mean=%d, median=%d → %s",
            len(mean_ids), len(median_ids), csv_path,
        )


def _sample_and_plot(
    enriched: pd.DataFrame,
    data_dir: Path,
    output_dir: Path,
    sample_n: int,
) -> None:
    """Sample extreme-ratio lakes and plot area time series grids."""
    extreme = enriched[
        (enriched["ratio_mean"] > 10) | (enriched["ratio_mean"] < 0.1)
    ]
    n_extreme = len(extreme)
    if n_extreme == 0:
        log.info("无均值比值差异10倍以上的湖泊，跳过抽样绘图")
        return

    n_sample = min(sample_n, n_extreme)
    sampled = extreme.sample(n=n_sample, random_state=42)
    log.info(
        "从 %d 个极端比例湖泊中抽样 %d 个",
        n_extreme, n_sample,
    )

    hylak_ids = sorted(sampled["hylak_id"].astype(int).tolist())
    atlas_map = dict(zip(
        sampled["hylak_id"].astype(int),
        sampled["atlas_area"],
    ))
    ratio_map = dict(zip(
        sampled["hylak_id"].astype(int),
        sampled["ratio_mean"],
    ))

    lake_data = _load_lake_area_series(data_dir, hylak_ids)
    if not lake_data:
        log.warning("无法加载湖泊面积时序数据，跳过抽样绘图")
        return

    n_per_page = 12
    n_pages = (len(lake_data) + n_per_page - 1) // n_per_page
    sorted_ids = sorted(lake_data.keys())

    for page in range(n_pages):
        start = page * n_per_page
        end = min(start + n_per_page, len(sorted_ids))
        page_ids = sorted_ids[start:end]
        page_data = {hid: lake_data[hid] for hid in page_ids}
        page_atlas = {hid: atlas_map[hid] for hid in page_ids}
        page_ratio = {hid: ratio_map[hid] for hid in page_ids}

        fig = plot_lake_area_grid(
            page_data, page_atlas, page_ratio,
            title=f"遥感面积差异湖泊抽样 ({page + 1}/{n_pages})",
        )
        save(fig, output_dir / f"area_grid_{page + 1}.png")

    log.info("已生成 %d 张抽样网格图", n_pages)


def _load_lake_area_series(
    data_dir: Path,
    hylak_ids: list[int],
) -> dict[int, pd.DataFrame]:
    """Load lake_area time series for given hylak_ids via DuckDB."""
    if not hylak_ids:
        return {}
    with DuckDBClient(data_dir=data_dir) as client:
        tables = client.list_registered_tables()
        if "lake_area" not in tables:
            log.warning("lake_area表不在 %s 中", data_dir)
            return {}
        placeholders = ",".join("?" for _ in hylak_ids)
        df = client.query_df(
            f"SELECT hylak_id, "
            f"YEAR(year_month) AS year, "
            f"MONTH(year_month) AS month, "
            f"water_area "
            f"FROM lake_area "
            f"WHERE hylak_id IN ({placeholders}) "
            f"ORDER BY hylak_id, year_month",
            parameters=hylak_ids,
        )
    if df.empty:
        return {}
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)
    result: dict[int, pd.DataFrame] = {}
    for hid, group in df.groupby("hylak_id"):
        result[int(hid)] = group.drop(columns=["hylak_id"]).reset_index(drop=True)
    log.info("加载了 %d 个湖泊的面积时序数据", len(result))
    return result


def main() -> None:
    """Entry point for command-line execution."""
    args = parse_args()
    Logger("run_area_comparison")
    run(args)


if __name__ == "__main__":
    main()
