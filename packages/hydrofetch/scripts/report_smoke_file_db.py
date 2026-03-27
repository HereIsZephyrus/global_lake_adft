"""Generate a detailed Chinese integrity report for smoke file+db outputs.

This script compares Parquet outputs written by the ``file`` sink against rows
stored in PostgreSQL by the ``db`` sink.  It produces:

* Markdown report
* JSON summary
* CSV difference extracts (when differences exist)
* Chinese matplotlib plots
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import font_manager
from psycopg import sql

matplotlib.use("Agg")
matplotlib.rcParams["axes.unicode_minus"] = False
matplotlib.rcParams["figure.dpi"] = 130
matplotlib.rcParams["axes.grid"] = True
matplotlib.rcParams["grid.alpha"] = 0.25


@dataclass
class DatasetSummary:
    rows: int
    unique_lakes: int
    unique_dates: int
    duplicate_keys: int
    band_null_counts: dict[str, int]
    band_null_rates: dict[str, float]


@dataclass
class ComparisonSummary:
    keys_only_in_file: int
    keys_only_in_db: int
    value_mismatch_rows: int
    max_abs_diff_by_band: dict[str, float]


BAND_UNITS: dict[str, str] = {
    "temperature_2m": "K",
    "dewpoint_temperature_2m": "K",
    "total_precipitation_sum": "m/日",
    "potential_evaporation_sum": "m/日",
}

QUALITY_RULES: dict[str, tuple[float, float]] = {
    "temperature_2m": (180.0, 340.0),
    "dewpoint_temperature_2m": (150.0, 330.0),
    "total_precipitation_sum": (0.0, 2.0),
    "potential_evaporation_sum": (-0.2, 0.2),
}


def _configure_chinese_font() -> None:
    candidate_files = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Medium.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
    ]
    candidate_names = [
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "Droid Sans Fallback",
        "Microsoft YaHei",
        "SimHei",
        "WenQuanYi Zen Hei",
    ]

    for font_path in candidate_files:
        path = Path(font_path)
        if path.is_file():
            font_manager.fontManager.addfont(str(path))
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in candidate_names:
        if name in available:
            matplotlib.rcParams["font.family"] = "sans-serif"
            matplotlib.rcParams["font.sans-serif"] = [name, "DejaVu Sans"]
            return
    matplotlib.rcParams["font.family"] = "sans-serif"
    matplotlib.rcParams["font.sans-serif"] = ["DejaVu Sans"]


def _parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=repo_root / "data" / "hydrofetch_smoke_file_db_out",
        help="Parquet 输出目录",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=repo_root / "data" / "hydrofetch_smoke_file_db_report",
        help="报告输出目录",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repo_root / "packages" / "hydrofetch" / ".env",
        help="hydrofetch .env 文件路径",
    )
    parser.add_argument(
        "--db-table",
        default="era5_forcing",
        help="数据库表名，默认 era5_forcing",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-9,
        help="file/db 数值比较绝对误差阈值",
    )
    return parser.parse_args()


def _display_name(col: str) -> str:
    return f"{col} [{BAND_UNITS[col]}]" if col in BAND_UNITS else col


def _load_file_df(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*_sampled.parquet"))
    if not files:
        raise FileNotFoundError(f"未在 {input_dir} 找到 *_sampled.parquet")
    dfs = [pd.read_parquet(path) for path in files]
    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def _get_band_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if col not in ("hylak_id", "date")]


def _summarize_dataset(df: pd.DataFrame, bands: list[str]) -> DatasetSummary:
    key_dupes = int(df.duplicated(subset=["hylak_id", "date"]).sum())
    null_counts = {band: int(df[band].isna().sum()) for band in bands}
    rows = len(df)
    return DatasetSummary(
        rows=rows,
        unique_lakes=int(df["hylak_id"].nunique()),
        unique_dates=int(df["date"].nunique()),
        duplicate_keys=key_dupes,
        band_null_counts=null_counts,
        band_null_rates={
            band: (null_counts[band] / rows if rows else math.nan) for band in bands
        },
    )


def _load_db_df(
    *,
    env_file: Path,
    table: str,
    date_min: pd.Timestamp,
    date_max: pd.Timestamp,
) -> pd.DataFrame:
    from hydrofetch.config import load_env
    from hydrofetch.db.client import DBClient

    load_env(env_file)
    db = DBClient.from_config()
    with db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT * FROM {table} WHERE date >= %(date_min)s AND date <= %(date_max)s "
                    "ORDER BY date, hylak_id"
                ).format(table=sql.Identifier(table)),
                {
                    "date_min": date_min.date(),
                    "date_max": date_max.date(),
                },
            )
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    if "ingested_at" in df.columns:
        df = df.drop(columns=["ingested_at"])
    return df


def _compare_file_db(
    file_df: pd.DataFrame,
    db_df: pd.DataFrame,
    bands: list[str],
    *,
    tolerance: float,
) -> tuple[ComparisonSummary, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    key_cols = ["hylak_id", "date"]
    file_keys = file_df[key_cols].drop_duplicates()
    db_keys = db_df[key_cols].drop_duplicates()

    only_in_file = file_keys.merge(db_keys, on=key_cols, how="left", indicator=True)
    only_in_file = only_in_file[only_in_file["_merge"] == "left_only"].drop(columns="_merge")

    only_in_db = db_keys.merge(file_keys, on=key_cols, how="left", indicator=True)
    only_in_db = only_in_db[only_in_db["_merge"] == "left_only"].drop(columns="_merge")

    merged = file_df.merge(
        db_df,
        on=key_cols,
        how="inner",
        suffixes=("_file", "_db"),
    )
    mismatch_mask = np.zeros(len(merged), dtype=bool)
    max_abs_diff_by_band: dict[str, float] = {}

    for band in bands:
        file_values = merged[f"{band}_file"]
        db_values = merged[f"{band}_db"]
        both_nan = file_values.isna() & db_values.isna()
        diffs = (file_values - db_values).abs()
        unequal = ~(both_nan | (diffs.fillna(0.0) <= tolerance))
        mismatch_mask |= unequal.to_numpy()
        max_abs_diff_by_band[band] = float(diffs.max(skipna=True) or 0.0)

    mismatch_rows = merged.loc[mismatch_mask].copy()
    summary = ComparisonSummary(
        keys_only_in_file=int(len(only_in_file)),
        keys_only_in_db=int(len(only_in_db)),
        value_mismatch_rows=int(len(mismatch_rows)),
        max_abs_diff_by_band=max_abs_diff_by_band,
    )
    return summary, only_in_file, only_in_db, mismatch_rows


def _quality_checks(df: pd.DataFrame, bands: list[str]) -> dict[str, Any]:
    issues: dict[str, Any] = {
        "全空行数": int(df[bands].isna().all(axis=1).sum()),
        "超范围统计": {},
    }
    for band in bands:
        if band not in QUALITY_RULES:
            continue
        low, high = QUALITY_RULES[band]
        mask = df[band].notna() & ((df[band] < low) | (df[band] > high))
        issues["超范围统计"][band] = {
            "count": int(mask.sum()),
            "range": [low, high],
        }
    return issues


def _basic_stats(df: pd.DataFrame, bands: list[str]) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    for band in bands:
        series = df[band].dropna()
        stats[band] = {
            "count": int(series.count()),
            "mean": float(series.mean()) if not series.empty else math.nan,
            "std": float(series.std()) if len(series) > 1 else math.nan,
            "min": float(series.min()) if not series.empty else math.nan,
            "median": float(series.median()) if not series.empty else math.nan,
            "max": float(series.max()) if not series.empty else math.nan,
        }
    return stats


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _save_csv(path: Path, df: pd.DataFrame) -> None:
    df.to_csv(path, index=False, encoding="utf-8")


def _plot_daily_counts(file_df: pd.DataFrame, db_df: pd.DataFrame, out_dir: Path) -> Path:
    file_counts = file_df.groupby("date").size().rename("文件")
    db_counts = db_df.groupby("date").size().rename("数据库")
    counts = pd.concat([file_counts, db_counts], axis=1).fillna(0).astype(int)
    fig, ax = plt.subplots(figsize=(11, 5))
    x = np.arange(len(counts))
    width = 0.38
    ax.bar(x - width / 2, counts["文件"], width=width, label="文件")
    ax.bar(x + width / 2, counts["数据库"], width=width, label="数据库")
    ax.set_title("每日记录数对比")
    ax.set_xlabel("日期")
    ax.set_ylabel("记录数")
    ax.legend(title="来源")
    ax.set_xticks(x)
    ax.set_xticklabels([idx.strftime("%Y-%m-%d") for idx in counts.index], rotation=45, ha="right")
    plt.tight_layout()
    path = out_dir / "每日记录数对比.png"
    plt.savefig(path)
    plt.close()
    return path


def _plot_lake_coverage(file_df: pd.DataFrame, db_df: pd.DataFrame, out_dir: Path) -> Path:
    file_cov = file_df.groupby("hylak_id").size().rename("文件")
    db_cov = db_df.groupby("hylak_id").size().rename("数据库")
    coverage = pd.concat([file_cov, db_cov], axis=1).fillna(0).astype(int)
    fig, ax = plt.subplots(figsize=(12, 5))
    x = np.arange(len(coverage))
    width = 0.38
    ax.bar(x - width / 2, coverage["文件"], width=width, label="文件")
    ax.bar(x + width / 2, coverage["数据库"], width=width, label="数据库")
    ax.set_title("各湖泊覆盖天数对比")
    ax.set_xlabel("hylak_id")
    ax.set_ylabel("天数")
    ax.legend(title="来源")
    ax.set_xticks(x)
    ax.set_xticklabels([str(idx) for idx in coverage.index], rotation=0)
    plt.tight_layout()
    path = out_dir / "各湖泊覆盖天数对比.png"
    plt.savefig(path)
    plt.close()
    return path


def _plot_band_means(file_df: pd.DataFrame, db_df: pd.DataFrame, bands: list[str], out_dir: Path) -> Path:
    file_mean = file_df.groupby("date")[bands].mean()
    db_mean = db_df.groupby("date")[bands].mean()
    n = len(bands)
    fig, axes = plt.subplots(n, 1, figsize=(12, 3.2 * n), sharex=True)
    if n == 1:
        axes = [axes]
    for ax, band in zip(axes, bands):
        ax.plot(file_mean.index, file_mean[band], "o-", label="文件")
        ax.plot(db_mean.index, db_mean[band], "s--", label="数据库")
        ax.set_ylabel(_display_name(band))
        ax.set_title(f"{band} 日均值对比")
        ax.legend()
    axes[-1].set_xlabel("日期")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    path = out_dir / "各变量日均值对比.png"
    plt.savefig(path)
    plt.close()
    return path


def _plot_null_rates(
    file_summary: DatasetSummary,
    db_summary: DatasetSummary,
    bands: list[str],
    out_dir: Path,
) -> Path:
    df = pd.DataFrame(
        {
            "文件": [file_summary.band_null_rates[band] for band in bands],
            "数据库": [db_summary.band_null_rates[band] for band in bands],
        },
        index=bands,
    )
    ax = df.plot(kind="bar", figsize=(11, 5))
    ax.set_title("各变量空值率对比")
    ax.set_xlabel("变量")
    ax.set_ylabel("空值率")
    ax.legend(title="来源")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    path = out_dir / "各变量空值率对比.png"
    plt.savefig(path)
    plt.close()
    return path


def _write_markdown_report(
    *,
    out_path: Path,
    file_summary: DatasetSummary,
    db_summary: DatasetSummary,
    comparison: ComparisonSummary,
    file_quality: dict[str, Any],
    db_quality: dict[str, Any],
    file_stats: dict[str, Any],
    db_stats: dict[str, Any],
    plots: list[Path],
    diff_files: list[Path],
) -> None:
    def _fmt_rate(x: float) -> str:
        return "NaN" if math.isnan(x) else f"{x:.2%}"

    lines: list[str] = []
    lines.append("# Smoke 完整性与质量报告")
    lines.append("")
    lines.append("## 一、总体结论")
    lines.append("")

    issues: list[str] = []
    if file_summary.duplicate_keys or db_summary.duplicate_keys:
        issues.append("存在重复主键。")
    if comparison.keys_only_in_file or comparison.keys_only_in_db:
        issues.append("文件与数据库键集合不一致。")
    if comparison.value_mismatch_rows:
        issues.append("文件与数据库存在数值不一致。")
    if file_quality["全空行数"] or db_quality["全空行数"]:
        issues.append("存在整行全空记录。")

    if issues:
        lines.append("发现以下异常：")
        for item in issues:
            lines.append(f"- {item}")
    else:
        lines.append("本次检查未发现明显完整性异常，文件与数据库整体一致。")

    lines.append("")
    lines.append("## 二、完整性检查")
    lines.append("")
    lines.append("| 指标 | 文件 | 数据库 |")
    lines.append("|---|---:|---:|")
    lines.append(f"| 总行数 | {file_summary.rows} | {db_summary.rows} |")
    lines.append(f"| 湖泊数 | {file_summary.unique_lakes} | {db_summary.unique_lakes} |")
    lines.append(f"| 日期数 | {file_summary.unique_dates} | {db_summary.unique_dates} |")
    lines.append(f"| 重复主键数 | {file_summary.duplicate_keys} | {db_summary.duplicate_keys} |")
    lines.append("")
    lines.append(f"- 仅在文件中存在的键：{comparison.keys_only_in_file}")
    lines.append(f"- 仅在数据库中存在的键：{comparison.keys_only_in_db}")
    lines.append(f"- 数值不一致的行数：{comparison.value_mismatch_rows}")
    lines.append("")
    lines.append("## 三、质量检查")
    lines.append("")
    lines.append("### 文件侧")
    lines.append(f"- 全空行数：{file_quality['全空行数']}")
    for band, meta in file_quality["超范围统计"].items():
        lines.append(f"- {band} 超范围记录：{meta['count']}，合理范围 {meta['range']}")
    lines.append("")
    lines.append("### 数据库侧")
    lines.append(f"- 全空行数：{db_quality['全空行数']}")
    for band, meta in db_quality["超范围统计"].items():
        lines.append(f"- {band} 超范围记录：{meta['count']}，合理范围 {meta['range']}")
    lines.append("")
    lines.append("### 空值率")
    for band in file_summary.band_null_rates:
        lines.append(
            f"- {band}: 文件 {_fmt_rate(file_summary.band_null_rates[band])} / "
            f"数据库 {_fmt_rate(db_summary.band_null_rates[band])}"
        )

    lines.append("")
    lines.append("## 四、基本统计量")
    lines.append("")
    lines.append("### 文件侧")
    lines.append("```json")
    lines.append(json.dumps(file_stats, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("### 数据库侧")
    lines.append("```json")
    lines.append(json.dumps(db_stats, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 五、最大绝对误差")
    lines.append("")
    for band, value in comparison.max_abs_diff_by_band.items():
        lines.append(f"- {band}: {value:.12g}")
    lines.append("")
    lines.append("## 六、产物")
    lines.append("")
    for plot in plots:
        lines.append(f"- 图表：`{plot}`")
    for diff in diff_files:
        lines.append(f"- 差异明细：`{diff}`")
    lines.append("")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = _parse_args()
    _configure_chinese_font()
    _ensure_output_dir(args.output)

    file_df = _load_file_df(args.input)
    bands = _get_band_columns(file_df)
    db_df = _load_db_df(
        env_file=args.env_file,
        table=args.db_table,
        date_min=file_df["date"].min(),
        date_max=file_df["date"].max(),
    )
    if db_df.empty:
        raise RuntimeError(f"数据库表 {args.db_table!r} 在目标日期范围内无数据。")

    file_summary = _summarize_dataset(file_df, bands)
    db_summary = _summarize_dataset(db_df, bands)
    comparison, only_in_file, only_in_db, mismatch_rows = _compare_file_db(
        file_df, db_df, bands, tolerance=args.tolerance
    )
    file_quality = _quality_checks(file_df, bands)
    db_quality = _quality_checks(db_df, bands)
    file_stats = _basic_stats(file_df, bands)
    db_stats = _basic_stats(db_df, bands)

    diff_files: list[Path] = []
    if not only_in_file.empty:
        path = args.output / "仅文件存在的键.csv"
        _save_csv(path, only_in_file)
        diff_files.append(path)
    if not only_in_db.empty:
        path = args.output / "仅数据库存在的键.csv"
        _save_csv(path, only_in_db)
        diff_files.append(path)
    if not mismatch_rows.empty:
        path = args.output / "数值不一致明细.csv"
        _save_csv(path, mismatch_rows)
        diff_files.append(path)

    plots = [
        _plot_daily_counts(file_df, db_df, args.output),
        _plot_lake_coverage(file_df, db_df, args.output),
        _plot_band_means(file_df, db_df, bands, args.output),
        _plot_null_rates(file_summary, db_summary, bands, args.output),
    ]

    summary_payload = {
        "file_summary": asdict(file_summary),
        "db_summary": asdict(db_summary),
        "comparison": asdict(comparison),
        "file_quality": file_quality,
        "db_quality": db_quality,
        "file_stats": file_stats,
        "db_stats": db_stats,
        "plots": [str(path) for path in plots],
        "diff_files": [str(path) for path in diff_files],
    }
    (args.output / "summary.json").write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    _write_markdown_report(
        out_path=args.output / "report.md",
        file_summary=file_summary,
        db_summary=db_summary,
        comparison=comparison,
        file_quality=file_quality,
        db_quality=db_quality,
        file_stats=file_stats,
        db_stats=db_stats,
        plots=plots,
        diff_files=diff_files,
    )

    print(f"报告已生成：{args.output / 'report.md'}")
    print(f"摘要已生成：{args.output / 'summary.json'}")
    for plot in plots:
        print(f"图表：{plot}")
    for diff in diff_files:
        print(f"差异明细：{diff}")


if __name__ == "__main__":
    main()
