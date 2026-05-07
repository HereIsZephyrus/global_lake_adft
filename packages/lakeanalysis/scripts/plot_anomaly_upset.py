"""Plot UpSet diagram + donut chart of anomaly set intersections.

Reads anomaly_flags from area_anomalies (postgres or parquet backend),
decodes into boolean columns, and produces a combined UpSet + donut figure.

Usage:
    uv run python scripts/plot_anomaly_upset.py
    uv run python scripts/plot_anomaly_upset.py --output-dir data/figures/upset
    uv run python scripts/plot_anomaly_upset.py --min-size 5
    uv run python scripts/plot_anomaly_upset.py --limit 5000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import upsetplot
from matplotlib.gridspec import GridSpec

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakeanalysis.logger import Logger
from lakeanalysis.quality.filters import decode_anomaly_flags
from lakeviz.layout import save
from lakeviz.plot_config import setup_chinese_font

log = logging.getLogger(__name__)

_SET_COLS = ["is_median_zero", "is_flat_or_pv", "is_area_mismatch", "is_shift"]

_FLAG_TO_SET = {
    "median_zero": "is_median_zero",
    "flat": "is_flat",
    "area_ratio": "is_area_ratio",
    "outside_range": "is_outside_range",
    "pv": "is_pv",
    "shift": "is_shift",
}

_DISPLAY_NAMES = {
    "is_median_zero": "面积为0",
    "is_flat_or_pv": "序列异常",
    "is_area_mismatch": "面积偏差",
}

_COLORS = {
    "is_median_zero": "#E74C3C",
    "is_flat_or_pv": "#3498DB",
    "is_area_mismatch": "#F39C12",
    "normal": "#2ECC71",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot UpSet diagram + donut chart of anomaly set intersections."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/figures/upset",
        metavar="DIR",
        help="Output directory (default: data/figures/upset).",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=0,
        metavar="N",
        help="Minimum intersection size to display (default: 0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit number of lakes (for testing).",
    )
    return parser.parse_args()


def _decode_flags_df(df: pd.DataFrame) -> pd.DataFrame:
    decoded = df["anomaly_flags"].apply(lambda f: decode_anomaly_flags(int(f)))
    flags_df = pd.DataFrame(decoded.tolist(), index=df.index)

    result = pd.DataFrame({"hylak_id": df["hylak_id"].astype(int)})
    for flag_name, set_col in _FLAG_TO_SET.items():
        result[set_col] = flags_df.get(flag_name, False)

    result["is_flat_or_pv"] = result["is_flat"] | result["is_pv"]
    result["is_area_mismatch"] = result["is_area_ratio"] | result["is_outside_range"]

    result = result[["hylak_id"] + _SET_COLS]
    return result


def _load_flags_from_postgres(limit: int | None = None) -> pd.DataFrame:
    from lakesource.postgres import series_db

    limit_sql = f"LIMIT {limit}" if limit else ""
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT hylak_id, anomaly_flags "
                f"FROM area_anomalies "
                f"ORDER BY hylak_id "
                f"{limit_sql}"
            )
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=colnames)
    return _decode_flags_df(df)


def _load_flags_from_parquet(
    source: SourceConfig,
    limit: int | None = None,
) -> pd.DataFrame:
    from lakesource.parquet.client import DuckDBClient

    client = DuckDBClient(data_dir=source.data_dir)

    limit_sql = f"LIMIT {limit}" if limit else ""
    df = client.query_df(
        f"SELECT hylak_id, anomaly_flags "
        f"FROM area_anomalies "
        f"ORDER BY hylak_id "
        f"{limit_sql}"
    )

    if df.empty:
        return pd.DataFrame()

    return _decode_flags_df(df)


def _plot_upset(fig, df: pd.DataFrame, min_size: int) -> dict:
    plot_df = df.copy()
    for col in _SET_COLS:
        plot_df[col] = plot_df[col].astype(bool)

    rename_map = {col: _DISPLAY_NAMES[col] for col in _SET_COLS}
    display_cols = list(rename_map.values())

    anomaly_type = []
    for _, row in plot_df.iterrows():
        types = [_DISPLAY_NAMES[c] for c in _SET_COLS if row[c]]
        anomaly_type.append(types[0] if types else "无")
    plot_df["anomaly_type"] = anomaly_type

    plot_df = plot_df.rename(columns=rename_map)

    counts = upsetplot.from_indicators(display_cols, data=plot_df)
    if min_size > 0:
        counts = counts[counts >= min_size]

    us = upsetplot.UpSet(counts, intersection_plot_elements=0, show_counts=True)
    us.add_stacked_bars(
        by="anomaly_type",
        colors=["#E74C3C", "#3498DB", "#F39C12"],
        title="交集大小",
        elements=5,
    )

    axes = us.plot(fig=fig)

    _COLOR_MAP = {
        _DISPLAY_NAMES["is_median_zero"]: _COLORS["is_median_zero"],
        _DISPLAY_NAMES["is_flat_or_pv"]: _COLORS["is_flat_or_pv"],
        _DISPLAY_NAMES["is_area_mismatch"]: _COLORS["is_area_mismatch"],
    }

    for key, ax in axes.items():
        if ax.get_ylabel() == "Intersection size":
            ax.set_ylabel("交集大小")
        for text in list(ax.texts):
            if text.get_text() == "Intersection size":
                text.set_text("交集大小")
            if text.get_text() in _COLOR_MAP:
                text.set_color(_COLOR_MAP[text.get_text()])

    if "matrix" in axes:
        for text in axes["matrix"].get_yticklabels() + axes["matrix"].get_xticklabels():
            if text.get_text() in _COLOR_MAP:
                text.set_color(_COLOR_MAP[text.get_text()])

    for ax in axes.values():
        if ax.get_legend() is not None:
            ax.get_legend().remove()

    return axes


def _plot_donut(ax, df: pd.DataFrame, n_total: int) -> None:
    n_anomalies = len(df)
    n_normal = n_total - n_anomalies

    sizes = [int(df[col].sum()) for col in _SET_COLS]
    labels = [_DISPLAY_NAMES[col] for col in _SET_COLS]
    colors = [_COLORS[col] for col in _SET_COLS]

    sizes.append(n_normal)
    labels.append("正常")
    colors.append(_COLORS["normal"])

    wedges, _, autotexts = ax.pie(
        sizes,
        colors=colors,
        autopct=lambda pct: f"{pct:.1f}%",
        startangle=90,
        pctdistance=0.75,
        wedgeprops=dict(width=0.4, edgecolor="white", linewidth=2),
    )
    for t in autotexts:
        t.set_fontsize(9)

    ax.legend(
        wedges, labels,
        loc="center",
        bbox_to_anchor=(0.5, -0.12),
        ncol=2,
        fontsize=9,
        frameon=False,
    )


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    load_env()
    source = SourceConfig()
    setup_chinese_font()

    if source.backend.value == "postgres":
        log.info("Using postgres backend")
        df = _load_flags_from_postgres(limit=args.limit)
    else:
        log.info("Using parquet backend: %s", source.data_dir)
        df = _load_flags_from_parquet(source, limit=args.limit)

    if df.empty:
        log.warning("No anomaly data found")
        return

    n_flagged = df[df[_SET_COLS].any(axis=1)].shape[0]
    log.info(
        "Loaded %d records, %d flagged (%.1f%%)",
        len(df), n_flagged, n_flagged / len(df) * 100,
    )
    for col in _SET_COLS:
        n = int(df[col].sum())
        log.info("  %s: %d (%.1f%%)", col, n, n / len(df) * 100)

    from lakesource.postgres import series_db
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM lake_info")
            n_total = int(cur.fetchone()[0])

    fig = plt.figure(figsize=(16, 5))

    ax_donut = fig.add_axes([0.01, 0.15, 0.28, 0.75])
    _plot_donut(ax_donut, df, n_total)

    _plot_upset(fig, df, args.min_size)

    fig.suptitle("湖泊异常分析", fontsize=14, fontweight="bold")
    save(fig, output_dir / "anomaly_upset.png")

    log.info("Combined UpSet + donut saved to %s", output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_anomaly_upset")
    run(args)


if __name__ == "__main__":
    main()
