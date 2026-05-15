"""Plot UpSet diagram + donut chart of anomaly set intersections.

Reads anomaly_flags from area_anomalies (postgres or parquet backend),
decodes into boolean columns, and produces a combined UpSet + donut figure.

Usage:
    uv run python scripts/plot_anomaly_upset.py
    uv run python scripts/plot_anomaly_upset.py --output-dir figure
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
from lakeviz.config import DEFAULT_VIZ_CONFIG
from lakeviz.style.presets import Theme

log = logging.getLogger(__name__)

_SET_COLS = ["is_median_zero", "is_flat_or_pv", "is_area_mismatch", "is_shift"]

_FLAG_TO_SET = {
    "zero_quantile": "is_median_zero",
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
    "is_shift": "趋势突变",
}

_COLORS = {
    "is_median_zero": "#E74C3C",
    "is_flat_or_pv": "#3498DB",
    "is_area_mismatch": "#F39C12",
    "is_shift": "#9B59B6",
    "normal": "#2ECC71",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot UpSet diagram + donut chart of anomaly set intersections."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="figure",
        metavar="DIR",
        help="Output directory (default: figure).",
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

    plot_df = plot_df.rename(columns=rename_map)

    counts = upsetplot.from_indicators(display_cols, data=plot_df)
    if min_size > 0:
        counts = counts[counts >= min_size]

    us = upsetplot.UpSet(
        counts,
        intersection_plot_elements=5,
        show_counts=True,
        facecolor="black",
        totals_plot_elements=3,
    )

    for col, color in zip(_SET_COLS, ["#E74C3C", "#3498DB", "#F39C12", "#9B59B6"]):
        us.style_categories(
            _DISPLAY_NAMES[col],
            bar_facecolor=color,
        )

    axes = us.plot(fig=fig)

    for key, ax in axes.items():
        if ax.get_ylabel() == "Intersection size":
            ax.set_ylabel("交集大小")
        for text in list(ax.texts):
            if text.get_text() == "Intersection size":
                text.set_text("交集大小")

    for ax in axes.values():
        if ax.get_legend() is not None:
            ax.get_legend().remove()

    return axes


def _plot_donut(ax, df: pd.DataFrame, n_total: int) -> tuple[list, list[str]]:
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
    ax.set_anchor("N")
    for t in autotexts:
        t.set_fontsize(9)
    return list(wedges), labels


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    load_env()
    source = SourceConfig()
    Theme.apply()

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

    fig = plt.figure(figsize=(16, 5.8))

    _plot_upset(fig, df, args.min_size)

    ax_donut = fig.add_axes([0.08, 0.35, 0.28, 0.56])
    wedges, labels = _plot_donut(ax_donut, df, n_total)

    legend = fig.legend(
        wedges,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.02),
        bbox_transform=fig.transFigure,
        ncol=5,
        fontsize=9,
        frameon=False,
    )

    fig.suptitle("湖泊异常分析", fontsize=14, fontweight="bold", y=0.98)
    out_path = output_dir / "anomaly_upset.png"
    fig.savefig(
        out_path,
        dpi=DEFAULT_VIZ_CONFIG.default_dpi,
        bbox_inches="tight",
        bbox_extra_artists=(legend,),
        pad_inches=0.35,
    )
    plt.close(fig)
    log.info("Saved figure to %s", out_path)

    log.info("Combined UpSet + donut saved to %s", output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_anomaly_upset")
    run(args)


if __name__ == "__main__":
    main()
