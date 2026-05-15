"""Plot failed quantile lakes as 4x5 lake pages.

Each page shows 20 lakes. For every lake we draw:
1. raw monthly series
2. frozen-filtered monthly series
3. a short failure summary

Usage:
    uv run python packages/lakeanalysis/scripts/plot_quantile_failed_lake_pages.py \
        --run-status output/smoke_mpi/full/quantile_run_status.parquet \
        --output-dir output/smoke_mpi/full/failed_lake_pages
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakeanalysis.logger import Logger
from lakesource.config import Backend, SourceConfig
from lakesource.env import load_env
from lakesource.provider.factory import create_provider
from lakeanalysis.quality import filter_frozen_rows

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot failed quantile lakes as 4x5 pages.")
    parser.add_argument("--run-status", type=Path, required=True, help="Path to *_run_status.parquet")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for PNG pages")
    parser.add_argument("--limit", type=int, default=80, help="Max failed lakes to plot")
    parser.add_argument("--page-size", type=int, default=20, help="Lakes per page")
    return parser.parse_args()


def _load_failed_ids(run_status: Path, limit: int) -> list[int]:
    df = pd.read_parquet(run_status)
    if "status" not in df.columns or "hylak_id" not in df.columns:
        raise ValueError(f"invalid run_status file: {run_status}")
    if "error_message" not in df.columns:
        return []
    failed = df[df["status"] == "error"].copy()
    failed = failed[failed["error_message"].notna()]
    failed = failed[failed["error_message"].astype(str).str.contains("STL|Need at least|trend window", na=False)]
    return [int(v) for v in failed["hylak_id"].dropna().tolist()[:limit]]


def _draw_one(ax, hylak_id: int, raw_df: pd.DataFrame, frozen_df: pd.DataFrame, reason: str) -> None:
    if raw_df.empty:
        ax.set_title(f"{hylak_id} empty", fontsize=7)
        ax.text(0.5, 0.5, "empty", ha="center", va="center", transform=ax.transAxes, fontsize=7)
        ax.set_axis_off()
        return

    raw_dates = pd.to_datetime({"year": raw_df["year"], "month": raw_df["month"], "day": 1})
    raw_area_km2 = raw_df["water_area"].to_numpy(dtype=float) / 1_000_000.0
    ax.plot(raw_dates, raw_area_km2, linewidth=0.7, color="#9ecae1", label="raw")

    if not frozen_df.empty:
        frozen_dates = pd.to_datetime({"year": frozen_df["year"], "month": frozen_df["month"], "day": 1})
        frozen_area_km2 = frozen_df["water_area"].to_numpy(dtype=float) / 1_000_000.0
        ax.plot(frozen_dates, frozen_area_km2, linewidth=0.8, color="#08519c", label="frozen")

    raw_years = int(raw_df["year"].nunique()) if "year" in raw_df.columns else 0
    frozen_years = int(frozen_df["year"].nunique()) if not frozen_df.empty and "year" in frozen_df.columns else 0
    ax.set_title(f"{hylak_id} rawY={raw_years} frozenY={frozen_years}", fontsize=7)
    ax.text(0.01, 0.02, reason[:90], fontsize=5.5, transform=ax.transAxes, va="bottom", ha="left")
    ax.tick_params(labelsize=6)
    ax.grid(alpha=0.15)


def main() -> None:
    Logger("plot_quantile_failed_lake_pages")
    load_env()
    args = parse_args()

    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme

    Theme.apply()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    source = SourceConfig(backend=Backend.PARQUET, output_filter="full")
    provider = create_provider(source)

    failed_ids = _load_failed_ids(args.run_status, args.limit)
    if not failed_ids:
        log.warning("No failed STL lakes found in %s", args.run_status)
        return

    run_status_df = pd.read_parquet(args.run_status)

    error_map = (
        run_status_df[run_status_df["status"] == "error"]
        .set_index("hylak_id")["error_message"]
        .fillna("")
        .astype(str)
        .to_dict()
    )

    lake_map = provider.fetch_lake_area_by_ids(failed_ids)
    frozen_map = provider.fetch_frozen_year_months_by_ids(failed_ids)

    page_size = max(1, args.page_size)
    per_page = 20
    n_pages = (min(len(failed_ids), page_size) + per_page - 1) // per_page

    for page_idx in range(n_pages):
        batch = failed_ids[page_idx * per_page : (page_idx + 1) * per_page]
        if not batch:
            break

        fig, axes = plt.subplots(5, 4, figsize=(22, 18))
        axes_flat = axes.flatten()
        for row, hylak_id in enumerate(batch):
            raw_df = lake_map.get(hylak_id, pd.DataFrame())
            frozen_df = filter_frozen_rows(raw_df, frozen_map.get(hylak_id, set())) if not raw_df.empty else pd.DataFrame()
            reason = error_map.get(hylak_id, "")
            _draw_one(axes_flat[row], hylak_id, raw_df, frozen_df, reason)

        for i in range(len(batch), 20):
            ax = axes_flat[i]
            ax.set_axis_off()

        fig.suptitle(f"Failed quantile lakes page {page_idx + 1}/{n_pages}", fontsize=14, fontweight="bold")
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        out_path = args.output_dir / f"quantile_failed_lakes_{page_idx + 1}.png"
        fig.savefig(out_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        log.info("Saved %s", out_path)


if __name__ == "__main__":
    main()
