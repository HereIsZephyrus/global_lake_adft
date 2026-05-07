"""Plot KDE of lake area distribution: lake_info vs area_quality."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gaussian_kde

from lakesource.postgres import series_db
from lakeviz.layout import save
from lakeviz.style.presets import Theme

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="KDE of lake area distribution: lake_info vs area_quality."
    )
    parser.add_argument("--limit", type=int, default=None, metavar="N")
    parser.add_argument("--output-dir", type=str, default="data/figures/histogram")
    return parser.parse_args()


def _plot_kde(ax, all_areas, qual_areas, title, n_grid=500):
    log_all = np.log10(all_areas[all_areas > 0])
    log_qual = np.log10(qual_areas[qual_areas > 0])
    x_grid = np.linspace(min(log_all.min(), log_qual.min()), max(log_all.max(), log_qual.max()), n_grid)

    kde_all = gaussian_kde(log_all)
    kde_qual = gaussian_kde(log_qual)

    ax.plot(10**x_grid, kde_all(x_grid), label=f"全部湖泊 (n={len(log_all)})", color="steelblue", linewidth=1.5)
    ax.fill_between(10**x_grid, kde_all(x_grid), alpha=0.2, color="steelblue")
    ax.plot(10**x_grid, kde_qual(x_grid), label=f"质量筛选后 (n={len(log_qual)})", color="coral", linewidth=1.5)
    ax.fill_between(10**x_grid, kde_qual(x_grid), alpha=0.2, color="coral")

    ax.set_xscale("log")
    ax.set_xlabel("湖泊面积 (km²)")
    ax.set_ylabel("概率密度")
    ax.set_title(title)
    ax.legend()


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lake_area FROM lake_info WHERE hylak_id < %s",
                (args.limit,),
            ) if args.limit else cur.execute("SELECT lake_area FROM lake_info")
            lake_info_areas = np.array([float(r[0]) for r in cur.fetchall()])

            cur.execute(
                "SELECT atlas_area FROM area_quality WHERE hylak_id < %s",
                (args.limit,),
            ) if args.limit else cur.execute("SELECT atlas_area FROM area_quality")
            area_quality_areas = np.array([float(r[0]) for r in cur.fetchall()])

    log.info("lake_info: %d lakes, area_quality: %d lakes", len(lake_info_areas), len(area_quality_areas))

    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    Theme.apply()

    _plot_kde(axes[0], lake_info_areas, area_quality_areas, "全部湖泊")

    mask_all = lake_info_areas > 10
    mask_qual = area_quality_areas > 10
    _plot_kde(axes[1], lake_info_areas[mask_all], area_quality_areas[mask_qual], "面积 > 10 km²")

    fig.suptitle(f"湖泊面积分布：全部 vs 质量筛选后 (hylak_id < {args.limit})")
    fig.tight_layout()
    save(fig, output_dir / "lake_area_histogram.png")
    log.info("Saved to %s", output_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    run(parse_args())


if __name__ == "__main__":
    main()
