"""Plot comparison global maps: Quantile vs PWM exceedance rates.

Reads grid aggregation parquet files and produces 6 global distribution maps.

Usage:
    python scripts/plot_comparison_global.py \
        --input-dir data/comparison \
        --output-dir figures/comparison
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np

from lakeviz.config import DEFAULT_VIZ_CONFIG
from lakeviz.grid import agg_to_grid_matrix
from lakeviz.map_plot import draw_global_grid

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot comparison global maps.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-dir", type=str, required=True,
        help="Directory containing grid_*.parquet files.",
    )
    parser.add_argument(
        "--output-dir", type=str, default="figures/comparison",
        help="Output directory for figures.",
    )
    parser.add_argument(
        "--resolution", type=float, default=0.5,
        help="Grid resolution in degrees.",
    )
    return parser.parse_args()


def _plot_map(
    agg_path: Path,
    output_path: Path,
    value_col: str,
    title: str,
    cbar_label: str,
    cmap: str = "YlOrRd",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
) -> None:
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    if not agg_path.exists():
        log.warning("Input file not found: %s", agg_path)
        return

    import pandas as pd
    agg = pd.read_parquet(agg_path)
    if agg.empty:
        log.warning("Empty aggregation: %s", agg_path)
        return

    lons, lats, values = agg_to_grid_matrix(agg, value_col, 0.5)

    fig, ax = plt.subplots(
        figsize=(16, 8),
        subplot_kw={"projection": ccrs.Robinson()},
    )
    draw_global_grid(
        ax, lons, lats, values,
        title=title,
        cmap=cmap,
        log_scale=log_scale,
        vmin=vmin,
        vmax=vmax,
        cbar_label=cbar_label,
    )
    fig.savefig(output_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", output_path)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    plots = [
        ("grid_q_high_rate.parquet", "q_high_rate", "Quantile 高值超越频率", "超越率", "YlOrRd", True, None, None),
        ("grid_pwm_high_rate.parquet", "pwm_high_rate", "PWM 高值超越频率", "超越率", "YlOrRd", True, None, None),
        ("grid_diff_high_rate.parquet", "diff_high_rate", "高值超越频率差异 (PWM - Quantile)", "差异", "RdBu", False, None, None),
        ("grid_q_low_rate.parquet", "q_low_rate", "Quantile 低值超越频率", "超越率", "YlOrRd", True, None, None),
        ("grid_pwm_low_rate.parquet", "pwm_low_rate", "PWM 低值超越频率", "超越率", "YlOrRd", True, None, None),
        ("grid_diff_low_rate.parquet", "diff_low_rate", "低值超越频率差异 (PWM - Quantile)", "差异", "RdBu", False, None, None),
    ]

    for agg_file, value_col, title, cbar_label, cmap, log_scale, vmin, vmax in plots:
        agg_path = input_dir / agg_file
        out_file = agg_file.replace("grid_", "").replace(".parquet", ".png")
        output_path = output_dir / out_file

        _plot_map(
            agg_path, output_path, value_col,
            title, cbar_label, cmap, log_scale, vmin, vmax,
        )

    log.info("All plots generated.")


if __name__ == "__main__":
    main()
