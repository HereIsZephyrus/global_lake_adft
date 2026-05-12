"""High-quality interpolation figures for specified lakes.

Layout per figure: 2 columns, each column has a main axes (top)
and one or more detail axes (bottom, side-by-side) — one per
true-linear segment.

Usage:
    uv run python scripts/plot_interpolation_hq.py --hylak-ids 9961,889,1406317
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from lakesource.config import Backend, SourceConfig
from lakesource.provider.factory import create_provider
from lakeanalysis.logger import Logger
from lakeanalysis.quality.interpolation import (
    InterpolationConfig,
    get_collinear_segments,
)
from lakeviz.domain.interpolation import (
    draw_interpolation_timeline_hq_main,
    draw_interpolation_timeline_hq_inset,
)

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="High-quality interpolation figures for specified lakes."
    )
    parser.add_argument(
        "--hylak-ids",
        type=str,
        required=True,
        help="Comma-separated hylak_id list.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=200,
    )
    return parser.parse_args()


def _fetch_and_detect(provider, hid: int, config: InterpolationConfig):
    lake_frames = provider.fetch_lake_area_by_ids([hid])
    frozen_map = provider.fetch_frozen_year_months_by_ids([hid])

    if hid not in lake_frames:
        return None, None

    df = lake_frames[hid]
    frozen = frozen_map.get(hid)

    segments = get_collinear_segments(df, frozen_year_months=frozen, config=config)
    seg_dicts = [
        {
            "start_idx": s.start_idx,
            "end_idx": s.end_idx,
            "is_flat": s.is_flat,
            "diff_value": s.diff_value,
            "length": s.length,
        }
        for s in segments
    ]
    return df, seg_dicts


def run(
    hylak_ids: list[int],
    data_dir: Path | None = None,
    output_dir: Path | None = None,
    dpi: int = 200,
) -> None:
    if data_dir is None:
        source_config = SourceConfig()
        parquet_dir = source_config.data_dir
    else:
        parquet_dir = data_dir
        source_config = SourceConfig(backend=Backend.PARQUET, data_dir=parquet_dir)

    if output_dir is None:
        output_dir = parquet_dir.parent / "interpolation" / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)

    source_config = SourceConfig(backend=Backend.PARQUET, data_dir=parquet_dir)
    provider = create_provider(source_config)
    config = InterpolationConfig()

    n_figs = (len(hylak_ids) + 1) // 2

    for fig_idx in range(n_figs):
        left_id = hylak_ids[fig_idx * 2]
        right_id = hylak_ids[fig_idx * 2 + 1] if fig_idx * 2 + 1 < len(hylak_ids) else None

        left_df, left_segs = _fetch_and_detect(provider, int(left_id), config)
        right_df, right_segs = (
            _fetch_and_detect(provider, int(right_id), config) if right_id else (None, None)
        )

        left_n = len([s for s in (left_segs or []) if not s["is_flat"]])
        right_n = len([s for s in (right_segs or []) if not s["is_flat"]])
        max_n = max(left_n, right_n, 1)

        total_cols = 2 * max_n
        fig = plt.figure(figsize=(24, 12))
        gs = gridspec.GridSpec(
            2, total_cols,
            height_ratios=[1.5, 1],
            hspace=0.15,
            wspace=0.4 / max_n,
            figure=fig,
        )

        for col, (df, segs) in enumerate([(left_df, left_segs), (right_df, right_segs)]):
            col_start = col * max_n

            ax_main = fig.add_subplot(gs[0, col_start:col_start + max_n])

            if df is None or segs is None:
                ax_main.text(0.5, 0.5, f"No data", ha="center", va="center")
                for j in range(max_n):
                    ax = fig.add_subplot(gs[1, col_start + j])
                    ax.set_visible(False)
                continue

            hid = [left_id, right_id][col]
            draw_interpolation_timeline_hq_main(ax_main, df, segs, hylak_id=int(hid))

            linear_segs = [s for s in segs if not s["is_flat"]]
            n_linear = len(linear_segs)

            if n_linear == 0:
                ax_blank = fig.add_subplot(gs[1, col_start:col_start + max_n])
                ax_blank.set_visible(False)
                continue

            for j in range(max_n):
                ax_inset = fig.add_subplot(gs[1, col_start + j])
                if j < n_linear:
                    draw_interpolation_timeline_hq_inset(ax_inset, df, linear_segs[j], color_idx=j)
                else:
                    ax_inset.set_visible(False)

        fig.subplots_adjust(left=0.04, right=0.98, top=0.95, bottom=0.06)
        out_path = output_dir / f"hq_{fig_idx + 1:02d}.png"
        fig.savefig(out_path, dpi=dpi)
        plt.close(fig)
        log.info("Saved %s", out_path)

    log.info("Done. Generated %d figures in %s", n_figs, output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_interpolation_hq")
    hylak_ids = [int(x.strip()) for x in args.hylak_ids.split(",")]
    run(
        hylak_ids=hylak_ids,
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        dpi=args.dpi,
    )


if __name__ == "__main__":
    main()
