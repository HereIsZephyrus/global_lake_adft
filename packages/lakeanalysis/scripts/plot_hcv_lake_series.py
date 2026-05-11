"""Plot water_area time series for 100 randomly sampled H×CV <= 0.001 lakes.

4x5 grid per figure, 5 figures total = 100 lakes.

Usage:
    uv run python scripts/plot_hcv_lake_series.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from lakesource.env import load_env
from lakesource.postgres import fetch_lake_area_by_ids, series_db
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def main() -> None:
    Logger("plot_hcv_lake_series")
    load_env()

    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme
    Theme.apply()

    with series_db.connection_context() as conn:
        meta = pd.read_sql(
            "SELECT ecv.hylak_id, ecv.h_cv, ecv.n_obs, ecv.n_distinct, "
            "ecv.dominant_ratio, ecv.n_frozen "
            "FROM area_entropy_cv ecv "
            "WHERE ecv.h_cv <= 0.001 "
            "ORDER BY RANDOM() LIMIT 100",
            conn,
        )

    if meta.empty:
        log.warning("No H×CV <= 0.001 lakes found")
        return

    log.info("Sampled %d H×CV <= 0.001 lakes", len(meta))

    id_list = meta["hylak_id"].tolist()
    with series_db.connection_context() as conn:
        lake_frames = fetch_lake_area_by_ids(conn, id_list)

    hcv_map = meta.set_index("hylak_id")["h_cv"].to_dict()
    nobs_map = meta.set_index("hylak_id")["n_obs"].to_dict()
    ndist_map = meta.set_index("hylak_id")["n_distinct"].to_dict()
    dr_map = meta.set_index("hylak_id")["dominant_ratio"].to_dict()

    n_cols, n_rows = 4, 5
    per_fig = n_cols * n_rows
    n_figs = (len(id_list) + per_fig - 1) // per_fig

    output_dir = DATA_DIR / "figures" / "quality"
    output_dir.mkdir(parents=True, exist_ok=True)

    for fig_idx in range(n_figs):
        start = fig_idx * per_fig
        end = min(start + per_fig, len(id_list))
        batch = id_list[start:end]

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 16))
        axes_flat = axes.flatten()

        for i, hid in enumerate(batch):
            ax = axes_flat[i]
            df = lake_frames.get(hid)
            hcv = hcv_map.get(hid, 0)
            n_obs = nobs_map.get(hid, 0)
            n_dist = ndist_map.get(hid, 0)
            dr = dr_map.get(hid, 0)

            if df is not None and not df.empty:
                dates = pd.to_datetime({"year": df["year"], "month": df["month"], "day": 1})
                area_km2 = df["water_area"].values / 1e6
                ax.plot(dates, area_km2, linewidth=0.6, color="steelblue")

            ax.set_title(
                f"ID {hid} H×CV={hcv:.4f} n={n_obs} d={n_dist} dr={dr:.2f}",
                fontsize=6,
            )
            ax.tick_params(labelsize=6)
            ax.grid(alpha=0.15)

        for i in range(len(batch), per_fig):
            axes_flat[i].set_visible(False)

        fig.suptitle(
            f"H×CV <= 0.001 湖泊时序 ({fig_idx + 1}/{n_figs})",
            fontsize=13, fontweight="bold",
        )
        fig.tight_layout(rect=[0, 0, 1, 0.96])

        path = output_dir / f"hcv_lake_series_{fig_idx + 1}.png"
        fig.savefig(path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        log.info("Saved: %s", path)


if __name__ == "__main__":
    main()
