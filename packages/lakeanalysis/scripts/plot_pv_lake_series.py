"""Plot water_area time series for 100 randomly sampled pv-anomalous lakes.

4x5 grid per figure, 5 figures total = 100 lakes.

Usage:
    uv run python scripts/plot_pv_lake_series.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.postgres import fetch_lake_area_by_ids, series_db
from lakeanalysis.quality.filters import FLAG_NAMES, decode_anomaly_flags

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def _flag_label(flags: int) -> str:
    names = [FLAG_NAMES[b] for b in sorted(FLAG_NAMES) if flags & b]
    return "+".join(names) if names else "none"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme
    Theme.apply()

    with series_db.connection_context() as conn:
        meta = pd.read_sql(
            "SELECT hylak_id, anomaly_flags, rs_area_median, atlas_area "
            "FROM area_anomalies WHERE anomaly_flags & 16 = 16 "
            "ORDER BY RANDOM() LIMIT 100",
            conn,
        )

    if meta.empty:
        log.warning("No pv-anomalous lakes found")
        return

    log.info("Sampled %d pv-anomalous lakes", len(meta))

    id_list = meta["hylak_id"].tolist()
    with series_db.connection_context() as conn:
        lake_frames = fetch_lake_area_by_ids(conn, id_list)

    atlas_map = meta.set_index("hylak_id")["atlas_area"].to_dict()
    flags_map = meta.set_index("hylak_id")["anomaly_flags"].to_dict()

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
            flags = flags_map.get(hid, 0)
            atlas_km2 = atlas_map.get(hid, 0.0)

            if df is not None and not df.empty:
                dates = pd.to_datetime({"year": df["year"], "month": df["month"], "day": 1})
                area_km2 = df["water_area"].values / 1e6
                ax.plot(dates, area_km2, linewidth=0.6, color="steelblue")
                if atlas_km2 > 0:
                    ax.axhline(atlas_km2, color="red", linewidth=1.5, alpha=0.7)

            label = _flag_label(flags)
            ax.set_title(f"ID {hid} [{label}]", fontsize=7)
            ax.tick_params(labelsize=6)
            ax.grid(alpha=0.15)

        for i in range(len(batch), per_fig):
            axes_flat[i].set_visible(False)

        fig.suptitle(
            f"PV 异常湖泊时序 ({fig_idx + 1}/{n_figs})",
            fontsize=13, fontweight="bold",
        )
        fig.tight_layout(rect=[0, 0, 1, 0.96])

        path = output_dir / f"pv_lake_series_{fig_idx + 1}.png"
        fig.savefig(path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        log.info("Saved: %s", path)


if __name__ == "__main__":
    main()
