"""Plot single-lake PWM-Hawkes timeline with events and transition months.

Usage:
    uv run python scripts/plot_pwm_hawkes_lake.py --hylak-id 1023
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakesource.env import load_env
from lakesource.postgres import (
    fetch_hawkes_transition_monthly,
    fetch_lake_area_by_ids,
    fetch_pwm_extreme_extremes_by_id,
    series_db,
)
from lakeviz.domain.eot import draw_extremes_with_hawkes
from lakeviz.style.presets import Theme
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "pwm_hawkes" / "plots"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Single-lake PWM-Hawkes timeline plot.")
    parser.add_argument("--hylak-id", type=int, required=True)
    parser.add_argument("--annotate-top-n", type=int, default=8)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR)
    return parser.parse_args()


def _adapt_extremes(df: pd.DataFrame) -> pd.DataFrame:
    """Rename PWM extremes columns to match draw_extremes_with_hawkes expectations."""
    if df.empty:
        return df
    out = df.loc[:, ["year", "month", "water_area", "threshold", "event_type"]].copy()
    out = out.rename(
        columns={
            "threshold": "threshold_at_event",
            "event_type": "tail",
        }
    )
    out["tail"] = out["tail"].astype(str)
    return out


def _adapt_hawkes(df: pd.DataFrame) -> pd.DataFrame:
    """Select columns needed by draw_extremes_with_hawkes for Hawkes shading."""
    if df.empty:
        return df
    cols = ["year", "month", "direction"]
    available = [c for c in cols if c in df.columns]
    if not available:
        return pd.DataFrame(columns=cols)
    out = df.loc[:, available].copy()
    out["year"] = out["year"].astype(int)
    out["month"] = out["month"].astype(int)
    out["direction"] = out["direction"].astype(str)
    if "significant" in df.columns:
        out = out[df["significant"].astype(bool)].copy()
    return out


def run(
    hylak_id: int,
    annotate_top_n_each_tail: int = 8,
    output_dir: Path | None = None,
) -> Path:
    """Fetch data and save a single-lake PWM-Hawkes timeline plot."""
    output_dir = output_dir or DATA_DIR

    with series_db.connection_context() as conn:
        series_map = fetch_lake_area_by_ids(conn, [hylak_id])
        series_df = series_map.get(hylak_id)
        if series_df is None:
            raise ValueError(f"No lake_area series found for hylak_id={hylak_id}")

        extremes_raw = fetch_pwm_extreme_extremes_by_id(conn, hylak_id)
        hawkes_raw = fetch_hawkes_transition_monthly(
            conn, hylak_id=hylak_id
        )

    extremes_df = _adapt_extremes(extremes_raw)
    hawkes_df = _adapt_hawkes(hawkes_raw)

    if extremes_df.empty:
        log.warning("No PWM extremes found for hylak_id=%d", hylak_id)
    if hawkes_df.empty:
        log.warning("No Hawkes transition monthly data for hylak_id=%d", hylak_id)

    Theme.apply()
    output_dir.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(14, 6))
    ax = fig.add_subplot(111)
    draw_extremes_with_hawkes(
        ax,
        hylak_id,
        series_df,
        extremes_df if not extremes_df.empty else None,
        hawkes_df if not hawkes_df.empty else None,
        annotate_top_n_each_tail=annotate_top_n_each_tail,
    )
    # Adjust title to reflect PWM source
    ax.set_title(
        f"Lake {hylak_id} Area Timeline (PWM Extremes + Hawkes Transitions)"
    )

    out_path = output_dir / f"hylak_{hylak_id}_pwm_hawkes_timeline.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> None:
    load_env()
    args = parse_args()
    Logger("plot_pwm_hawkes_lake")
    out_path = run(
        hylak_id=args.hylak_id,
        annotate_top_n_each_tail=args.annotate_top_n,
        output_dir=args.output_dir,
    )
    log.info("Saved plot: %s", out_path)


if __name__ == "__main__":
    main()
