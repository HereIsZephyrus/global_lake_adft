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

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakesource.provider import create_provider
from lakeviz.domain.eot import draw_extremes_with_hawkes
from lakeviz.style.presets import Theme
from lakeanalysis.logger import Logger
from lakeanalysis.pwm_extreme.compute import compute_monthly_thresholds
from lakeanalysis.pwm_extreme.events import run_runs_declustering
from lakesource.pwm_extreme.schema import PWMExtremeConfig

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = ROOT / "data" / "figures" / "pwm_hawkes"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Single-lake PWM-Hawkes timeline plot.")
    parser.add_argument("--hylak-id", type=int, required=True)
    parser.add_argument("--annotate-top-n", type=int, default=8)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR)
    return parser.parse_args()


def _compute_extremes_in_memory(
    hylak_id: int, series_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compute PWM extreme events in-memory from lake_area data."""
    config = PWMExtremeConfig(min_observations_per_month=2)
    result = compute_monthly_thresholds(series_df, hylak_id=hylak_id, config=config)
    extremes = result.extremes_df.copy()
    if extremes.empty:
        return pd.DataFrame(columns=["year", "month", "water_area", "threshold_at_event", "tail"])
    declustered = run_runs_declustering(extremes, run_length=1)
    events = declustered.rename(
        columns={"threshold": "threshold_at_event", "event_type": "tail"}
    )
    events["tail"] = events["tail"].astype(str)
    return events


def _read_hawkes_transitions(hylak_id: int) -> pd.DataFrame:
    """Read hawkes_transition_monthly from parquet for a single lake."""
    config = SourceConfig()
    parquet_path = config.data_dir / "pwm_hawkes_transition_monthly.parquet"
    if not parquet_path.exists():
        return pd.DataFrame(columns=["year", "month", "direction"])
    df = pd.read_parquet(parquet_path)
    df = df[df["hylak_id"] == hylak_id]
    if df.empty:
        return df
    out = df.loc[:, ["year", "month", "direction"]].copy()
    out["year"] = out["year"].astype(int)
    out["month"] = out["month"].astype(int)
    out["direction"] = out["direction"].astype(str)
    if "significant" in df.columns:
        sig_mask = df["significant"].fillna(False).astype(bool)
        out = out[sig_mask].copy()
    return out


def run(
    hylak_id: int,
    annotate_top_n_each_tail: int = 8,
    output_dir: Path | None = None,
) -> Path:
    """Fetch data and save a single-lake PWM-Hawkes timeline plot."""
    output_dir = output_dir or DATA_DIR
    load_env()

    config = SourceConfig()
    provider = create_provider(config)
    lake_map = provider.fetch_lake_area_by_ids([hylak_id])
    if hylak_id not in lake_map:
        raise ValueError(f"No lake_area series found for hylak_id={hylak_id}")

    series_df = lake_map[hylak_id]
    extremes_df = _compute_extremes_in_memory(hylak_id, series_df)
    hawkes_df = _read_hawkes_transitions(hylak_id)

    if extremes_df.empty:
        log.warning("No PWM extremes found for hylak_id=%d", hylak_id)
    if hawkes_df.empty:
        log.warning("No Hawkes transition data for hylak_id=%d", hylak_id)

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
    ax.set_title(
        f"Lake {hylak_id} Area Timeline (PWM Extremes + Hawkes Transitions)"
    )

    out_path = output_dir / f"hylak_{hylak_id}_pwm_hawkes_timeline.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> None:
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
