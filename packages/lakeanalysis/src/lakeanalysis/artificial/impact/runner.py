"""Application runner for artificial lake impact analysis."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakesource.postgres import series_db
from lakeviz.artificial import (
    plot_anomaly_ratio_comparison,
    plot_delta_cv_distribution,
    plot_typical_pair_timeline,
    plot_volatility_comparison,
)

from ..fetch import load_pairs_and_areas
from .events import compute_pair_events
from .metrics import compute_pair_metrics

log = logging.getLogger(__name__)

_FIELDNAMES = [
    "hylak_id", "nearest_id", "topo_level",
    "cv_a", "pct_change_std_a", "range_ratio_a", "n_obs_a",
    "cv_n", "pct_change_std_n", "range_ratio_n", "n_obs_n",
    "delta_cv", "delta_pct_change_std", "delta_range_ratio",
    "n_events_a", "anomaly_ratio_a",
    "n_events_n", "anomaly_ratio_n",
    "n_unique_a", "delta_anomaly_ratio",
]


@dataclass(frozen=True)
class ImpactRunConfig:
    data_dir: Path
    limit_pairs: int | None = None
    z_threshold: float = 3.0
    show_plot: bool = False


def impact_csv_path(data_dir: Path) -> Path:
    return data_dir / "impact.csv"


def run_impact(config: ImpactRunConfig) -> None:
    """Load pairs and lake_area, compute impact metrics, write CSV and optionally plot."""
    config.data_dir.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        pairs, lake_frames = load_pairs_and_areas(conn)

    if not pairs:
        log.warning("No af_nearest pairs with topo_level>8 found after quality filtering.")
        return

    if config.limit_pairs is not None:
        pairs = pairs[: config.limit_pairs]
        log.info("Limited to first %d pairs", len(pairs))

    rows: list[dict[str, int | float]] = []
    for rec in pairs:
        hylak_id = rec["hylak_id"]
        nearest_id = rec["nearest_id"]
        df_a = lake_frames.get(hylak_id)
        df_n = lake_frames.get(nearest_id)
        if df_a is None or df_n is None:
            log.debug("Skip pair (%d, %d): missing lake_area", hylak_id, nearest_id)
            continue

        vol = compute_pair_metrics(df_a, df_n)
        evt = compute_pair_events(df_a, df_n, threshold=config.z_threshold)
        rows.append(
            {
                "hylak_id": hylak_id,
                "nearest_id": nearest_id,
                "topo_level": rec["topo_level"],
                "cv_a": vol["cv_a"],
                "cv_n": vol["cv_n"],
                "delta_cv": vol["delta_cv"],
                "pct_change_std_a": vol["pct_change_std_a"],
                "pct_change_std_n": vol["pct_change_std_n"],
                "delta_pct_change_std": vol["delta_pct_change_std"],
                "range_ratio_a": vol["range_ratio_a"],
                "range_ratio_n": vol["range_ratio_n"],
                "delta_range_ratio": vol["delta_range_ratio"],
                "n_obs_a": vol["n_obs_a"],
                "n_obs_n": vol["n_obs_n"],
                "n_events_a": evt["n_events_a"],
                "n_events_n": evt["n_events_n"],
                "anomaly_ratio_a": evt["anomaly_ratio_a"],
                "anomaly_ratio_n": evt["anomaly_ratio_n"],
                "delta_anomaly_ratio": evt["delta_anomaly_ratio"],
                "n_unique_a": evt["n_unique_a"],
                "z_threshold": config.z_threshold,
            }
        )

    if not rows:
        log.warning("No pairs with valid lake_area data.")
        return

    write_impact_csv(config.data_dir, rows)
    log.info("Wrote %d rows to %s", len(rows), impact_csv_path(config.data_dir))

    if config.show_plot:
        show_impact_plots(config.data_dir, rows, lake_frames)


def write_impact_csv(data_dir: Path, rows: list[dict[str, int | float]]) -> None:
    with impact_csv_path(data_dir).open("w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_impact_csv(data_dir: Path) -> pd.DataFrame:
    csv_path = impact_csv_path(data_dir)
    if not csv_path.exists():
        log.warning("CSV not found: %s", csv_path)
        return pd.DataFrame()
    return pd.read_csv(csv_path)


def show_impact_plots(
    data_dir: Path,
    rows: list[dict[str, int | float]] | None = None,
    lake_frames: dict[int, pd.DataFrame] | None = None,
) -> None:
    impact_df = pd.DataFrame(rows) if rows is not None else load_impact_csv(data_dir)
    if impact_df.empty:
        log.warning("No data for plotting.")
        return

    log.info("Plotting impact summary for %d pairs.", len(impact_df))
    plot_dir = data_dir / "plot"
    plot_dir.mkdir(parents=True, exist_ok=True)

    fig = plot_volatility_comparison(impact_df)
    fig.savefig(plot_dir / "volatility_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_delta_cv_distribution(impact_df)
    fig.savefig(plot_dir / "delta_cv_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_anomaly_ratio_comparison(impact_df)
    fig.savefig(plot_dir / "anomaly_ratio_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    if lake_frames is None:
        return

    top_pairs = impact_df.nlargest(3, "delta_cv")
    for _, pair in top_pairs.iterrows():
        hylak_id = int(pair["hylak_id"])
        nearest_id = int(pair["nearest_id"])
        df_a = lake_frames.get(hylak_id)
        df_n = lake_frames.get(nearest_id)
        if df_a is None or df_n is None:
            continue
        fig = plot_typical_pair_timeline(df_a, df_n, pair.to_dict())
        fig.savefig(plot_dir / f"timeline_{hylak_id}_{nearest_id}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)

    log.info("Saved plots to %s", plot_dir)
