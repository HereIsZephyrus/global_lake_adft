"""Application runner for artificial lake similarity analysis."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakesource.postgres import series_db
from lakeviz.plot_config import setup_chinese_font
from lakeviz.similarity import (
    plot_acf_cosine_distribution,
    plot_pearson_distribution,
    plot_pearson_vs_acf,
)

from ..fetch import load_pairs_and_areas
from .compute import compute_pair_similarity

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SimilarityRunConfig:
    data_dir: Path
    limit_pairs: int | None = None
    show_plot: bool = False


def similarity_csv_path(data_dir: Path) -> Path:
    return data_dir / "similarity.csv"


def run_similarity(config: SimilarityRunConfig) -> None:
    """Load pairs and lake_area, compute similarity, write CSV and optionally plot."""
    config.data_dir.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        pairs, lake_frames = load_pairs_and_areas(conn)

    if not pairs:
        log.warning("No af_nearest pairs with topo_level>8 found.")
        return

    if config.limit_pairs is not None:
        pairs = pairs[: config.limit_pairs]
        log.info("Limited to first %d pairs", len(pairs))

    rows: list[dict[str, int | float]] = []
    for rec in pairs:
        hylak_id = rec["hylak_id"]
        nearest_id = rec["nearest_id"]
        df_a = lake_frames.get(hylak_id)
        df_b = lake_frames.get(nearest_id)
        if df_a is None or df_b is None:
            log.debug("Skip pair (%d, %d): missing lake_area", hylak_id, nearest_id)
            continue
        metrics = compute_pair_similarity(df_a, df_b)
        rows.append(
            {
                "hylak_id": hylak_id,
                "nearest_id": nearest_id,
                "topo_level": rec["topo_level"],
                "pearson_r": metrics["pearson_r"],
                "acf_cos_sim": metrics["acf_cos_sim"],
                "n_common": metrics["n_common"],
            }
        )

    if not rows:
        log.warning("No pairs with valid lake_area data.")
        return

    write_similarity_csv(config.data_dir, rows)
    log.info("Wrote %d rows to %s", len(rows), similarity_csv_path(config.data_dir))

    if config.show_plot:
        show_similarity_plots(config.data_dir)


def write_similarity_csv(data_dir: Path, rows: list[dict[str, int | float]]) -> None:
    fieldnames = ["hylak_id", "nearest_id", "topo_level", "pearson_r", "acf_cos_sim", "n_common"]
    with similarity_csv_path(data_dir).open("w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_similarity_summary(data_dir: Path) -> pd.DataFrame:
    csv_path = similarity_csv_path(data_dir)
    if not csv_path.exists():
        log.warning("CSV not found: %s", csv_path)
        return pd.DataFrame()
    return pd.read_csv(csv_path)


def show_similarity_plots(data_dir: Path) -> None:
    summary_df = load_similarity_summary(data_dir)
    if summary_df.empty:
        log.warning("No data for plotting.")
        return

    log.info("Plotting summary for %d pairs.", len(summary_df))
    setup_chinese_font()
    plot_dir = data_dir / "plot"
    plot_dir.mkdir(parents=True, exist_ok=True)

    fig = plot_pearson_distribution(summary_df)
    fig.savefig(plot_dir / "pearson_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_acf_cosine_distribution(summary_df)
    fig.savefig(plot_dir / "acf_cosine_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_pearson_vs_acf(summary_df)
    fig.savefig(plot_dir / "pearson_vs_acf.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    log.info("Saved plots to %s", plot_dir)
