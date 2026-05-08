"""Tests for quantile batch plot generation."""

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

# pylint: disable=wrong-import-position
from lakeanalysis.quantile import save_lake_plots, save_summary_plots, write_summary_cache
from lakeanalysis.quantile.summary import cache_root_for
# pylint: enable=wrong-import-position


def build_plot_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build minimal label and transition DataFrames for plot tests."""
    labels_df = pd.DataFrame(
        [
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 1,
                "water_area": 90.0,
                "monthly_climatology": 100.0,
                "anomaly": -10.0,
                "q_low": -8.0,
                "q_high": 8.0,
                "extreme_label": "extreme_low",
            },
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 2,
                "water_area": 112.0,
                "monthly_climatology": 100.0,
                "anomaly": 12.0,
                "q_low": -8.0,
                "q_high": 8.0,
                "extreme_label": "extreme_high",
            },
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 3,
                "water_area": 103.0,
                "monthly_climatology": 100.0,
                "anomaly": 3.0,
                "q_low": -8.0,
                "q_high": 8.0,
                "extreme_label": "normal",
            },
        ]
    )
    transitions_df = pd.DataFrame(
        [
            {
                "hylak_id": 101,
                "from_year": 2000,
                "from_month": 1,
                "to_year": 2000,
                "to_month": 2,
                "transition_type": "low_to_high",
                "from_anomaly": -10.0,
                "to_anomaly": 12.0,
                "from_label": "extreme_low",
                "to_label": "extreme_high",
            }
        ]
    )
    return labels_df, transitions_df


def test_save_plot_outputs(tmp_path: Path) -> None:
    """Verify lake-level and summary plots are saved as non-empty files."""
    labels_df, transitions_df = build_plot_inputs()
    lake_paths = save_lake_plots(labels_df, transitions_df, tmp_path, hylak_id=101)
    cache_root = cache_root_for(tmp_path)
    write_summary_cache(
        cache_root,
        transition_counts=pd.DataFrame(
            [{"transition_type": "low_to_high", "count": 1}]
        ),
        transition_seasonality=pd.DataFrame(
            [{"to_month": 2, "count": 1}]
        ),
        lake_transition_counts=pd.DataFrame(
            [{"hylak_id": 101, "transition_count": 1}]
        ),
        lake_extreme_counts=pd.DataFrame(
            [{"hylak_id": 101, "extreme_count": 2}]
        ),
        run_metadata={"done_count": 1, "error_count": 0},
    )
    summary_paths = save_summary_plots(cache_root, tmp_path)

    for output_path in list(lake_paths.values()) + list(summary_paths.values()):
        assert output_path.exists()
        assert output_path.stat().st_size > 0
