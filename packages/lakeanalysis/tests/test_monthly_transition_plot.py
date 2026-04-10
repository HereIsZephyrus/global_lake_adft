from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from lakeanalysis.monthly_transition import save_lake_plots, save_summary_plots


def build_plot_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
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
    labels_df, transitions_df = build_plot_inputs()
    lake_paths = save_lake_plots(labels_df, transitions_df, tmp_path, hylak_id=101)
    summary_paths = save_summary_plots(transitions_df, tmp_path)

    for output_path in list(lake_paths.values()) + list(summary_paths.values()):
        assert output_path.exists()
        assert output_path.stat().st_size > 0
