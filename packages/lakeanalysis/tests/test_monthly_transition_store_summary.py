from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from lakeanalysis.monthly_transition import (
    MonthlyTransitionResult,
    load_summary_cache,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
    save_summary_plots_from_cache,
    write_summary_cache,
)


def build_result() -> MonthlyTransitionResult:
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
            }
        ]
    )
    extremes_df = pd.DataFrame(
        [
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 1,
                "event_type": "low",
                "water_area": 90.0,
                "monthly_climatology": 100.0,
                "anomaly": -10.0,
                "threshold": -8.0,
            }
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
                "to_anomaly": 11.0,
                "from_label": "extreme_low",
                "to_label": "extreme_high",
            }
        ]
    )
    return MonthlyTransitionResult(
        hylak_id=101,
        climatology_df=pd.DataFrame(),
        labels_df=labels_df,
        extremes_df=extremes_df,
        transitions_df=transitions_df,
        q_low=-8.0,
        q_high=8.0,
    )


def test_store_row_helpers_shape_expected_columns() -> None:
    result = build_result()

    label_rows = result_to_label_rows(result, workflow_version="test-v1")
    extreme_rows = result_to_extreme_rows(result, workflow_version="test-v1")
    transition_rows = result_to_transition_rows(result, workflow_version="test-v1")
    status_row = make_run_status_row(
        hylak_id=101,
        chunk_start=0,
        chunk_end=1000,
        workflow_version="test-v1",
        status="done",
        error_message=None,
    )

    assert label_rows[0]["extreme_label"] == "extreme_low"
    assert label_rows[0]["workflow_version"] == "test-v1"
    assert extreme_rows[0]["event_type"] == "low"
    assert transition_rows[0]["transition_type"] == "low_to_high"
    assert status_row["chunk_end"] == 1000


def test_summary_cache_and_plots_roundtrip(tmp_path: Path) -> None:
    cache_paths = write_summary_cache(
        tmp_path / "summary_cache",
        transition_counts=pd.DataFrame(
            [{"transition_type": "low_to_high", "count": 3}]
        ),
        transition_seasonality=pd.DataFrame([{"to_month": 2, "count": 3}]),
        lake_transition_counts=pd.DataFrame([{"hylak_id": 101, "transition_count": 3}]),
        lake_extreme_counts=pd.DataFrame([{"hylak_id": 101, "extreme_count": 6}]),
        run_status=pd.DataFrame(
            [{"status": "done", "count": 10}, {"status": "error", "count": 1}]
        ),
    )

    loaded = load_summary_cache(tmp_path / "summary_cache")
    plot_paths = save_summary_plots_from_cache(
        cache_root=tmp_path / "summary_cache",
        output_root=tmp_path / "summary",
    )

    assert cache_paths["run_metadata"].exists()
    assert loaded["run_metadata"]["done_count"] == 10
    for output_path in plot_paths.values():
        assert output_path.exists()
        assert output_path.stat().st_size > 0
