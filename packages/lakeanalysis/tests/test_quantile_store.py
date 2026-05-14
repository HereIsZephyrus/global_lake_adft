import pandas as pd
import pytest
import warnings

from lakeanalysis.decomposition import MonthlyClimatologyMethod
from lakeanalysis.quantile import (
    RUN_STATUS_DONE,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
    run_monthly_anomaly_transition,
)


def _build_series() -> pd.DataFrame:
    rows = []
    for year, offset in ((2000, -20.0), (2001, 0.0), (2002, 20.0)):
        for month in range(1, 13):
            rows.append(
                {
                    "year": year,
                    "month": month,
                    "water_area": 100.0 + month + offset,
                }
            )
    return pd.DataFrame(rows)


def test_result_row_shapers_emit_expected_keys() -> None:
    series_df = _build_series()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        method = MonthlyClimatologyMethod()
        decomp = method.decompose(series_df)

    result = run_monthly_anomaly_transition(
        decomp,
        hylak_id=123,
    )

    label_rows = result_to_label_rows(result)
    extreme_rows = result_to_extreme_rows(result)
    transition_rows = result_to_transition_rows(result)

    assert label_rows
    assert set(label_rows[0]) == {
        "hylak_id",
        "year",
        "month",
        "water_area",
        "monthly_climatology",
        "anomaly",
        "q_low",
        "q_high",
        "extreme_label",
    }
    assert set(extreme_rows[0]) == {
        "hylak_id",
        "year",
        "month",
        "event_type",
        "water_area",
        "monthly_climatology",
        "anomaly",
        "threshold",
        "extreme_label",
    }
    assert isinstance(transition_rows, list)


def test_make_run_status_row_validates_and_truncates_message() -> None:
    row = make_run_status_row(
        hylak_id=5,
        chunk_start=0,
        chunk_end=100,
        status=RUN_STATUS_DONE,
        error_message="x" * 800,
    )
    assert row["status"] == RUN_STATUS_DONE
    assert len(row["error_message"]) == 500

    with pytest.raises(ValueError, match="Invalid run status"):
        make_run_status_row(1, 0, 10, "bad-status")
