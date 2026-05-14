import pandas as pd
import pytest
import warnings

from lakeanalysis.decomposition import MonthlyClimatologyMethod
from lakeanalysis.quantile import (
    detect_abrupt_transitions,
    run_monthly_anomaly_transition,
    validate_monthly_series,
)


def build_five_year_series() -> pd.DataFrame:
    rows = []
    year_offsets = {
        2000: -20.0,
        2001: -10.0,
        2002: 0.0,
        2003: 10.0,
        2004: 20.0,
    }
    for year, offset in year_offsets.items():
        for month in range(1, 13):
            rows.append(
                {
                    "year": year,
                    "month": month,
                    "water_area": 100.0 + month + offset,
                }
            )
    return pd.DataFrame(rows)


def test_validate_monthly_series_deduplicates_months() -> None:
    duplicated = pd.DataFrame(
        [
            {"year": 2000, "month": 1, "water_area": 101.0},
            {"year": 2000, "month": 1, "water_area": 102.0},
        ]
    )
    result = validate_monthly_series(duplicated)
    assert len(result) == 1
    assert result.iloc[0]["water_area"] == 101.0


def test_run_monthly_anomaly_transition_computes_expected_extremes() -> None:
    series_df = build_five_year_series()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        method = MonthlyClimatologyMethod()
        decomp = method.decompose(series_df)

    result = run_monthly_anomaly_transition(
        decomp,
        hylak_id=101,
    )

    assert result.q_low == pytest.approx(-20.0)
    assert result.q_high == pytest.approx(20.0)

    low_count = (result.labels_df["extreme_label"] == "extreme_low").sum()
    high_count = (result.labels_df["extreme_label"] == "extreme_high").sum()
    assert low_count == 12
    assert high_count == 12

    grouped_means = result.labels_df.groupby("month")["index_value"].mean()
    assert grouped_means.abs().max() == pytest.approx(0.0)


def test_frozen_months_are_excluded_from_outputs() -> None:
    frozen_keys = {200001, 200402}
    series_df = build_five_year_series()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        method = MonthlyClimatologyMethod()
        decomp = method.decompose(series_df)

    # Apply frozen filtering manually (pre-decomposition in real pipeline)
    df = decomp.index_df
    year_month = df["year"].astype(int) * 100 + df["month"].astype(int)
    df = df.loc[~year_month.isin(frozen_keys)].reset_index(drop=True)

    from lakeanalysis.decomposition.base import DecompositionResult
    decomp_filtered = DecompositionResult(index_df=df, metadata=decomp.metadata)

    result = run_monthly_anomaly_transition(
        decomp_filtered,
        hylak_id=101,
    )

    remaining_keys = set((result.labels_df["year"] * 100 + result.labels_df["month"]).tolist())
    assert frozen_keys.isdisjoint(remaining_keys)


def test_detect_abrupt_transitions_requires_true_calendar_adjacency() -> None:
    labeled_df = pd.DataFrame(
        [
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 1,
                "month_ordinal": 2000 * 12,
                "index_value": -5.0,
                "extreme_label": "extreme_low",
            },
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 2,
                "month_ordinal": 2000 * 12 + 1,
                "anomaly": 6.0,
                "extreme_label": "extreme_high",
            },
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 4,
                "month_ordinal": 2000 * 12 + 3,
                "anomaly": -7.0,
                "extreme_label": "extreme_low",
            },
            {
                "hylak_id": 101,
                "year": 2000,
                "month": 6,
                "month_ordinal": 2000 * 12 + 5,
                "anomaly": 8.0,
                "extreme_label": "extreme_high",
            },
        ]
    )

    transitions_df = detect_abrupt_transitions(labeled_df)

    assert len(transitions_df) == 1
    assert transitions_df.iloc[0]["transition_type"] == "low_to_high"
    assert transitions_df.iloc[0]["from_month"] == 1
    assert transitions_df.iloc[0]["to_month"] == 2
