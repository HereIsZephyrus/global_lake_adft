"""Tests for shared Hawkes pipeline helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.hawkes.pipeline import (
    build_hawkes_result_row,
    build_hawkes_transition_monthly_rows,
    quantile_string,
)
from lakeanalysis.hawkes.types import (
    HawkesEventSeries,
    TYPE_DRY,
    TYPE_WET,
)


def _make_series(n: int = 240):
    records = []
    for year in range(2000, 2000 + n // 12):
        for month in range(1, 13):
            records.append(
                {"year": year, "month": month, "water_area": 100.0 + 10.0 * np.sin(2 * np.pi * month / 12)}
            )
    return pd.DataFrame(records).iloc[:n]


def _make_event_series(series_df, n_events=20):
    times = np.linspace(2000.0, 2003.0, n_events)
    event_types = np.array([TYPE_DRY if i % 3 == 0 else TYPE_WET for i in range(n_events)], dtype=int)
    events_table = pd.DataFrame(
        {
            "time": times,
            "year": np.floor(times).astype(int),
            "month": [1 + i % 12 for i in range(n_events)],
            "event_type": event_types,
            "water_area": [180.0 if t == TYPE_WET else 30.0 for t in event_types],
            "threshold": [150.0] * n_events,
            "severity": [30.0 if t == TYPE_WET else 10.0 for t in event_types],
        }
    )
    timeline = series_df.loc[:, ["year", "month"]].copy()
    timeline["time"] = timeline["year"] + (timeline["month"] - 1) / 12.0
    return HawkesEventSeries(
        times=times,
        event_types=event_types,
        start_time=float(times[0]),
        end_time=float(times[-1] + 1.0 / 12.0),
        timeline=timeline,
        events_table=events_table,
    ), events_table


class TestQuantileString:
    def test_returns_string(self):
        result = quantile_string(0.95)
        assert isinstance(result, str)
        assert result == "0.95"

    def test_deterministic(self):
        a = quantile_string(0.5)
        b = quantile_string(0.5)
        assert a == b


class TestBuildHawkesResultRow:
    def test_success_row(self):
        summary = {
            "hylak_id": 1,
            "threshold_quantile": 0.95,
            "converged": True,
            "message": "ok",
            "n_events": 50,
            "n_dry_events": 20,
            "n_wet_events": 30,
            "log_likelihood": -123.4,
            "objective_value": 0.5,
            "mu_D": 0.1,
            "mu_W": 0.2,
            "alpha_DD": 0.3,
            "alpha_DW": 0.0,
            "alpha_WD": 0.1,
            "alpha_WW": 0.2,
            "beta_DD": 1.0,
            "beta_DW": 1.0,
            "beta_WD": 1.0,
            "beta_WW": 1.0,
            "spectral_radius": 0.5,
            "lrt_p_D_to_W": 0.001,
            "lrt_p_W_to_D": 0.5,
            "qc_pass": True,
            "qc_event_rate": 0.1,
            "qc_relative_amplitude": 0.2,
            "qc_median_severity": 15.0,
            "error_message": None,
        }
        row = build_hawkes_result_row(summary)
        assert row["hylak_id"] == 1
        assert row["converged"] is True
        assert row["mu_d"] == 0.1
        assert row["alpha_dd"] == 0.3

    def test_error_row(self):
        summary = {
            "hylak_id": 2,
            "threshold_quantile": 0.95,
            "converged": False,
            "message": "error",
            "n_events": None,
            "n_dry_events": None,
            "n_wet_events": None,
            "log_likelihood": None,
            "objective_value": None,
            "mu_D": None,
            "mu_W": None,
            "alpha_DD": None,
            "alpha_DW": None,
            "alpha_WD": None,
            "alpha_WW": None,
            "beta_DD": None,
            "beta_DW": None,
            "beta_WD": None,
            "beta_WW": None,
            "spectral_radius": None,
            "lrt_p_D_to_W": None,
            "lrt_p_W_to_D": None,
            "qc_pass": False,
            "qc_event_rate": None,
            "qc_relative_amplitude": None,
            "qc_median_severity": None,
            "error_message": "something wrong",
        }
        row = build_hawkes_result_row(summary)
        assert row["hylak_id"] == 2
        assert row["error_message"] == "something wrong"


class TestBuildHawkesTransitionMonthlyRows:
    def test_empty_decomposition(self):
        rows = build_hawkes_transition_monthly_rows(
            hylak_id=1,
            threshold_quantile=0.95,
            decomposition=pd.DataFrame(),
            timeline=pd.DataFrame(),
            significance_quantile=0.95,
        )
        assert rows == []

    def test_non_empty(self):
        timeline = pd.DataFrame(
            {
                "year": [2000, 2000],
                "month": [1, 2],
                "time": [2000.0, 2000.08333],
            }
        )
        decomposition = pd.DataFrame(
            {
                "time": [2000.0, 2000.08333],
                "cross_D": [0.1, 0.2],
                "cross_W": [0.05, 0.15],
                "lambda_D": [0.5, 0.6],
                "lambda_W": [0.3, 0.4],
            }
        )
        rows = build_hawkes_transition_monthly_rows(
            hylak_id=1,
            threshold_quantile=0.95,
            decomposition=decomposition,
            timeline=timeline,
            significance_quantile=0.95,
        )
        assert len(rows) == 4
        for row in rows:
            assert row["hylak_id"] == 1
            assert row["direction"] in ("D_to_W", "W_to_D")
