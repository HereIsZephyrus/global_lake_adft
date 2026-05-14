"""Tests for shared EVT algorithm helpers in evt_common.py."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.extreme.evt import (
    ROUTE_A,
    ROUTE_B,
    build_empty_tail_summary_rows,
    build_fitted_tail_summary_rows,
    compute_return_level,
    fit_gpd_exceedances,
)


class TestFitGpdExceedances:
    def test_normal_fit(self):
        shape, scale = fit_gpd_exceedances(np.array([1.0, 2.0, 3.0, 4.0, 5.0]))
        assert np.isfinite(shape)
        assert scale > 0.0

    def test_insufficient_samples(self):
        with pytest.raises(ValueError, match="Need at least 3"):
            fit_gpd_exceedances(np.array([1.0, 2.0]))

    def test_negative_exceedance(self):
        with pytest.raises(ValueError, match="must be non-negative"):
            fit_gpd_exceedances(np.array([1.0, -1.0, 3.0]))


class TestComputeReturnLevel:
    def test_rate_le_one_returns_threshold(self):
        rl = compute_return_level(
            threshold=10.0, shape=0.2, scale=1.0, return_period=1, n_total=100, n_exceedances=50,
        )
        assert rl == 10.0

    def test_shape_near_zero(self):
        rl = compute_return_level(
            threshold=10.0, shape=1e-15, scale=2.0, return_period=50, n_total=100, n_exceedances=50,
        )
        assert rl > 10.0

    def test_positive_shape(self):
        rl = compute_return_level(
            threshold=10.0, shape=0.3, scale=2.0, return_period=50, n_total=100, n_exceedances=50,
        )
        assert rl > 10.0


class TestBuildEmptyTailSummaryRows:
    def test_returns_correct_number_of_rows(self):
        rows = build_empty_tail_summary_rows(
            tail="high", n_total=100, return_periods=(2, 5, 10),
            evt_route=ROUTE_A, strength_unit="index_value",
        )
        assert len(rows) == 3
        assert all(r["tail"] == "high" for r in rows)
        assert all(r["converged"] is False for r in rows)
        assert all(r["evt_route"] == ROUTE_A for r in rows)
        assert all(r["error_message"] == "No exceedances" for r in rows)

    def test_route_b(self):
        rows = build_empty_tail_summary_rows(
            tail="low", n_total=100, return_periods=(2,),
            evt_route=ROUTE_B, strength_unit="stl_residual",
        )
        assert len(rows) == 1
        assert rows[0]["evt_route"] == ROUTE_B
        assert rows[0]["strength_unit"] == "stl_residual"


class TestBuildFittedTailSummaryRows:
    def test_empty_tail_df_falls_back(self):
        empty_df = pd.DataFrame(columns=["threshold", "exceedance"])
        rows = build_fitted_tail_summary_rows(
            empty_df,
            tail="high", n_total=100, return_periods=(2, 5),
            evt_route=ROUTE_A, strength_unit="index_value",
        )
        assert all(r["converged"] is False for r in rows)
        assert all(r["error_message"] == "No exceedances" for r in rows)

    def test_successful_fit(self):
        tail_df = pd.DataFrame(
            {"threshold": [90.0, 90.0, 90.0, 90.0], "exceedance": [5.0, 6.0, 7.0, 8.0]}
        )
        rows = build_fitted_tail_summary_rows(
            tail_df,
            tail="high", n_total=100, return_periods=(2, 5),
            evt_route=ROUTE_A, strength_unit="index_value",
        )
        assert len(rows) == 2
        assert all(r["converged"] is True for r in rows)
        assert all(r["tail"] == "high" for r in rows)
        assert all(r["return_period"] in (2, 5) for r in rows)

    def test_insufficient_exceedances(self):
        tail_df = pd.DataFrame(
            {"threshold": [90.0], "exceedance": [5.0]}
        )
        rows = build_fitted_tail_summary_rows(
            tail_df,
            tail="high", n_total=100, return_periods=(10, 50),
            evt_route=ROUTE_A, strength_unit="index_value",
        )
        assert all(r["converged"] is False for r in rows)
        assert all(r["return_level"] is None for r in rows)
