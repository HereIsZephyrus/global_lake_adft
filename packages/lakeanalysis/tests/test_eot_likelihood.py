import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.likelihood import NHPPLogLikelihood
from lakeanalysis.eot.models import LocationModel
from lakeanalysis.eot.series import MonthlyTimeSeries


def _make_series(years=3):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            rows.append({
                "year": year,
                "month": month,
                "water_area": 100.0,
            })
    return MonthlyTimeSeries.from_frame(pd.DataFrame(rows))


def _make_extremes(n=5):
    rng = np.random.default_rng(42)
    values = rng.uniform(100, 120, n)
    return pd.DataFrame({
        "year": [2000] * n,
        "month": list(range(6, 6 + n)),
        "time": np.linspace(0.4, 2.0, n),
        "value": values,
        "original_value": values,
        "cluster_id": list(range(1, n + 1)),
        "cluster_size": [1] * n,
        "threshold": np.full(n, 90.0, dtype=float),
    })


class TestConstruction:
    def test_empty_extremes_raises(self):
        series = _make_series()
        lm = LocationModel()
        with pytest.raises(ValueError, match="At least one declustered exceedance"):
            NHPPLogLikelihood(
                series=series,
                extremes=pd.DataFrame(),
                threshold=80.0,
                location_model=lm,
            )

    def test_u_grid_shape_mismatch_raises(self):
        series = _make_series()
        lm = LocationModel()
        extremes = _make_extremes()
        with pytest.raises(ValueError, match="u_grid length"):
            NHPPLogLikelihood(
                series=series,
                extremes=extremes,
                threshold=80.0,
                location_model=lm,
                integration_points=256,
                u_grid=np.array([1.0, 2.0]),
            )

    def test_u_grid_none_defaults_to_constant(self):
        series = _make_series()
        lm = LocationModel()
        extremes = _make_extremes()
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=80.0,
            location_model=lm,
            integration_points=256,
            u_grid=None,
        )
        np.testing.assert_array_equal(nll.u_grid, np.full(256, 80.0))

    def test_integration_grid_endpoints(self):
        series = _make_series()
        lm = LocationModel()
        extremes = _make_extremes()
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=80.0,
            location_model=lm,
        )
        assert nll.integration_grid[0] == 0.0
        assert nll.integration_grid[-1] == pytest.approx(series.duration_years)


class TestLikelihoodEvaluation:
    def test_sigma_negative_returns_inf(self):
        series = _make_series()
        lm = LocationModel()
        extremes = _make_extremes()
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=80.0,
            location_model=lm,
        )
        theta = np.array([100.0, 0.0, 0.0, 0.0, -1.0, 0.1])
        result = nll(theta)
        assert result == float("inf")

    def test_split_theta(self):
        series = _make_series()
        lm = LocationModel(n_harmonics=1, include_trend=True)
        extremes = _make_extremes()
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=80.0,
            location_model=lm,
        )
        theta = np.array([10.0, 1.0, 2.0, 3.0, 5.0, 0.2])
        loc, sigma, xi = nll._split_theta(theta)
        assert len(loc) == lm.n_params
        assert sigma == pytest.approx(5.0)
        assert xi == pytest.approx(0.2)

    def test_gumbel_branch_xi_zero(self):
        series = _make_series()
        lm = LocationModel(n_harmonics=1, include_trend=False)
        extremes = _make_extremes()
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=80.0,
            location_model=lm,
        )
        theta = np.array([100.0, 0.0, 0.0, 10.0, 0.0])
        result = nll(theta)
        assert np.isfinite(result)

    def test_gp_branch_xi_nonzero(self):
        series = _make_series()
        lm = LocationModel(n_harmonics=1, include_trend=False)
        extremes = _make_extremes()
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=80.0,
            location_model=lm,
        )
        theta = np.array([100.0, 0.0, 0.0, 10.0, 0.1])
        result = nll(theta)
        assert np.isfinite(result)

    def test_extreme_times_and_values_set_correctly(self):
        series = _make_series()
        lm = LocationModel()
        extremes = _make_extremes(n=3)
        nll = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=85.0,
            location_model=lm,
        )
        assert len(nll.extreme_times) == 3
        assert len(nll.extreme_values) == 3
        assert nll.threshold == 85.0
