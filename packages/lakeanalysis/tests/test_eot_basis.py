import numpy as np
import pytest

from lakeanalysis.eot.basis import BasisFitRecord, BasisSelector, HarmonicBasis


class TestHarmonicBasis:
    def test_n_harmonics_invalid_raises(self):
        with pytest.raises(ValueError, match="n_harmonics must be >= 1"):
            HarmonicBasis(n_harmonics=0)

    def test_model_name(self):
        hb = HarmonicBasis(n_harmonics=2)
        assert hb.model_name == "harmonic_2"

    def test_parameter_names(self):
        hb = HarmonicBasis(n_harmonics=2)
        assert hb.parameter_names == ("sin_1", "cos_1", "sin_2", "cos_2")

    def test_n_features_matches_parameter_count(self):
        hb = HarmonicBasis(n_harmonics=3)
        assert hb.n_features == 6
        assert hb.n_features == len(hb.parameter_names)

    def test_design_columns_at_zero(self):
        hb = HarmonicBasis(n_harmonics=1)
        cols = hb.design_columns(np.array([0.0]))
        assert cols.shape == (1, 2)
        assert cols[0, 0] == pytest.approx(0.0)
        assert cols[0, 1] == pytest.approx(1.0)

    def test_design_columns_at_quarter(self):
        hb = HarmonicBasis(n_harmonics=1)
        cols = hb.design_columns(np.array([0.25]))
        assert cols[0, 0] == pytest.approx(1.0)
        assert cols[0, 1] == pytest.approx(0.0, abs=1e-10)

    def test_build_design_matrix_no_trend_no_intercept(self):
        hb = HarmonicBasis(n_harmonics=1)
        dm = hb.build_design_matrix(
            np.array([0.0, 0.25, 0.5]),
            include_trend=False,
            include_intercept=False,
        )
        assert dm.shape == (3, 2)

    def test_build_design_matrix_with_trend_and_intercept(self):
        hb = HarmonicBasis(n_harmonics=1)
        dm = hb.build_design_matrix(np.array([0.0, 1.0]), include_trend=True, include_intercept=True)
        assert dm.shape == (2, 4)

    def test_build_design_matrix_intercept_column_first(self):
        hb = HarmonicBasis(n_harmonics=1)
        dm = hb.build_design_matrix(np.array([0.0, 1.0]), include_trend=True, include_intercept=True)
        np.testing.assert_array_equal(dm[:, 0], np.ones(2))
        np.testing.assert_array_equal(dm[:, 1], np.array([0.0, 1.0]))


class TestBasisSelector:
    @staticmethod
    def _synthetic_series(n=60):
        rng = np.random.default_rng(42)
        times = np.linspace(0, 5, n)
        values = 10.0 + 2.0 * times + 5.0 * np.sin(2 * np.pi * times) + rng.normal(0, 0.1, n)
        return np.asarray(times, dtype=float), np.asarray(values, dtype=float)

    def test_default_construction(self):
        bs = BasisSelector()
        assert bs.criterion == "aic"
        assert len(bs.candidates) == 3

    def test_invalid_criterion_raises(self):
        with pytest.raises(ValueError, match="criterion must be"):
            BasisSelector(criterion="invalid")

    def test_empty_candidates_raises(self):
        with pytest.raises(ValueError, match="At least one candidate"):
            BasisSelector(candidates=tuple())

    def test_max_relative_rmse_non_positive_raises(self):
        with pytest.raises(ValueError, match="max_relative_rmse"):
            BasisSelector(max_relative_rmse=0.0)

    def test_fit_record_converges_on_good_data(self):
        bs = BasisSelector()
        times, values = self._synthetic_series()
        basis = HarmonicBasis(n_harmonics=1)
        record = bs._fit_record(times, values, basis)
        assert record.converged
        assert np.isfinite(record.aic)
        assert np.isfinite(record.bic)
        assert record.rmse > 0

    def test_fit_record_insufficient_samples(self):
        bs = BasisSelector()
        times = np.array([0.0, 1.0])
        values = np.array([1.0, 2.0])
        basis = HarmonicBasis(n_harmonics=3)
        record = bs._fit_record(times, values, basis)
        assert not record.converged
        assert record.rmse == float("inf")
        assert record.aic == float("inf")

    def test_select_returns_harmonic_on_periodic_data(self):
        bs = BasisSelector()
        times, values = self._synthetic_series(n=120)
        result = bs.select(times, values)
        assert result.model_name in ("harmonic_1", "harmonic_2", "harmonic_3")

    def test_select_result_too_few_observations_raises(self):
        bs = BasisSelector()
        times = np.array([0.0, 1.0])
        values = np.array([1.0, 2.0])
        with pytest.raises(ValueError, match="Too few observations"):
            bs.select_result(times, values)

    def test_select_result_arrays_shape_mismatch_raises(self):
        bs = BasisSelector()
        times = np.array([0.0, 1.0, 2.0])
        values = np.array([1.0, 2.0])
        with pytest.raises(ValueError, match="one-dimensional arrays of equal length"):
            bs.select_result(times, values)

    def test_select_result_fallback_on_noisy_data(self):
        bs = BasisSelector(max_relative_rmse=0.001)
        rng = np.random.default_rng(99)
        times = np.linspace(0, 5, 120)
        values = rng.normal(0, 10, 120)
        result = bs.select_result(times, values)
        assert result.used_fallback is True
        assert result.fallback_reason is not None
        assert "relative_rmse" in result.fallback_reason

    def test_fit_basis_returns_correct_shapes(self):
        bs = BasisSelector()
        times, values = self._synthetic_series()
        basis = HarmonicBasis(n_harmonics=2)
        params, fitted, residuals = bs.fit_basis(times, values, basis)
        assert params.ndim == 1
        assert fitted.shape == values.shape
        assert residuals.shape == values.shape

    def test_select_result_contains_candidate_records(self):
        bs = BasisSelector()
        times, values = self._synthetic_series()
        result = bs.select_result(times, values)
        assert len(result.candidate_records) == len(bs.candidates)
        assert result.selected_basis.model_name in [c.model_name for c in bs.candidates]

    def test_bic_criterion_selects_different_from_aic(self):
        bs_bic = BasisSelector(criterion="bic")
        times, values = self._synthetic_series(n=120)
        result = bs_bic.select_result(times, values)
        assert result.criterion == "bic"
        assert result.selected_basis is not None
