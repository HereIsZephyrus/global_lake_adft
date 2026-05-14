"""Hawkes calculator architecture tests.

Verifies:
- PWM-Hawkes uses run_single_lake_service (STL path) not legacy raw-water_area
- PWM-Hawkes severity is in percentile units (index_value-based)
- EOT-Hawkes defaults to RunsDeclustering, can be configured for NoDeclustering
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.batch.lake_dataset import LakeDataset
from lakesource.pwm.schema import PWMExtremeConfig


def _make_synthetic_series(
    hylak_id: int = 1,
    years: int = 6,
    base_area: float = 100.0,
    seasonal_amplitude: float = 30.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Build a synthetic monthly area series with seasonal + trend.

    Produces a DataFrame compatible with ``LakeTask.series_df``.
    """
    rng = np.random.default_rng(seed)
    n = years * 12
    records = []
    for i in range(n):
        year = 2000 + i // 12
        month = i % 12 + 1
        trend = (i / n) * 20.0
        seasonal = seasonal_amplitude * np.sin(2 * np.pi * (month - 1) / 12.0)
        noise = rng.normal(0, 5.0)
        water_area = base_area + trend + seasonal + noise
        water_area = max(water_area, 1.0)
        records.append({
            "hylak_id": hylak_id,
            "year": year,
            "month": month,
            "water_area": water_area,
            "year_month": year * 100 + month,
        })
    return pd.DataFrame(records)


def _make_single_dataset(
    series_df: pd.DataFrame,
    hylak_id: int = 1,
    frozen_year_months: frozenset[int] | None = None,
) -> LakeDataset:
    """Build a size-1 LakeDataset from a single-lake DataFrame."""
    ym = series_df["year"].astype(int) * 100 + series_df["month"].astype(int)
    values = series_df["water_area"].to_numpy(dtype=float).reshape(1, -1)
    frozen_mask = None
    if frozen_year_months:
        frozen_mask = np.isin(ym.to_numpy(), list(frozen_year_months)).reshape(1, -1)
    return LakeDataset(
        hylak_ids=np.array([hylak_id], dtype=np.int64),
        year_months=ym.to_numpy(dtype=np.int64),
        values=values,
        frozen_mask=frozen_mask,
    )


class TestPWMHawkesUsesSTLPath:
    """PWM-Hawkes calculator MUST use run_single_lake_service (STL)."""

    def test_pwm_hawkes_produces_index_value_based_severity(self) -> None:
        """After running PWM-Hawkes, severity should be in percentile units.

        The key test: severity is computed as |index_value - threshold|,
        both in percentile [0, 100] space, NOT raw km².
        """
        from lakeanalysis.pwm.service import run_single_lake_service
        from lakesource.pwm.schema import (
            PWMExtremeServiceConfig,
            PWMExtremeConfig,
        )
        from lakeanalysis.pwm.events import run_runs_declustering

        series_df = _make_synthetic_series(seed=42)
        service_config = PWMExtremeServiceConfig(
            pwm_config=PWMExtremeConfig(n_pwm=3, min_observations_per_month=5),
            method="stl",
        )
        pwm_result = run_single_lake_service(
            series_df, hylak_id=1, config=service_config, frozen_year_months=None,
        )

        extremes = pwm_result.extremes_df
        assert not extremes.empty, "Expected some extreme events"

        assert "index_value" in extremes.columns, \
            "extremes_df must contain index_value column"
        assert "severity" in extremes.columns
        assert "water_area" in extremes.columns

        # Severity in percentile space should be small (|percentile - threshold|)
        severity_vals = extremes["severity"].dropna().to_numpy(dtype=float)
        assert severity_vals.size > 0
        # percentile-based severity: 0 < severity < 100 typically
        assert (severity_vals >= 0).all(), f"severity must be non-negative, got min {severity_vals.min()}"
        assert severity_vals.max() <= 100.0, \
            f"severity in percentile space should be <= 100, got {severity_vals.max()}"

        # severity should be <= |index_value - threshold| (matches definition)
        for _, row in extremes.iterrows():
            expected = abs(row["index_value"] - row["threshold"])
            assert abs(row["severity"] - expected) < 1e-9, \
                f"severity={row['severity']} should equal |index_value - threshold|={expected}"

    def test_pwm_hawkes_calculator_invokes_service(self) -> None:
        """PWMHawkesCalculator.run_dataset() uses decay index + segment filtering.

        Synthetic 6-year sinusoidal data typically does not produce transition
        segments (both high and low within one decay window), so the calculator
        should return a result (pass or fail).  We verify the table keys work.
        """
        from lakeanalysis.batch.calculator.pwm_hawkes import PWMHawkesCalculator

        series_df = _make_synthetic_series(seed=43)
        ds = _make_single_dataset(series_df, hylak_id=1)

        calculator = PWMHawkesCalculator(
            pwm_config=PWMExtremeConfig(n_pwm=3, min_observations_per_month=5),
            min_event_rate=0.005,
            max_event_rate=0.50,
            min_median_severity=0.1,
            method="stl",
        )

        rows_by_table, _, _ = calculator.run_dataset(ds)

        assert "pwm_hawkes_results" in rows_by_table
        assert "pwm_hawkes_lrt" in rows_by_table
        assert "pwm_hawkes_transition_monthly" in rows_by_table
        assert "pwm_hawkes_run_status" in rows_by_table
        assert "pwm_hawkes_segments" in rows_by_table

    def test_pwm_hawkes_accepts_phi_method_configuration(self) -> None:
        from lakeanalysis.batch.calculator.pwm_hawkes import PWMHawkesCalculator

        series_df = _make_synthetic_series(seed=44)
        ds = _make_single_dataset(series_df, hylak_id=1)

        calculator = PWMHawkesCalculator(
            pwm_config=PWMExtremeConfig(n_pwm=3, min_observations_per_month=5),
            min_event_rate=0.005,
            max_event_rate=0.50,
            min_median_severity=0.1,
            method="stl",
            phi_method="log1p",
        )

        rows_by_table, _, _ = calculator.run_dataset(ds)
        assert "pwm_hawkes_results" in rows_by_table

    def test_pwm_hawkes_route_b_runs_with_amplitude_evt(self) -> None:
        from lakeanalysis.batch.calculator.pwm_hawkes import PWMHawkesCalculator

        series_df = _make_synthetic_series(seed=45)
        ds = _make_single_dataset(series_df, hylak_id=1)

        calculator = PWMHawkesCalculator(
            pwm_config=PWMExtremeConfig(n_pwm=3, min_observations_per_month=5),
            min_event_rate=0.005,
            max_event_rate=0.50,
            min_median_severity=0.1,
            method="stl",
            evt_route="B",
        )

        rows_by_table, _, _ = calculator.run_dataset(ds)
        assert "pwm_hawkes_results" in rows_by_table


class TestEOTHawkesDefaults:
    """EOT-Hawkes MUST default to RunsDeclustering."""

    def test_eot_hawkes_defaults_runs_declustering(self) -> None:
        from lakeanalysis.batch.calculator.eot_hawkes import EOTHawkesCalculator

        calc = EOTHawkesCalculator()
        assert calc._decluster_run_length == 1, \
            "EOT-Hawkes must default to decluster_run_length=1 (RunsDeclustering)"

    def test_eot_hawkes_no_declustering_configurable(self) -> None:
        from lakeanalysis.batch.calculator.eot_hawkes import EOTHawkesCalculator

        calc = EOTHawkesCalculator(decluster_run_length=None)
        assert calc._decluster_run_length is None, \
            "null decluster_run_length should mean NoDeclustering"

    def test_eot_hawkes_produces_split_table_keys(self) -> None:
        """EOT-Hawkes calculator uses eot_hawkes_* table keys, not legacy hawkes_*."""
        from lakeanalysis.batch.calculator.eot_hawkes import (
            EOTHawkesCalculator,
        )
        from lakeanalysis.hawkes.pipeline import (
            HawkesCoreResult,
            build_error_summary,
            build_hawkes_result_row,
        )

        calc = EOTHawkesCalculator()

        rows_by_table = calc.error_to_rows(42, ValueError("test"), 0, 100)
        assert "eot_hawkes_results" in rows_by_table
        assert "eot_hawkes_run_status" in rows_by_table

        summary = {"hylak_id": 42, "threshold_quantile": 0.90, "converged": True,
                    "log_likelihood": -100.0, "n_events": 20, "n_dry_events": 10,
                    "n_wet_events": 10, "qc_pass": True, "error_message": None}
        lrt_rows = [{"hylak_id": 42, "threshold_quantile": 0.90, "test_name": "D_to_W",
                      "lr_statistic": 1.0, "df": 1, "p_value": 0.5,
                      "significance_level": 0.05, "reject_null": False,
                      "restricted_log_likelihood": -110.0, "full_log_likelihood": -100.0}]

        success_result = calc._make_result(
            HawkesCoreResult(
                summary=summary,
                lrt_rows=lrt_rows,
                transition_monthly_rows=[],
            )
        )
        rows_by_table = calc.result_to_rows(success_result)
        assert "eot_hawkes_results" in rows_by_table
        assert "eot_hawkes_lrt" in rows_by_table
        assert "eot_hawkes_transition_monthly" in rows_by_table
        assert "eot_hawkes_run_status" in rows_by_table

    def test_eot_hawkes_emits_return_level_rows(self) -> None:
        from lakeanalysis.batch.calculator.eot_hawkes import EOTHawkesCalculator

        series_df = _make_synthetic_series(seed=46)
        ds = _make_single_dataset(series_df, hylak_id=1)
        calc = EOTHawkesCalculator(
            threshold_quantile=0.90,
            min_event_rate=0.005,
            max_event_rate=0.50,
            min_median_severity=0.1,
        )

        rows_by_table, _, _ = calc.run_dataset(ds)
        assert "eot_return_levels" in rows_by_table
