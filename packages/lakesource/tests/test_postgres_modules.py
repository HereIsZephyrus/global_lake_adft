"""Tests for lake_* module imports and symbols."""

from __future__ import annotations

import pytest


class TestLakeModulesImport:
    def test_lake_area_imports(self) -> None:
        from lakesource.postgres import lake_area
        assert hasattr(lake_area, "fetch_lake_area")
        assert hasattr(lake_area, "fetch_lake_area_chunk")
        assert hasattr(lake_area, "fetch_af_nearest_high_topo")
        assert hasattr(lake_area, "fetch_impact_pairs")
        assert hasattr(lake_area, "fetch_lake_area_by_ids")

    def test_lake_eot_imports(self) -> None:
        from lakesource.postgres import lake_eot
        assert hasattr(lake_eot, "fetch_eot_extremes_by_id")
        assert hasattr(lake_eot, "ensure_eot_results_table")
        assert hasattr(lake_eot, "upsert_eot_results")
        assert hasattr(lake_eot, "upsert_eot_extremes")
        assert hasattr(lake_eot, "upsert_eot_run_status")

    def test_lake_quantile_imports(self) -> None:
        from lakesource.postgres import lake_quantile
        assert hasattr(lake_quantile, "ensure_quantile_tables")
        assert hasattr(lake_quantile, "upsert_quantile_labels")
        assert hasattr(lake_quantile, "upsert_quantile_extremes")
        assert hasattr(lake_quantile, "upsert_quantile_abrupt_transitions")
        assert hasattr(lake_quantile, "upsert_quantile_run_status")
        assert hasattr(lake_quantile, "count_quantile_status_in_range")
        assert hasattr(lake_quantile, "fetch_quantile_status_ids_in_range")

    def test_lake_hawkes_imports(self) -> None:
        from lakesource.postgres import lake_hawkes
        assert hasattr(lake_hawkes, "ensure_hawkes_results_table")
        assert hasattr(lake_hawkes, "upsert_hawkes_results")
        assert hasattr(lake_hawkes, "upsert_hawkes_lrt")
        assert hasattr(lake_hawkes, "upsert_hawkes_transition_monthly")

    def test_lake_pwm_imports(self) -> None:
        from lakesource.postgres import lake_pwm
        assert hasattr(lake_pwm, "ensure_pwm_extreme_tables")
        assert hasattr(lake_pwm, "upsert_pwm_extreme_thresholds")
        assert hasattr(lake_pwm, "upsert_pwm_extreme_run_status")
        assert hasattr(lake_pwm, "count_pwm_extreme_status_in_range")
        assert hasattr(lake_pwm, "fetch_pwm_extreme_status_ids_in_range")

    def test_lake_entropy_imports(self) -> None:
        from lakesource.postgres import lake_entropy
        assert hasattr(lake_entropy, "ensure_entropy_table")
        assert hasattr(lake_entropy, "upsert_entropy")
        assert hasattr(lake_entropy, "ensure_area_entropy_cv_table")
        assert hasattr(lake_entropy, "upsert_area_entropy_cv")

    def test_lake_misc_imports(self) -> None:
        from lakesource.postgres import lake_misc
        assert hasattr(lake_misc, "fetch_frozen_year_months_by_ids")
        assert hasattr(lake_misc, "fetch_frozen_year_months_chunk")
        assert hasattr(lake_misc, "fetch_seasonal_amplitude_chunk")
        assert hasattr(lake_misc, "fetch_linear_trend_by_ids")
        assert hasattr(lake_misc, "fetch_anomaly_hylak_ids")
        assert hasattr(lake_misc, "fetch_quality_done_hylak_ids_in_range")
        assert hasattr(lake_misc, "fetch_max_lake_info_hylak_id")
        assert hasattr(lake_misc, "count_source_hylak_ids_in_range")
        assert hasattr(lake_misc, "fetch_source_hylak_ids_in_range")
        assert hasattr(lake_misc, "ensure_comparison_tables")
        assert hasattr(lake_misc, "upsert_comparison_run_status")
        assert hasattr(lake_misc, "fetch_comparison_status_ids_in_range")
        assert hasattr(lake_misc, "ensure_interpolation_detect_table")
        assert hasattr(lake_misc, "upsert_interpolation_detect")


class TestAreaQualityExports:
    def test_quality_run_status_exports(self) -> None:
        from lakesource.postgres.area_quality import (
            RUN_STATUS_DONE,
            RUN_STATUS_ERROR,
            make_quality_run_status_row,
            ensure_quality_run_status_table,
            upsert_quality_run_status,
        )
        assert RUN_STATUS_DONE == "done"
        assert RUN_STATUS_ERROR == "error"
        row = make_quality_run_status_row(
            hylak_id=1,
            status=RUN_STATUS_ERROR,
            chunk_start=0,
            chunk_end=100,
            error_message="test error",
        )
        assert row["hylak_id"] == 1
        assert row["status"] == "error"
        assert row["error_message"] == "test error"
