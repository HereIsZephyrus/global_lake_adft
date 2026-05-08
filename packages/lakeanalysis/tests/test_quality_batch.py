"""Tests for QualityCalculator."""

from __future__ import annotations

import pytest
from lakeanalysis.quality.batch import QualityCalculator, QualityRunConfig


@pytest.fixture
def calculator() -> QualityCalculator:
    return QualityCalculator(QualityRunConfig())


class TestQualityCalculatorErrorToRows:
    def test_error_to_rows_returns_quality_run_status(self, calculator: QualityCalculator) -> None:
        result = calculator.error_to_rows(
            hylak_id=123,
            error=ValueError("test error"),
            chunk_start=0,
            chunk_end=1000,
        )
        assert "quality_run_status" in result
        rows = result["quality_run_status"]
        assert len(rows) == 1
        assert rows[0]["hylak_id"] == 123
        assert rows[0]["status"] == "error"
        assert rows[0]["chunk_start"] == 0
        assert rows[0]["chunk_end"] == 1000
        assert "test error" in rows[0]["error_message"]

    def test_error_to_rows_does_not_raise(self, calculator: QualityCalculator) -> None:
        try:
            calculator.error_to_rows(
                hylak_id=1,
                error=RuntimeError("boom"),
                chunk_start=0,
                chunk_end=100,
            )
        except Exception as exc:
            pytest.fail(f"error_to_rows raised {exc} - should return error dict instead")


class TestQualityCalculatorResultToRows:
    def test_result_to_rows_returns_quality_run_status(self, calculator: QualityCalculator) -> None:
        result = {
            "row": {
                "hylak_id": 456,
                "rs_area_mean": 1.5,
                "rs_area_median": 1.4,
                "atlas_area": 2.0,
                "anomaly_flags": 0,
            },
            "is_anomalous": False,
        }
        rows = calculator.result_to_rows(result)
        assert "area_quality" in rows
        assert "quality_run_status" in rows
        assert len(rows["area_quality"]) == 1
        assert len(rows["quality_run_status"]) == 1
        assert rows["quality_run_status"][0]["hylak_id"] == 456

    def test_result_to_rows_anomalous_returns_area_anomalies(
        self, calculator: QualityCalculator
    ) -> None:
        result = {
            "row": {
                "hylak_id": 789,
                "rs_area_mean": 1.5,
                "rs_area_median": 1.4,
                "atlas_area": 2.0,
                "anomaly_flags": 1,
            },
            "is_anomalous": True,
        }
        rows = calculator.result_to_rows(result)
        assert "area_anomalies" in rows
        assert "quality_run_status" in rows


class TestQualityBatchWriterSupportsQualityRunStatus:
    def test_quality_batch_writer_persists_quality_run_status(self) -> None:
        from unittest.mock import MagicMock
        from lakeanalysis.quality.batch import QualityBatchWriter

        mock_provider = MagicMock()
        writer = QualityBatchWriter(mock_provider)
        writer.ensure_schema("quality")
        mock_provider.ensure_table.assert_any_call("quality_run_status")

        writer.persist({"quality_run_status": [{"hylak_id": 1, "status": "error"}]})
        mock_provider.upsert_rows.assert_called()
