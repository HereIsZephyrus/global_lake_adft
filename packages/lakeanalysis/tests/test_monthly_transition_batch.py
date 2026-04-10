from pathlib import Path

import pandas as pd

import lakeanalysis.monthly_transition.batch as batch_module
from lakeanalysis.monthly_transition import (
    MonthlyTransitionBatchConfig,
    MonthlyTransitionResult,
    MonthlyTransitionServiceConfig,
    process_chunk_lakes,
    run_monthly_transition_batch,
)


def build_lake_map() -> dict[int, pd.DataFrame]:
    base_rows = []
    for year, offset in ((2000, -10.0), (2001, 0.0), (2002, 10.0)):
        for month in range(1, 13):
            base_rows.append(
                {
                    "year": year,
                    "month": month,
                    "water_area": 100.0 + month + offset,
                }
            )
    return {
        101: pd.DataFrame(base_rows),
        202: pd.DataFrame(base_rows),
    }


def test_batch_config_exposes_service_config() -> None:
    config = MonthlyTransitionBatchConfig(
        output_root=Path("tmp"),
        min_valid_per_month=3,
        min_valid_observations=36,
    )
    assert config.service_config == MonthlyTransitionServiceConfig(
        min_valid_per_month=3,
        min_valid_observations=36,
    )


def test_process_chunk_lakes_isolates_errors_and_skips_processed_ids() -> None:
    lake_map = build_lake_map()

    def fake_run_single(series_df, *, hylak_id, config, frozen_year_months, use_frozen_mask):
        assert frozen_year_months is None
        assert use_frozen_mask is False
        if hylak_id == 202:
            raise ValueError("boom")
        labels_df = pd.DataFrame(
            [
                {
                    "hylak_id": hylak_id,
                    "year": 2000,
                    "month": 1,
                    "water_area": 1.0,
                    "monthly_climatology": 1.0,
                    "anomaly": 0.0,
                    "q_low": -1.0,
                    "q_high": 1.0,
                    "extreme_label": "normal",
                }
            ]
        )
        return MonthlyTransitionResult(
            hylak_id=hylak_id,
            climatology_df=pd.DataFrame(),
            labels_df=labels_df,
            extremes_df=pd.DataFrame(),
            transitions_df=pd.DataFrame(),
            q_low=-1.0,
            q_high=1.0,
        )

    payload = process_chunk_lakes(
        lake_map,
        chunk_start=0,
        chunk_end=1000,
        service_config=MonthlyTransitionServiceConfig(
            min_valid_per_month=3,
            min_valid_observations=36,
        ),
        processed_hylak_ids={101},
        run_single_fn=fake_run_single,
    )

    assert payload.skipped_lakes == 1
    assert payload.success_lakes == 0
    assert payload.error_lakes == 1
    assert len(payload.status_rows) == 1
    assert payload.status_rows[0]["hylak_id"] == 202
    assert payload.status_rows[0]["status"] == "error"


def test_run_batch_rebuilds_summary_cache_from_db_aggregates(monkeypatch, tmp_path: Path) -> None:
    class DummyContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummySeriesDB:
        @staticmethod
        def connection_context():
            return DummyContext()

    summary_payload = {
        "transition_counts": pd.DataFrame(
            [{"transition_type": "low_to_high", "count": 2}]
        ),
        "transition_seasonality": pd.DataFrame(
            [{"to_month": 7, "count": 2}]
        ),
        "lake_transition_counts": pd.DataFrame(
            [{"hylak_id": 101, "transition_count": 2}]
        ),
        "lake_extreme_counts": pd.DataFrame(
            [{"hylak_id": 101, "extreme_count": 4}]
        ),
        "run_metadata": {"labels_rows": 24, "extremes_rows": 4, "transitions_rows": 2},
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(batch_module, "series_db", DummySeriesDB())
    monkeypatch.setattr(batch_module, "ensure_monthly_transition_tables", lambda conn: None)
    monkeypatch.setattr(batch_module, "fetch_max_hylak_id", lambda conn: 999)
    monkeypatch.setattr(batch_module, "count_source_lakes_in_chunk", lambda conn, start, end: 2)
    monkeypatch.setattr(batch_module, "count_processed_lakes_in_chunk", lambda conn, start, end: 2)
    monkeypatch.setattr(batch_module, "fetch_summary_cache_sources", lambda conn: summary_payload)

    def fail_fetch_lake_area_chunk(conn, chunk_start, chunk_end):
        raise AssertionError("chunk data should not be fetched when the chunk is already processed")

    def fake_write_summary_cache(cache_root, **kwargs):
        captured["cache_root"] = cache_root
        captured["payload"] = kwargs
        return {"run_metadata": cache_root / "run_metadata.json"}

    monkeypatch.setattr(batch_module, "fetch_lake_area_chunk", fail_fetch_lake_area_chunk)
    monkeypatch.setattr(batch_module, "write_summary_cache", fake_write_summary_cache)

    report = run_monthly_transition_batch(
        MonthlyTransitionBatchConfig(
            output_root=tmp_path,
            chunk_size=1000,
            plot_summary=False,
        )
    )

    assert report.total_chunks == 1
    assert report.processed_chunks == 0
    assert report.skipped_chunks == 1
    assert report.source_lakes == 2
    assert captured["cache_root"] == tmp_path / "summary_cache"
    assert captured["payload"] == summary_payload
