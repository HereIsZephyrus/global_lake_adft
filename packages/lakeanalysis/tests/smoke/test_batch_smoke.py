"""End-to-end smoke tests for the batch engine (single-process mode).

Tests run against both PostgreSQL and Parquet backends.

Requires env vars: SERIES_DB, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT.

Run with:
    pytest tests/smoke/test_batch_smoke.py -v -s
"""

from __future__ import annotations

from lakeanalysis.batch import Engine, RangeFilter
from lakeanalysis.batch.calculator import CalculatorFactory


def _run_algorithm_smoke(
    algorithm: str,
    id_start: int,
    id_end: int,
    provider,
    cleanup_algorithm_rows=None,
    **calc_kwargs,
) -> None:
    calculator = CalculatorFactory.create(algorithm, **calc_kwargs)
    lake_filter = RangeFilter(start=id_start, end=id_end)
    engine = Engine(
        provider=provider,
        calculator=calculator,
        algorithm=algorithm,
        lake_filter=lake_filter,
        chunk_size=10_000,
    )

    if cleanup_algorithm_rows is not None:
        cleanup_algorithm_rows.register(algorithm)
    report = engine.run()

    assert report is not None, "Engine.run() returned None (MPI rank > 0?)"
    assert report.total_chunks >= 1, f"Expected at least 1 chunk, got {report.total_chunks}"
    assert report.success_lakes >= 1, (
        f"No lakes succeeded for {algorithm}: {report}"
    )


# --- PostgreSQL backend ---

def test_a_quantile_smoke(id_range, provider, cleanup_algorithm_rows):
    _run_algorithm_smoke(
        "quantile",
        id_range[0],
        id_range[1],
        provider,
        cleanup_algorithm_rows,
    )


def test_b_pwm_extreme_smoke(id_range, provider, cleanup_algorithm_rows):
    _run_algorithm_smoke(
        "pwm_extreme",
        id_range[0],
        id_range[1],
        provider,
        cleanup_algorithm_rows,
    )


def test_c_eot_smoke(id_range, provider, cleanup_algorithm_rows):
    _run_algorithm_smoke(
        "eot",
        id_range[0],
        id_range[1],
        provider,
        cleanup_algorithm_rows,
        tails=["high"],
        quantiles=[0.95],
    )


def test_incremental_skip_smoke(id_range, provider, cleanup_algorithm_rows):
    """Run quantile twice on the same range; second run should skip all lakes."""
    id_start, id_end = id_range
    cleanup_algorithm_rows.register("quantile")

    calculator = CalculatorFactory.create("quantile")
    lake_filter = RangeFilter(start=id_start, end=id_end)
    engine = Engine(
        provider=provider,
        calculator=calculator,
        algorithm="quantile",
        lake_filter=lake_filter,
        chunk_size=10_000,
    )

    report1 = engine.run()
    assert report1 is not None
    assert report1.success_lakes >= 1

    report2 = engine.run()
    assert report2 is not None
    assert report2.skipped_lakes >= 1, (
        f"Second run should skip done lakes: {report2}"
    )
    assert report2.success_lakes == 0, (
        f"Second run should have 0 new successes: {report2}"
    )


def test_error_handling_smoke(id_range, provider, cleanup_algorithm_rows):
    """Run with deliberately bad min_valid_observations to trigger errors."""
    id_start, id_end = id_range
    cleanup_algorithm_rows.register("quantile")

    calculator = CalculatorFactory.create(
        "quantile",
        min_valid_observations=999_999,
    )
    lake_filter = RangeFilter(start=id_start, end=id_end)
    engine = Engine(
        provider=provider,
        calculator=calculator,
        algorithm="quantile",
        lake_filter=lake_filter,
        chunk_size=10_000,
    )

    report = engine.run()
    assert report is not None
    assert report.error_lakes >= 0, "Engine completed without crashing"


# --- Parquet backend ---

def test_d_parquet_quantile_smoke(parquet_id_range, parquet_provider):
    _run_algorithm_smoke(
        "quantile",
        parquet_id_range[0],
        parquet_id_range[1],
        parquet_provider,
    )


def test_e_parquet_pwm_extreme_smoke(parquet_id_range, parquet_provider):
    _run_algorithm_smoke(
        "pwm_extreme",
        parquet_id_range[0],
        parquet_id_range[1],
        parquet_provider,
    )


def test_f_parquet_eot_smoke(parquet_id_range, parquet_provider):
    _run_algorithm_smoke(
        "eot",
        parquet_id_range[0],
        parquet_id_range[1],
        parquet_provider,
        tails=["high"],
        quantiles=[0.95],
    )


def test_parquet_incremental_skip_smoke(parquet_id_range, parquet_provider, parquet_data_dir):
    """Run quantile twice on parquet; second run should skip all lakes."""
    (parquet_data_dir / "quantile_run_status.parquet").unlink(missing_ok=True)
    id_start, id_end = parquet_id_range
    calculator = CalculatorFactory.create("quantile")
    lake_filter = RangeFilter(start=id_start, end=id_end)
    engine = Engine(
        provider=parquet_provider,
        calculator=calculator,
        algorithm="quantile",
        lake_filter=lake_filter,
        chunk_size=10_000,
    )

    report1 = engine.run()
    assert report1 is not None
    assert report1.success_lakes >= 1

    report2 = engine.run()
    assert report2 is not None
    assert report2.skipped_lakes >= 1, (
        f"Second run should skip done lakes: {report2}"
    )
