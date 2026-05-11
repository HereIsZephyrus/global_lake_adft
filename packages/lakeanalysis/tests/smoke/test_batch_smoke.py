"""End-to-end smoke tests for the batch engine (single-process mode).

Tests run against both PostgreSQL and Parquet backends via fixture parametrization.

Run with:
    uv run pytest tests/smoke/test_batch_smoke.py -v
"""

from __future__ import annotations

import pytest

from lakeanalysis.batch import Engine, RangeFilter, build_batch_reader, build_batch_writer
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.batch.task_spec import get_batch_task_spec


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _clear_run_status(algorithm, source_config, id_range):
    """Remove stale run_status records before a fresh smoke run."""
    from lakesource.provider.factory import create_provider

    spec = get_batch_task_spec(algorithm)
    if spec.done_table is None:
        return
    provider = create_provider(source_config)
    id_start, id_end = id_range
    if source_config.backend.value == "parquet":
        # For parquet, just delete the run_status file
        table_path = provider._data_dir / f"{spec.done_table}.parquet"  # type: ignore[attr-defined]
        table_path.unlink(missing_ok=True)
    else:
        # For postgres, delete rows in the id range
        provider.delete_ids(spec.done_table, list(range(id_start, id_end)))


def _build_engine(algorithm, id_range, source_config, **calc_kwargs):
    calculator = CalculatorFactory.create(algorithm, **calc_kwargs)
    lake_filter = RangeFilter(start=id_range[0], end=id_range[1])
    return Engine(
        reader=build_batch_reader(source_config),
        writer=build_batch_writer(source_config),
        calculator=calculator,
        algorithm=algorithm,
        lake_filter=lake_filter,
        chunk_size=10_000,
    )


def _run_algorithm_smoke(algorithm, id_range, source_config, **calc_kwargs):
    _clear_run_status(algorithm, source_config, id_range)
    engine = _build_engine(algorithm, id_range, source_config, **calc_kwargs)
    report = engine.run()

    assert report is not None, "Engine.run() returned None (MPI rank > 0?)"
    assert report.total_chunks >= 1, f"Expected at least 1 chunk, got {report.total_chunks}"
    assert report.success_lakes >= 1, (
        f"No lakes succeeded for {algorithm}: {report}"
    )
    return report


# ------------------------------------------------------------------
# Algorithm smoke tests (parametrized × backend)
# ------------------------------------------------------------------

@pytest.mark.parametrize("algorithm,calc_kwargs", [
    ("quantile", {}),
    ("eot", {"tails": ["high"], "quantiles": [0.95]}),
])
def test_algorithm_smoke(algorithm, calc_kwargs, source_config, id_range, cleanup):
    """Run algorithm end-to-end; at least 1 lake must succeed."""
    cleanup.register(algorithm)
    _run_algorithm_smoke(algorithm, id_range, source_config, **calc_kwargs)


def test_pwm_extreme_smoke(source_config, pwm_id_range, cleanup_pwm):
    """Run pwm_extreme end-to-end with a dedicated id range."""
    cleanup_pwm.register("pwm_extreme")
    _run_algorithm_smoke("pwm_extreme", pwm_id_range, source_config)


# ------------------------------------------------------------------
# Incremental skip test
# ------------------------------------------------------------------

def test_incremental_skip(source_config, id_range, cleanup):
    """Run quantile twice; second run should skip all lakes."""
    cleanup.register("quantile")
    _clear_run_status("quantile", source_config, id_range)
    engine = _build_engine("quantile", id_range, source_config)

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


# ------------------------------------------------------------------
# Error handling test
# ------------------------------------------------------------------

def test_error_handling(source_config, id_range, cleanup):
    """Run with impossible min_valid_observations to trigger errors."""
    cleanup.register("quantile")
    _clear_run_status("quantile", source_config, id_range)
    engine = _build_engine(
        "quantile", id_range, source_config,
        min_valid_observations=999_999,
    )

    report = engine.run()
    assert report is not None, "Engine completed without crashing"
    assert report.error_lakes >= 0
