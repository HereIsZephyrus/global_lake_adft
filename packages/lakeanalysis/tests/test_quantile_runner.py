"""Tests for the run_quantile batch CLI entry point."""

# pylint: disable=too-few-public-methods,missing-function-docstring

import importlib.util
from pathlib import Path


RUNNER_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_quantile.py"
SPEC = importlib.util.spec_from_file_location("run_quantile", RUNNER_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def _default_args() -> MODULE.argparse.Namespace:
    """Return a namespace with defaults matching --full-scan mode."""
    return MODULE.argparse.Namespace(
        chunk_size=10_000,
        limit_id=None,
        id_start=0,
        id_end=None,
        min_valid_per_month=None,
        min_valid_observations=None,
        io_budget=4,
        method="stl",
    )


def _noop(*_args, **_kwargs) -> None:
    """No-op callable for monkeypatching."""


def _make_object(*_args, **_kwargs) -> object:
    """Trivial factory returning a sentinel object."""
    return object()


def test_main_builds_batch_engine_from_args(monkeypatch) -> None:
    """Verify main() wires SourceConfig, reader/writer/calculator, and Engine."""
    captured: dict[str, object] = {}

    class _FakeLogger:
        def __init__(self, name: str) -> None:
            captured["logger_name"] = name

    class _FakeSourceConfig:
        def __init__(self) -> None:
            captured["source_config_created"] = True

    class _FakeEngine:
        def __init__(self, **kwargs) -> None:
            captured["engine_kwargs"] = kwargs

        def run(self) -> None:
            captured["engine_run_called"] = True

    def fake_build_reader(source_config, **kwargs):
        captured["reader_source_config"] = source_config
        captured["reader_kwargs"] = kwargs
        return "reader"

    def fake_build_writer(source_config, **kwargs):
        captured["writer_source_config"] = source_config
        captured["writer_kwargs"] = kwargs
        return "writer"

    def fake_create(algorithm: str, **kwargs):
        captured["calculator_algorithm"] = algorithm
        captured["calculator_kwargs"] = kwargs
        return "calculator"

    monkeypatch.setattr(
        MODULE,
        "parse_args",
        lambda: MODULE.argparse.Namespace(
            chunk_size=25,
            limit_id=150,
            id_start=100,
            id_end=200,
            min_valid_per_month=3,
            min_valid_observations=36,
            io_budget=2,
            method="stl",
        ),
    )
    monkeypatch.setattr(MODULE, "Logger", _FakeLogger)
    monkeypatch.setattr(MODULE, "SourceConfig", _FakeSourceConfig)
    monkeypatch.setattr(MODULE, "Engine", _FakeEngine)
    monkeypatch.setattr(MODULE, "build_provider_batch_reader", fake_build_reader)
    monkeypatch.setattr(MODULE, "build_provider_batch_writer", fake_build_writer)
    monkeypatch.setattr(MODULE.CalculatorFactory, "create", fake_create)

    MODULE.main()

    assert captured["logger_name"] == "run_quantile"
    assert captured["source_config_created"] is True
    assert captured["reader_kwargs"] == {
        "done_table": "quantile_run_status",
        "done_requires_status": True,
    }
    assert captured["writer_kwargs"] == {"ensure_tables": ["quantile"]}
    assert captured["calculator_algorithm"] == "quantile"
    assert captured["calculator_kwargs"] == {
        "min_valid_per_month": 3,
        "min_valid_observations": 36,
        "method": "stl",
    }

    engine_kwargs = captured["engine_kwargs"]
    assert engine_kwargs["reader"] == "reader"
    assert engine_kwargs["writer"] == "writer"
    assert engine_kwargs["calculator"] == "calculator"
    assert engine_kwargs["algorithm"] == "quantile"
    assert engine_kwargs["chunk_size"] == 25
    assert engine_kwargs["io_budget"] == 2
    lake_filter = engine_kwargs["lake_filter"]
    assert isinstance(lake_filter, MODULE.RangeFilter)
    assert lake_filter.start == 100
    assert lake_filter.end == 150
    assert captured["engine_run_called"] is True


def test_main_skips_range_filter_when_full_scan(monkeypatch) -> None:
    """Verify main() sets lake_filter=None when no explicit ID range is given."""
    captured: dict[str, object] = {}

    class _FakeEngine:
        def __init__(self, **kwargs) -> None:
            captured["lake_filter"] = kwargs["lake_filter"]

        def run(self) -> None:
            pass

    monkeypatch.setattr(MODULE, "parse_args", _default_args)
    monkeypatch.setattr(MODULE, "Logger", _noop)
    monkeypatch.setattr(MODULE, "SourceConfig", _make_object)
    monkeypatch.setattr(MODULE, "Engine", _FakeEngine)
    monkeypatch.setattr(MODULE, "build_provider_batch_reader", _make_object)
    monkeypatch.setattr(MODULE, "build_provider_batch_writer", _make_object)
    monkeypatch.setattr(MODULE.CalculatorFactory, "create", _make_object)

    MODULE.main()

    assert captured["lake_filter"] is None
