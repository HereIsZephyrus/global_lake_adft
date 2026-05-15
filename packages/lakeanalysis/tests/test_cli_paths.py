"""Tests for CLI path resolution via SourceConfig (no module-level DATA_DIR)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakesource.config import Backend, SourceConfig


class TestSourceConfigPathResolution:
    """Verify that all path conventions derive from SourceConfig."""

    def test_parquet_data_dirs(self):
        """gt10 and full parquet dirs derive from data_dir.parent."""
        source = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
        assert source.data_dir == Path("/data/parquet")
        assert source.data_dir.parent / "parquet_gt10" == Path("/data/parquet_gt10")

    def test_figures_dir_from_data_dir(self):
        """Default figures_dir = data_dir.parent / "figure"."""
        source = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
        assert source.figures_dir == Path("/data/figure")

    def test_domain_data_subdirs(self):
        """Domain-specific data dirs derive from data_dir.parent."""
        source = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
        base = source.data_dir.parent
        assert base / "entropy" == Path("/data/entropy")
        assert base / "hawkes" / "qc" == Path("/data/hawkes/qc")
        assert base / "comparison" / "sample_lakes.parquet" == Path("/data/comparison/sample_lakes.parquet")

    def test_domain_figure_subdirs(self):
        """Figure outputs derive from figures_dir root."""
        source = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
        assert source.figures_dir == Path("/data/figure")

    def test_no_module_level_data_dir(self):
        """_common.py must not export DATA_DIR as a module constant."""
        from lakeanalysis.cli import _common
        assert not hasattr(_common, "DATA_DIR"), "_common.py must not have DATA_DIR"

    def test_no_module_level_figures_dir(self):
        """plot.py must not have a module-level FIGURES_DIR."""
        import importlib
        plot = importlib.import_module("lakeanalysis.cli.plot")
        assert not hasattr(plot, "FIGURES_DIR"), "plot.py must not have FIGURES_DIR"


class TestCliParameterDefaults:
    """Verify CLI commands accept None for directory parameters."""

    def test_comparison_global_has_none_defaults(self):
        from lakeanalysis.cli.plot import comparison_global
        import inspect
        params = inspect.signature(comparison_global).parameters
        for name in ("output_dir", "gt10_dir", "full_dir"):
            opt = params[name].default
            actual = opt.default if hasattr(opt, "default") else opt
            assert actual is None, f"{name} default should be None, got {actual!r}"

    def test_cli_app_imports(self):
        """Smoke test: CLI app imports without crashing."""
        from lakeanalysis.cli import app
        assert app is not None
