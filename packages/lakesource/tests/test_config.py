"""Tests for lakesource config."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakesource.config import Backend, SourceConfig
from lakesource.env import ensure_env_loaded


def test_default_backend_is_postgres():
    config = SourceConfig(backend=Backend.POSTGRES)
    assert config.backend == Backend.POSTGRES


def test_default_backend_from_adapter_yaml():
    config = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/tmp/data"))
    assert config.backend == Backend.PARQUET


def test_parquet_backend_requires_data_dir(monkeypatch):
    monkeypatch.delenv("PARQUET_DATA_DIR", raising=False)
    with pytest.raises(ValueError, match="data_dir is required"):
        SourceConfig(backend=Backend.PARQUET)


def test_parquet_backend_with_data_dir():
    config = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/tmp/data"))
    assert config.data_dir == Path("/tmp/data")


def test_year_filters_default_none():
    config = SourceConfig(backend=Backend.POSTGRES)
    assert config.year_start is None
    assert config.year_end is None


def test_year_filters_set():
    config = SourceConfig(backend=Backend.POSTGRES, year_start=2010, year_end=2020)
    assert config.year_start == 2010
    assert config.year_end == 2020


def test_ensure_env_loaded_is_idempotent(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("DB_HOST=example.test\n", encoding="utf-8")

    ensure_env_loaded(dotenv_path=env_path)
    ensure_env_loaded(dotenv_path=env_path)


class TestFiguresDir:
    """Tests for SourceConfig.figures_dir resolution."""

    def test_default_when_data_dir_is_set(self):
        config = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
        assert config.figures_dir == Path("/data/figure")

    def test_default_when_data_dir_is_none(self):
        config = SourceConfig(backend=Backend.POSTGRES)
        expected = Path.cwd() / "figure"
        assert config.figures_dir == expected

    def test_explicit_figures_dir(self):
        config = SourceConfig(
            backend=Backend.PARQUET,
            data_dir=Path("/data/parquet"),
            figures_dir=Path("/custom/figures"),
        )
        assert config.figures_dir == Path("/custom/figures")

    def test_env_var_overrides_default(self, monkeypatch):
        monkeypatch.setenv("FIGURES_DIR", "/env/figures")
        config = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
        assert config.figures_dir == Path("/env/figures")

    def test_env_var_overrides_data_dir_fallback(self, monkeypatch):
        monkeypatch.setenv("FIGURES_DIR", "/env/figures")
        config = SourceConfig(backend=Backend.POSTGRES)
        assert config.figures_dir == Path("/env/figures")
