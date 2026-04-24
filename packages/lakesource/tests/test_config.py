"""Tests for lakesource config."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakesource.config import Backend, SourceConfig


def test_default_backend_is_postgres():
    config = SourceConfig()
    assert config.backend == Backend.POSTGRES


def test_parquet_backend_requires_data_dir():
    with pytest.raises(ValueError, match="data_dir is required"):
        SourceConfig(backend=Backend.PARQUET)


def test_parquet_backend_with_data_dir():
    config = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/tmp/data"))
    assert config.data_dir == Path("/tmp/data")


def test_workflow_version_must_not_be_empty():
    with pytest.raises(ValueError, match="workflow_version must not be empty"):
        SourceConfig(workflow_version="  ")


def test_workflow_version_stripped():
    config = SourceConfig(workflow_version="  v1  ")
    assert config.workflow_version == "v1"


def test_year_filters_default_none():
    config = SourceConfig()
    assert config.year_start is None
    assert config.year_end is None


def test_year_filters_set():
    config = SourceConfig(year_start=2010, year_end=2020)
    assert config.year_start == 2010
    assert config.year_end == 2020
