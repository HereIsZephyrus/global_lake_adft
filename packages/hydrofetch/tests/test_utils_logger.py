"""Tests for hydrofetch.utils.logger."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from hydrofetch.utils.logger import setup_logging


@pytest.fixture(autouse=True)
def _reset_root_logging():
    root = logging.getLogger()
    before = list(root.handlers)
    yield
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(logging.WARNING)
    for handler in before:
        root.addHandler(handler)


def test_setup_logging_writes_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HYDROFETCH_LOG_DIR", str(tmp_path))
    monkeypatch.delenv("HYDROFETCH_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    log_path = setup_logging(verbose=False)
    assert log_path is not None
    assert log_path.parent == tmp_path
    assert log_path.name.startswith("hydrofetch_")
    assert log_path.suffix == ".log"

    logging.getLogger("test_logger").info("hello file")
    text = log_path.read_text(encoding="utf-8")
    assert "hello file" in text


def test_setup_logging_console_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HYDROFETCH_LOG_DIR", "none")

    assert setup_logging(verbose=False) is None


def test_verbose_forces_debug(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HYDROFETCH_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("HYDROFETCH_LOG_DIR", "none")

    setup_logging(verbose=True)
    assert logging.getLogger().level == logging.DEBUG
