"""Tests for lazy exports exposed by lakesource.postgres."""

from __future__ import annotations

import lakesource.postgres as postgres


def test_all_exported_symbols_resolve() -> None:
    for name in postgres.__all__:
        assert getattr(postgres, name) is not None


def test_exported_symbols_have_no_duplicates() -> None:
    assert len(postgres.__all__) == len(set(postgres.__all__))
