"""Tests for lakeviz layout composition."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from lakeviz.layout import create_figure, save
from lakeviz.style.base import AxKind, get_ax_kind


def test_create_figure_single():
    fig, axes = create_figure([{"name": "main", "row": 0, "col": 0}])
    assert "main" in axes
    assert get_ax_kind(axes["main"]) == AxKind.STATISTICAL
    plt.close(fig)


def test_create_figure_grid():
    fig, axes = create_figure([
        {"name": "a", "row": 0, "col": 0},
        {"name": "b", "row": 0, "col": 1},
        {"name": "c", "row": 1, "col": 0, "colspan": 2},
    ])
    assert set(axes.keys()) == {"a", "b", "c"}
    plt.close(fig)


def test_create_figure_with_kind():
    fig, axes = create_figure([
        {"name": "geo", "row": 0, "col": 0, "kind": AxKind.GEOGRAPHIC},
        {"name": "stat", "row": 0, "col": 1, "kind": AxKind.STATISTICAL},
    ])
    assert get_ax_kind(axes["geo"]) == AxKind.GEOGRAPHIC
    assert get_ax_kind(axes["stat"]) == AxKind.STATISTICAL
    plt.close(fig)


def test_create_figure_custom_figsize():
    fig, axes = create_figure(
        [{"name": "x", "row": 0, "col": 0}],
        figsize=(12, 6),
    )
    assert fig.get_size_inches()[0] == 12
    assert fig.get_size_inches()[1] == 6
    plt.close(fig)


def test_create_figure_width_ratios():
    fig, axes = create_figure(
        [{"name": "a", "row": 0, "col": 0}, {"name": "b", "row": 0, "col": 1}],
        width_ratios=[2, 1],
    )
    assert "a" in axes and "b" in axes
    plt.close(fig)


def test_save(tmp_path):
    fig, axes = create_figure([{"name": "x", "row": 0, "col": 0}])
    out = save(fig, tmp_path / "test.png", close=False)
    assert out.exists()
    plt.close(fig)
