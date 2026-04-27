"""Tests for lakeviz style system."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from lakeviz.style.base import DrawStyle, AxisStyle, PanelStyle, AxKind, apply_axis_style, stamp_ax, get_ax_kind
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.presets import Theme


def test_draw_style_frozen():
    s = DrawStyle(color="red", linewidth=2.0)
    with pytest.raises(AttributeError):
        s.color = "blue"


def test_line_style_inherits_draw_style():
    s = LineStyle(color="tomato", marker="o")
    assert s.color == "tomato"
    assert s.marker == "o"
    assert s.linewidth == 1.1  # NCL-style default from DrawStyle


def test_scatter_style_inherits_draw_style():
    s = ScatterStyle(s=42, marker="^")
    assert s.s == 42
    assert s.marker == "^"


def test_bar_style_inherits_draw_style():
    s = BarStyle(width=0.5)
    assert s.width == 0.5


def test_ax_kind_enum():
    assert AxKind.STATISTICAL.value == "statistical"
    assert AxKind.GEOGRAPHIC.value == "geographic"


def test_stamp_ax_default():
    fig, ax = plt.subplots()
    assert get_ax_kind(ax) == AxKind.STATISTICAL
    plt.close(fig)


def test_stamp_ax_geographic():
    fig, ax = plt.subplots()
    stamp_ax(ax, AxKind.GEOGRAPHIC)
    assert get_ax_kind(ax) == AxKind.GEOGRAPHIC
    plt.close(fig)


def test_apply_axis_style():
    fig, ax = plt.subplots()
    style = AxisStyle(xlabel="X", ylabel="Y", title="T", xlim=(0, 10), ylim=(-1, 1))
    apply_axis_style(ax, style)
    assert ax.get_xlabel() == "X"
    assert ax.get_ylabel() == "Y"
    assert ax.get_title() == "T"
    assert ax.get_xlim() == (0, 10)
    assert ax.get_ylim() == (-1, 1)
    plt.close(fig)


def test_theme_apply():
    Theme.apply()
    assert plt.rcParams["font.family"] == ["Times New Roman", "SimSun", "DejaVu Sans"]
    assert plt.rcParams["axes.unicode_minus"] is False
