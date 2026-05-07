"""Tests for lakeviz draw primitives."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from lakeviz.domain.entropy import draw_ae_distribution, draw_trend_summary
from lakeviz.draw.line import draw_line
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.bar import draw_bar
from lakeviz.draw.histogram import draw_histogram
from lakeviz.draw.reference import draw_axhline, draw_axvline, draw_diagonal
from lakeviz.draw.annotate import draw_annotate_point, draw_text_box
from lakeviz.style.base import AxKind, get_ax_kind
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.reference import ReferenceLineStyle


def test_draw_line_stamps_ax_kind():
    fig, ax = plt.subplots()
    draw_line(ax, [1, 2, 3], [4, 5, 6])
    assert get_ax_kind(ax) == AxKind.STATISTICAL
    plt.close(fig)


def test_draw_line_with_style():
    fig, ax = plt.subplots()
    style = LineStyle(color="red", marker="o", label="test")
    draw_line(ax, [1, 2, 3], [4, 5, 6], style=style)
    assert len(ax.get_lines()) >= 1
    plt.close(fig)


def test_draw_scatter_stamps_ax_kind():
    fig, ax = plt.subplots()
    draw_scatter(ax, [1, 2, 3], [4, 5, 6])
    assert get_ax_kind(ax) == AxKind.STATISTICAL
    plt.close(fig)


def test_draw_bar_stamps_ax_kind():
    fig, ax = plt.subplots()
    draw_bar(ax, ["a", "b", "c"], [1, 2, 3])
    assert get_ax_kind(ax) == AxKind.STATISTICAL
    plt.close(fig)


def test_draw_bar_with_colors():
    fig, ax = plt.subplots()
    draw_bar(ax, ["a", "b"], [1, 2], colors=["red", "blue"])
    assert get_ax_kind(ax) == AxKind.STATISTICAL
    plt.close(fig)


def test_draw_histogram_stamps_ax_kind():
    fig, ax = plt.subplots()
    draw_histogram(ax, np.random.randn(100))
    assert get_ax_kind(ax) == AxKind.STATISTICAL
    plt.close(fig)


def test_draw_axhline():
    fig, ax = plt.subplots()
    draw_axhline(ax, 0.5, style=ReferenceLineStyle(color="red", label="ref"))
    lines = ax.get_lines()
    assert len(lines) >= 1
    plt.close(fig)


def test_draw_axvline():
    fig, ax = plt.subplots()
    draw_axvline(ax, 0.5)
    xdata = ax.get_lines()[0].get_xdata()
    assert xdata[0] == xdata[1] == 0.5
    plt.close(fig)


def test_draw_diagonal():
    fig, ax = plt.subplots()
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    draw_diagonal(ax)
    plt.close(fig)


def test_draw_annotate_point():
    fig, ax = plt.subplots()
    draw_annotate_point(ax, "test", (0.5, 0.5))
    assert len(ax.texts) >= 1
    plt.close(fig)


def test_draw_text_box():
    fig, ax = plt.subplots()
    draw_text_box(ax, "info text")
    assert len(ax.texts) >= 1
    plt.close(fig)


def test_entropy_distribution_uses_vertical_mean_line():
    fig, ax = plt.subplots()
    draw_ae_distribution(ax, pd.DataFrame({"ae_overall": [0.2, 0.3, 0.5, 0.7]}))
    mean_line = ax.get_lines()[-1]
    xdata = mean_line.get_xdata()
    assert xdata[0] == xdata[1]
    plt.close(fig)


def test_entropy_trend_summary_uses_vertical_zero_lines():
    fig, (ax_slope, ax_change) = plt.subplots(1, 2)
    draw_trend_summary(
        ax_slope,
        ax_change,
        pd.DataFrame({"sens_slope": [-0.1, 0.2, 0.3], "change_per_decade_pct": [-1.0, 0.0, 2.0]}),
    )
    slope_ref = ax_slope.get_lines()[-1].get_xdata()
    change_ref = ax_change.get_lines()[-1].get_xdata()
    assert slope_ref[0] == slope_ref[1] == 0
    assert change_ref[0] == change_ref[1] == 0
    plt.close(fig)
