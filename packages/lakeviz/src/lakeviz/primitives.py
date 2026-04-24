"""Low-level reusable plotting primitives.

These functions accept only plain Python types and pandas DataFrames,
keeping lakeviz free of lakeanalysis type dependencies.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_line(
    x: np.ndarray | pd.Series,
    y: np.ndarray | pd.Series,
    *,
    ax: plt.Axes | None = None,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    label: str | None = None,
    color: str | None = None,
    linewidth: float = 1.0,
    linestyle: str = "-",
    marker: str | None = None,
    markersize: float = 3,
    figsize: tuple[float, float] = (8, 5),
) -> plt.Figure:
    """Plot a single line series."""
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()
    ax.plot(x, y, label=label, color=color, linewidth=linewidth, linestyle=linestyle, marker=marker, markersize=markersize)
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if label:
        ax.legend()
    fig.tight_layout()
    return fig


def plot_scatter(
    x: np.ndarray | pd.Series,
    y: np.ndarray | pd.Series,
    *,
    ax: plt.Axes | None = None,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    label: str | None = None,
    color: str | None = None,
    s: float = 16,
    alpha: float = 0.7,
    figsize: tuple[float, float] = (6, 6),
) -> plt.Figure:
    """Plot a scatter series."""
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()
    ax.scatter(x, y, label=label, color=color, s=s, alpha=alpha)
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if label:
        ax.legend()
    fig.tight_layout()
    return fig


def plot_scatter_with_diagonal(
    x: np.ndarray | pd.Series,
    y: np.ndarray | pd.Series,
    *,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    label: str | None = None,
    s: float = 16,
    alpha: float = 0.7,
    figsize: tuple[float, float] = (6, 6),
) -> plt.Figure:
    """Scatter plot with a diagonal reference line."""
    fig, ax = plt.subplots(figsize=figsize)
    ax.scatter(x, y, label=label, s=s, alpha=alpha)
    lo = min(float(x.min()), float(y.min()))
    hi = max(float(x.max()), float(y.max()))
    ax.plot([lo, hi], [lo, hi], linestyle="--", color="grey")
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    fig.tight_layout()
    return fig


def plot_bar(
    labels: Sequence[str],
    values: np.ndarray | pd.Series | list[float],
    *,
    ax: plt.Axes | None = None,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    colors: list[str] | None = None,
    figsize: tuple[float, float] = (8, 5),
    x_rotation: float = 0,
) -> plt.Figure:
    """Plot a bar chart."""
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.get_figure()
    ax.bar(labels, values, color=colors)
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if x_rotation:
        ax.tick_params(axis="x", rotation=x_rotation)
    fig.tight_layout()
    return fig


def plot_histogram(
    values: np.ndarray | pd.Series,
    *,
    bins: int = 40,
    density: bool = False,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    color: str | None = None,
    alpha: float = 0.6,
    edgecolor: str = "white",
    linewidth: float = 0.4,
    figsize: tuple[float, float] = (8, 5),
    vline: float | None = None,
    vline_label: str | None = None,
) -> plt.Figure:
    """Plot a histogram with optional vertical reference line."""
    fig, ax = plt.subplots(figsize=figsize)
    ax.hist(values, bins=bins, density=density, alpha=alpha, color=color, edgecolor=edgecolor, linewidth=linewidth)
    if vline is not None:
        ax.axvline(vline, color="red", linestyle="--", label=vline_label)
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if vline_label:
        ax.legend()
    fig.tight_layout()
    return fig


def plot_fill_between(
    x: np.ndarray | pd.Series,
    y_lower: np.ndarray | pd.Series,
    y_upper: np.ndarray | pd.Series,
    *,
    ax: plt.Axes,
    alpha: float = 0.2,
) -> None:
    """Add a shaded confidence band to an existing axes."""
    ax.fill_between(x, y_lower, y_upper, alpha=alpha)


def plot_axhline(
    y: float,
    *,
    ax: plt.Axes,
    color: str = "tomato",
    linestyle: str = "--",
    linewidth: float = 1.0,
    label: str | None = None,
) -> None:
    """Add a horizontal reference line to an existing axes."""
    ax.axhline(y, color=color, linestyle=linestyle, linewidth=linewidth, label=label)


def plot_axvline(
    x: float,
    *,
    ax: plt.Axes,
    color: str = "tab:green",
    linestyle: str = ":",
    linewidth: float = 0.9,
    alpha: float = 0.8,
) -> None:
    """Add a vertical reference line to an existing axes."""
    ax.axvline(x, color=color, linestyle=linestyle, linewidth=linewidth, alpha=alpha)


def annotate_point(
    text: str,
    xy: tuple[float, float],
    *,
    ax: plt.Axes,
    xytext: tuple[float, float] = (4, 6),
    fontsize: int = 8,
    color: str = "tomato",
) -> None:
    """Annotate a single point on an axes."""
    ax.annotate(text, xy, xytext=xytext, textcoords="offset points", fontsize=fontsize, color=color)