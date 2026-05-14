"""Tests for lakeviz.quality.plot: plot_anomaly_upset."""

from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import pandas as pd
import pytest

from lakeviz.artificial import plot_delta_cv_distribution
from lakeviz.quality.plot import plot_anomaly_upset
from lakeviz.quality import plot_area_scatter
from lakeviz.quantile import plot_anomaly_timeline


def _make_flags_df(n_per_combo: int = 5) -> pd.DataFrame:
    rows = []
    combos = [
        {"is_median_zero": False, "is_flat_or_pv": False, "is_area_mismatch": False, "is_shift": False},
        {"is_median_zero": True,  "is_flat_or_pv": False, "is_area_mismatch": False, "is_shift": False},
        {"is_median_zero": False, "is_flat_or_pv": True,  "is_area_mismatch": False, "is_shift": False},
        {"is_median_zero": False, "is_flat_or_pv": False, "is_area_mismatch": True,  "is_shift": False},
        {"is_median_zero": True,  "is_flat_or_pv": True,  "is_area_mismatch": False, "is_shift": False},
        {"is_median_zero": False, "is_flat_or_pv": True,  "is_area_mismatch": True,  "is_shift": False},
        {"is_median_zero": True,  "is_flat_or_pv": True,  "is_area_mismatch": True,  "is_shift": False},
        {"is_median_zero": False, "is_flat_or_pv": False, "is_area_mismatch": False, "is_shift": True},
        {"is_median_zero": True,  "is_flat_or_pv": False, "is_area_mismatch": False, "is_shift": True},
    ]
    hid = 1
    for combo in combos:
        for _ in range(n_per_combo):
            row = {"hylak_id": hid, **combo}
            rows.append(row)
            hid += 1
    return pd.DataFrame(rows)


def test_plot_anomaly_upset_returns_figure():
    df = _make_flags_df()
    fig = plot_anomaly_upset(df)
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_plot_anomaly_upset_missing_column():
    df = pd.DataFrame({"hylak_id": [1], "is_median_zero": [True]})
    with pytest.raises(ValueError, match="missing required column"):
        plot_anomaly_upset(df)


def test_plot_anomaly_upset_min_size():
    df = _make_flags_df(n_per_combo=2)
    fig = plot_anomaly_upset(df, min_size=3)
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_plot_anomaly_upset_no_counts():
    df = _make_flags_df()
    fig = plot_anomaly_upset(df, show_counts=False)
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_plot_anomaly_upset_custom_title():
    df = _make_flags_df()
    fig = plot_anomaly_upset(df, title="Custom Title")
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_plot_anomaly_timeline_empty_labels_df():
    fig = plot_anomaly_timeline(pd.DataFrame(columns=["year", "month", "index_value", "threshold_low", "threshold_high", "extreme_label"]))
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_plot_area_scatter_smoke():
    df = pd.DataFrame(
        {
            "atlas_area": [1.0, 2.0, 4.0],
            "rs_area_median": [1.1, 1.8, 4.2],
            "agreement_median": ["good", "moderate", "poor"],
        }
    )
    fig = plot_area_scatter(df)
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_plot_delta_cv_distribution_smoke():
    df = pd.DataFrame({"delta_cv": [0.1, -0.2, 0.4]})
    fig = plot_delta_cv_distribution(df)
    assert fig is not None
    import matplotlib.pyplot as plt
    plt.close(fig)
