"""Visual regression tests — generate test images to validate the draw architecture.

Each test creates a figure with synthetic data and saves it to
``data/figures/test_viz/``.  Run with ``pytest tests/test_viz_images.py -v``.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import cartopy.crs as ccrs

from lakeviz.style.base import AxKind, AxisStyle, apply_axis_style, get_ax_kind
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import Theme

from lakeviz.draw.line import draw_line
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.bar import draw_bar
from lakeviz.draw.histogram import draw_histogram
from lakeviz.draw.fill import draw_fill_between
from lakeviz.draw.reference import draw_axhline, draw_axvline, draw_diagonal
from lakeviz.draw.annotate import draw_annotate_point, draw_text_box

from lakeviz.domain.eot import (
    draw_mrl, draw_parameter_stability, draw_pp, draw_qq,
    draw_return_levels, draw_eot_extremes, draw_extremes_with_hawkes,
)
from lakeviz.domain.hawkes import (
    draw_event_timeline, draw_intensity_decomposition,
    draw_kernel_matrix, draw_lrt_summary,
)
from lakeviz.domain.quantile import (
    draw_monthly_timeline, draw_anomaly_timeline,
    draw_transition_count_summary, draw_transition_seasonality_summary,
    draw_adft_fallback,
)
from lakeviz.domain.basemodel import (
    draw_candidate_scores, draw_basis_fit, draw_residuals,
)
from lakeviz.domain.entropy import (
    draw_ae_distribution, draw_amplitude_histogram,
    draw_annual_ae, draw_trend_summary, draw_amplitude_vs_entropy,
)
from lakeviz.domain.similarity import (
    draw_pearson_distribution, draw_acf_cosine_distribution, draw_pearson_vs_acf,
)
from lakeviz.domain.pwm_extreme import (
    draw_quantile_function, draw_threshold_summary,
)
from lakeviz.layout import create_figure, save
from lakeviz.map_plot import draw_global_grid, plot_global_grid
from lakeviz.grid import agg_to_grid_matrix

OUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "figure_test"

rng = np.random.default_rng(42)


@pytest.fixture(autouse=True, scope="session")
def _setup_theme():
    Theme.apply()


def _save(fig, name):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    save(fig, OUT_DIR / name, close=False)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------

def _mrl_df(n=30):
    t = np.linspace(0.5, 5, n)
    return pd.DataFrame({"threshold": t, "mean_excess": 2.0 + 0.3 * t + rng.normal(0, 0.2, n)})


def _stability_df(n=30):
    t = np.linspace(0.5, 5, n)
    return pd.DataFrame({
        "threshold": t,
        "shape_xi": 0.1 + rng.normal(0, 0.05, n),
        "modified_scale": 1.0 + 0.2 * t + rng.normal(0, 0.1, n),
    })


def _pp_df(n=50):
    e = np.sort(rng.uniform(0, 1, n))
    m = np.clip(e + rng.normal(0, 0.05, n), 0, 1)
    return pd.DataFrame({"empirical_probability": e, "model_probability": m})


def _qq_df(n=50):
    t = np.sort(rng.exponential(1, n))
    return pd.DataFrame({"theoretical_quantile": t, "empirical_quantile": t + rng.normal(0, 0.3, n)})


def _rl_df():
    p = np.array([2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000])
    l = 10 + 5 * np.log(p) + rng.normal(0, 0.5, len(p))
    return pd.DataFrame({"return_period_years": p, "return_level": l, "ci_lower": l - 1.5, "ci_upper": l + 1.5})


def _series_df(n=120):
    rows = []
    for i in range(n):
        y, m = 2000 + i // 12, (i % 12) + 1
        rows.append({"year": y, "month": m, "time": y + m / 12,
                      "original_value": 100 + 10 * np.sin(m / 12 * 2 * np.pi) + rng.normal(0, 5),
                      "water_area": 100 + 10 * np.sin(m / 12 * 2 * np.pi) + rng.normal(0, 5)})
    return pd.DataFrame(rows)


def _extremes_df(sdf, n=15):
    idx = rng.choice(len(sdf), n, replace=False)
    e = sdf.iloc[idx].copy()
    e["threshold_at_event"] = e["original_value"] - rng.uniform(2, 8, n)
    e["tail"] = rng.choice(["high", "low"], n)
    return e


def _hawkes_events(n=20):
    return pd.DataFrame({"event_label": rng.choice(["D", "W"], n), "time": np.sort(rng.uniform(0, 10, n))})


def _decomp_df(n=200):
    t = np.linspace(0, 10, n)
    return pd.DataFrame({
        "time": t, "lambda_D": 0.5 + 0.3 * np.sin(t), "lambda_W": 0.4 + 0.2 * np.cos(t),
        "mu_D": 0.5 * np.ones(n), "mu_W": 0.4 * np.ones(n),
        "self_D": 0.3 * np.sin(t), "self_W": 0.2 * np.cos(t),
        "cross_D": 0.1 * np.sin(t + 1), "cross_W": 0.1 * np.cos(t + 1),
    })


def _lrt_df():
    return pd.DataFrame({"test_name": ["H0 vs H1", "H1 vs H2", "H2 vs H3"],
                          "lr_statistic": [15.3, 8.7, 2.1], "p_value": [0.001, 0.03, 0.15],
                          "significance_level": [0.05, 0.05, 0.05]})


def _labels_df(n=120):
    rows = []
    for i in range(n):
        y, m = 2000 + i // 12, (i % 12) + 1
        rows.append({"year": y, "month": m,
                      "water_area": 100 + 10 * np.sin(m / 12 * 2 * np.pi) + rng.normal(0, 5),
                      "monthly_climatology": 100 + 10 * np.sin(m / 12 * 2 * np.pi),
                      "anomaly": rng.normal(0, 5), "q_low": -15.0, "q_high": 15.0,
                      "extreme_label": rng.choice(["normal", "extreme_high", "extreme_low"], p=[0.7, 0.15, 0.15])})
    return pd.DataFrame(rows)


def _transitions_df(n=30):
    return pd.DataFrame({"transition_type": rng.choice(["low_to_high", "high_to_low"], n),
                          "to_year": rng.integers(2000, 2010, n), "to_month": rng.integers(1, 13, n)})


def _scores_df():
    return pd.DataFrame({"basis_name": ["harmonic2", "harmonic4", "poly3", "spline"],
                          "aic": [120, 95, 110, 88], "bic": [130, 105, 115, 98], "rmse": [5.2, 3.1, 4.5, 2.8]})


def _fit_df(n=120):
    t = np.linspace(2000, 2010, n)
    v = 100 + 10 * np.sin(t * 2 * np.pi) + rng.normal(0, 3, n)
    f = 100 + 10 * np.sin(t * 2 * np.pi)
    return pd.DataFrame({"time": t, "value": v, "fitted": f, "residual": v - f})


def _entropy_summary(n=500):
    return pd.DataFrame({"ae_overall": rng.uniform(0.5, 1.0, n), "mean_seasonal_amplitude": rng.normal(0.3, 0.1, n),
                          "sens_slope": rng.normal(0.001, 0.005, n), "change_per_decade_pct": rng.normal(5, 15, n)})


def _annual_ae_df(n=20):
    ae = rng.uniform(0.6, 0.95, n)
    return pd.DataFrame({"year": np.arange(2000, 2000 + n), "AE": ae, "AE_anomaly": ae - ae.mean()})


def _similarity_summary(n=1000):
    r = rng.uniform(-0.5, 0.95, n)
    return pd.DataFrame({"pearson_r": r, "acf_cos_sim": np.clip(r + rng.normal(0, 0.1, n), -1, 1)})


def _curve_df(n=50):
    u = np.linspace(0.01, 0.99, n)
    return pd.DataFrame({"u": u, "prior_y": -np.log(1 - u), "fitted_x": -np.log(1 - u) * (1 + 0.1 * u)})


def _thresholds_df():
    m = np.arange(1, 13)
    ma = 100 + 10 * np.sin(m / 12 * 2 * np.pi)
    return pd.DataFrame({"month": m, "mean_area": ma, "threshold_high": ma + 15, "threshold_low": ma - 15})


def _adft_df(n=10):
    return pd.DataFrame({"year": rng.integers(2000, 2009, n), "month": rng.integers(1, 13, n),
                          "is_drought_to_flood": rng.choice([True, False], n)})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_primitives():
    fig, axes = create_figure([
        {"name": "line", "row": 0, "col": 0}, {"name": "scatter", "row": 0, "col": 1},
        {"name": "bar", "row": 0, "col": 2}, {"name": "hist", "row": 1, "col": 0},
        {"name": "fill", "row": 1, "col": 1}, {"name": "ref", "row": 1, "col": 2},
    ], figsize=(18, 10))
    x = np.linspace(0, 10, 50)
    draw_line(axes["line"], x, np.sin(x), style=LineStyle(color="steelblue", marker="o", label="sin"))
    draw_line(axes["line"], x, np.cos(x), style=LineStyle(color="tomato", linestyle="--", label="cos"))
    apply_axis_style(axes["line"], AxisStyle(title="draw_line", xlabel="x", ylabel="y"))
    axes["line"].legend()
    draw_scatter(axes["scatter"], rng.uniform(0, 10, 50), rng.uniform(0, 5, 50), style=ScatterStyle(color="purple", s=30, alpha=0.6, label="pts"))
    apply_axis_style(axes["scatter"], AxisStyle(title="draw_scatter"))
    draw_bar(axes["bar"], ["A", "B", "C", "D"], [3, 7, 5, 2], colors=["#E74C3C", "#27AE60", "#8B008B", "#D2691E"])
    apply_axis_style(axes["bar"], AxisStyle(title="draw_bar"))
    draw_histogram(axes["hist"], rng.normal(0, 1, 500), style=HistogramStyle(color="darkcyan", alpha=0.8))
    apply_axis_style(axes["hist"], AxisStyle(title="draw_histogram"))
    draw_fill_between(axes["fill"], x, np.sin(x) - 0.3, np.sin(x) + 0.3)
    draw_line(axes["fill"], x, np.sin(x), style=LineStyle(color="steelblue", label="center"))
    apply_axis_style(axes["fill"], AxisStyle(title="draw_fill_between"))
    draw_axhline(axes["ref"], 0.5, style=ReferenceLineStyle(color="tomato", label="hline"))
    draw_axvline(axes["ref"], 5, style=ReferenceLineStyle(color="tab:green", label="vline"))
    draw_annotate_point(axes["ref"], "note", (5, 0.5), color="red")
    apply_axis_style(axes["ref"], AxisStyle(title="draw_reference + annotate"))
    fig.tight_layout()
    _save(fig, "primitives.png")


def test_eot_diagnostics():
    fig, axes = create_figure([
        {"name": "mrl", "row": 0, "col": 0}, {"name": "xi", "row": 0, "col": 1}, {"name": "sigma", "row": 0, "col": 2},
        {"name": "pp", "row": 1, "col": 0}, {"name": "qq", "row": 1, "col": 1}, {"name": "rl", "row": 1, "col": 2},
    ], figsize=(18, 10))
    draw_mrl(axes["mrl"], _mrl_df())
    draw_parameter_stability(axes["xi"], axes["sigma"], _stability_df())
    draw_pp(axes["pp"], _pp_df())
    draw_qq(axes["qq"], _qq_df())
    draw_return_levels(axes["rl"], _rl_df())
    fig.tight_layout()
    _save(fig, "eot_diagnostics.png")


def test_eot_extremes():
    sdf = _series_df()
    edf = _extremes_df(sdf)
    fig, ax = plt.subplots(figsize=(13, 4.8))
    draw_eot_extremes(ax, 12345, sdf, edf)
    fig.tight_layout()
    _save(fig, "eot_extremes.png")


def test_eot_hawkes_combined():
    sdf = _series_df()
    edf = _extremes_df(sdf)
    hdf = pd.DataFrame({"year": rng.integers(2000, 2009, 8), "month": rng.integers(1, 13, 8),
                          "direction": rng.choice(["D_to_W", "W_to_D"], 8)})
    fig, ax = plt.subplots(figsize=(14, 6))
    draw_extremes_with_hawkes(ax, 12345, sdf, edf, hdf)
    fig.tight_layout()
    _save(fig, "eot_hawkes_combined.png")


def test_hawkes_diagnostics():
    fig, axes = create_figure([
        {"name": "timeline", "row": 0, "col": 0, "colspan": 2},
        {"name": "d", "row": 1, "col": 0}, {"name": "w", "row": 1, "col": 1},
        {"name": "alpha", "row": 2, "col": 0}, {"name": "beta", "row": 2, "col": 1},
        {"name": "lr", "row": 3, "col": 0}, {"name": "p", "row": 3, "col": 1},
    ], figsize=(14, 16))
    draw_event_timeline(axes["timeline"], _hawkes_events())
    draw_intensity_decomposition(axes["d"], axes["w"], _decomp_df())
    draw_kernel_matrix(axes["alpha"], axes["beta"], np.array([[0.3, 0.1], [0.2, 0.4]]), np.array([[0.5, 0.3], [0.4, 0.6]]))
    draw_lrt_summary(axes["lr"], axes["p"], _lrt_df())
    fig.tight_layout()
    _save(fig, "hawkes_diagnostics.png")


def test_quantile_diagnostics():
    ldf = _labels_df()
    tdf = _transitions_df()
    fig, axes = create_figure([
        {"name": "monthly", "row": 0, "col": 0}, {"name": "anomaly", "row": 0, "col": 1},
        {"name": "count", "row": 1, "col": 0}, {"name": "season", "row": 1, "col": 1},
    ], figsize=(16, 10))
    draw_monthly_timeline(axes["monthly"], ldf, tdf, hylak_id=12345)
    draw_anomaly_timeline(axes["anomaly"], ldf, hylak_id=12345)
    draw_transition_count_summary(axes["count"], tdf)
    draw_transition_seasonality_summary(axes["season"], tdf)
    fig.tight_layout()
    _save(fig, "quantile_diagnostics.png")


def test_adft_fallback():
    fig, ax = plt.subplots(figsize=(14, 7))
    draw_adft_fallback(ax, 12345, _series_df(), _adft_df())
    fig.tight_layout()
    _save(fig, "adft_fallback.png")


def test_basemodel_diagnostics():
    fig, axes = create_figure([
        {"name": "score", "row": 0, "col": 0}, {"name": "rmse", "row": 0, "col": 1},
        {"name": "fit", "row": 1, "col": 0, "colspan": 2},
        {"name": "rts", "row": 2, "col": 0}, {"name": "rhist", "row": 2, "col": 1},
    ], figsize=(14, 14))
    draw_candidate_scores(axes["score"], axes["rmse"], _scores_df(), "aic", "spline")
    draw_basis_fit(axes["fit"], _fit_df(), "spline", "aic", 0.028)
    draw_residuals(axes["rts"], axes["rhist"], _fit_df())
    fig.tight_layout()
    _save(fig, "basemodel_diagnostics.png")


def test_entropy_diagnostics():
    s = _entropy_summary()
    a = _annual_ae_df()
    trend = {"sens_slope": 0.002, "mk_trend": "increasing", "mk_p": 0.03, "mk_significant": True}
    fig, axes = create_figure([
        {"name": "ae", "row": 0, "col": 0}, {"name": "amp", "row": 0, "col": 1},
        {"name": "line", "row": 1, "col": 0}, {"name": "bar", "row": 1, "col": 1},
        {"name": "slope", "row": 2, "col": 0}, {"name": "change", "row": 2, "col": 1},
        {"name": "vs", "row": 3, "col": 0, "colspan": 2},
    ], figsize=(14, 18))
    draw_ae_distribution(axes["ae"], s)
    draw_amplitude_histogram(axes["amp"], s)
    draw_annual_ae(axes["line"], axes["bar"], 12345, a, trend)
    draw_trend_summary(axes["slope"], axes["change"], s)
    draw_amplitude_vs_entropy(axes["vs"], s)
    fig.tight_layout()
    _save(fig, "entropy_diagnostics.png")


def test_similarity_diagnostics():
    s = _similarity_summary()
    fig, axes = create_figure([
        {"name": "pearson", "row": 0, "col": 0}, {"name": "acf", "row": 0, "col": 1},
        {"name": "vs", "row": 1, "col": 0, "colspan": 2},
    ], figsize=(14, 10))
    draw_pearson_distribution(axes["pearson"], s)
    draw_acf_cosine_distribution(axes["acf"], s)
    draw_pearson_vs_acf(axes["vs"], s)
    fig.tight_layout()
    _save(fig, "similarity_diagnostics.png")


def test_pwm_extreme_diagnostics():
    fig, axes = create_figure([
        {"name": "qf", "row": 0, "col": 0}, {"name": "th", "row": 0, "col": 1},
    ], figsize=(14, 5))
    draw_quantile_function(axes["qf"], _curve_df(), 6)
    draw_threshold_summary(axes["th"], _thresholds_df(), 12345)
    fig.tight_layout()
    _save(fig, "pwm_extreme_diagnostics.png")


def test_cross_domain_panel():
    fig, axes = create_figure([
        {"name": "mrl", "row": 0, "col": 0}, {"name": "pp", "row": 0, "col": 1},
        {"name": "timeline", "row": 1, "col": 0}, {"name": "ae", "row": 1, "col": 1},
    ], figsize=(14, 10))
    draw_mrl(axes["mrl"], _mrl_df())
    draw_pp(axes["pp"], _pp_df())
    draw_event_timeline(axes["timeline"], _hawkes_events())
    draw_ae_distribution(axes["ae"], _entropy_summary(200))
    fig.suptitle("Cross-domain panel (EOT + Hawkes + Entropy)", fontsize=14, fontweight="bold")
    fig.tight_layout()
    _save(fig, "cross_domain_panel.png")


# ---------------------------------------------------------------------------
# Geographic distribution tests
# ---------------------------------------------------------------------------

def _geo_grid_data(resolution=2.0):
    n_lon = int(360 / resolution)
    n_lat = int(180 / resolution)
    lons = np.linspace(-180 + resolution / 2, 180 - resolution / 2, n_lon)
    lats = np.linspace(-90 + resolution / 2, 90 - resolution / 2, n_lat)
    values = np.zeros((n_lat, n_lon))
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            dist = np.sqrt((lat - 30) ** 2 + (lon - 100) ** 2)
            values[i, j] = np.exp(-dist ** 2 / 1000) * 100 + rng.uniform(0, 5)
    values[:, :n_lon // 4] = np.nan
    return lons, lats, values


def _geo_grid_data_multi(resolution=5.0):
    n_lon = int(360 / resolution)
    n_lat = int(180 / resolution)
    lons = np.linspace(-180 + resolution / 2, 180 - resolution / 2, n_lon)
    lats = np.linspace(-90 + resolution / 2, 90 - resolution / 2, n_lat)
    values = np.zeros((n_lat, n_lon))
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            d1 = np.sqrt((lat - 40) ** 2 + (lon - 120) ** 2)
            d2 = np.sqrt((lat + 20) ** 2 + (lon + 60) ** 2)
            values[i, j] = 50 * np.exp(-d1 ** 2 / 500) + 30 * np.exp(-d2 ** 2 / 800) + rng.uniform(0, 3)
    return lons, lats, values


def test_global_grid_basic():
    lons, lats, values = _geo_grid_data(resolution=2.0)
    fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={"projection": ccrs.Robinson()})
    draw_global_grid(ax, lons, lats, values, title="全球分布测试 (单热点)", cbar_label="事件密度")
    assert get_ax_kind(ax) == AxKind.GEOGRAPHIC
    _save(fig, "global_grid_basic.png")


def test_global_grid_log_scale():
    lons, lats, values = _geo_grid_data_multi(resolution=5.0)
    fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={"projection": ccrs.Robinson()})
    draw_global_grid(ax, lons, lats, values, title="全球分布测试 (对数色阶)", cmap="YlOrRd", log_scale=True, cbar_label="每湖事件数")
    assert get_ax_kind(ax) == AxKind.GEOGRAPHIC
    _save(fig, "global_grid_log.png")


def test_global_grid_linear_scale():
    lons, lats, values = _geo_grid_data(resolution=3.0)
    values = values / values.max()
    fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={"projection": ccrs.Robinson()})
    draw_global_grid(ax, lons, lats, values, title="全球分布测试 (线性色阶)", cmap="RdYlGn", log_scale=False, vmin=0, vmax=1, cbar_label="收敛率")
    _save(fig, "global_grid_linear.png")


def test_global_grid_different_projections():
    lons, lats, values = _geo_grid_data_multi(resolution=4.0)
    fig, axes = plt.subplots(2, 2, figsize=(20, 16), subplot_kw={"projection": ccrs.Robinson()})
    axes = axes.flatten()
    projections = ["Robinson", "Robinson", "Robinson", "Robinson"]
    cmaps = ["YlOrRd", "Blues", "Greens", "PuBu"]
    titles = ["YlOrRd 色阶", "Blues 色阶", "Greens 色阶", "PuBu 色阶"]
    for ax, cmap, title in zip(axes, cmaps, titles):
        draw_global_grid(ax, lons, lats, values, title=title, cmap=cmap, cbar_label="密度")
        assert get_ax_kind(ax) == AxKind.GEOGRAPHIC
    fig.suptitle("不同色阶的全球分布图", fontsize=16, fontweight="bold")
    fig.tight_layout()
    _save(fig, "global_grid_cmaps.png")


def test_cross_domain_statistical_and_geographic():
    lons, lats, values = _geo_grid_data_multi(resolution=5.0)
    fig = plt.figure(figsize=(20, 12))
    import matplotlib.gridspec as gridspec
    gs = gridspec.GridSpec(2, 3, height_ratios=[1, 1.2])
    ax_mrl = fig.add_subplot(gs[0, 0])
    ax_pp = fig.add_subplot(gs[0, 1])
    ax_timeline = fig.add_subplot(gs[0, 2])
    ax_geo = fig.add_subplot(gs[1, :], projection=ccrs.Robinson())
    draw_mrl(ax_mrl, _mrl_df())
    assert get_ax_kind(ax_mrl) == AxKind.STATISTICAL
    draw_pp(ax_pp, _pp_df())
    assert get_ax_kind(ax_pp) == AxKind.STATISTICAL
    draw_event_timeline(ax_timeline, _hawkes_events())
    assert get_ax_kind(ax_timeline) == AxKind.STATISTICAL
    draw_global_grid(ax_geo, lons, lats, values, title="全球事件密度分布", cmap="YlOrRd", cbar_label="事件密度")
    assert get_ax_kind(ax_geo) == AxKind.GEOGRAPHIC
    fig.suptitle("跨域组合面板：统计图 + 地理分布图", fontsize=16, fontweight="bold")
    fig.tight_layout()
    _save(fig, "cross_domain_stat_geo.png")


def test_global_grid_via_agg_to_grid_matrix():
    agg_df = pd.DataFrame({
        "cell_lat": rng.uniform(-60, 60, 200),
        "cell_lon": rng.uniform(-180, 180, 200),
        "event_count": rng.integers(1, 50, 200),
        "lake_count": rng.integers(1, 10, 200),
    })
    agg_df["mean_per_lake"] = agg_df["event_count"] / agg_df["lake_count"]
    lons, lats, values = agg_to_grid_matrix(agg_df, "mean_per_lake", resolution=5.0)
    fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={"projection": ccrs.Robinson()})
    draw_global_grid(ax, lons, lats, values, title="从 agg_to_grid_matrix 生成的全球分布", cmap="YlOrRd", cbar_label="每湖事件数")
    assert get_ax_kind(ax) == AxKind.GEOGRAPHIC
    _save(fig, "global_grid_from_agg.png")


def test_multi_panel_geographic():
    lons1, lats1, vals1 = _geo_grid_data(resolution=4.0)
    lons2, lats2, vals2 = _geo_grid_data_multi(resolution=4.0)
    fig, axes = plt.subplots(1, 2, figsize=(24, 8), subplot_kw={"projection": ccrs.Robinson()})
    draw_global_grid(axes[0], lons1, lats1, vals1, title="分布 A：单热点模式", cmap="YlOrRd", cbar_label="密度 A")
    draw_global_grid(axes[1], lons2, lats2, vals2, title="分布 B：双热点模式", cmap="Blues", cbar_label="密度 B")
    for ax in axes:
        assert get_ax_kind(ax) == AxKind.GEOGRAPHIC
    fig.suptitle("多面板地理分布对比", fontsize=16, fontweight="bold")
    fig.tight_layout()
    _save(fig, "multi_panel_geographic.png")