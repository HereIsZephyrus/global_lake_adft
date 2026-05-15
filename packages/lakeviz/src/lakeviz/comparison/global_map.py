"""Global density comparison panels for quantile, PWM, EOT, and dataset subsets."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakesource.config import Backend, SourceConfig
from lakesource.provider import create_provider

from ..config import GlobalGridConfig
from ..grid import agg_to_grid_matrix
from ..layout import create_figure, save
from ..map_plot import draw_global_density

log = logging.getLogger(__name__)

_LAND_MASK_CACHE: dict[float, np.ndarray] = {}


def _get_land_mask(lons: np.ndarray, lats: np.ndarray, resolution: float) -> np.ndarray:
    """Return a boolean 2D array (n_lat, n_lon): True where grid center is on land."""
    if resolution in _LAND_MASK_CACHE:
        return _LAND_MASK_CACHE[resolution]

    from cartopy.io import shapereader
    from shapely.ops import unary_union
    from shapely.vectorized import contains

    land_shp = shapereader.natural_earth(resolution="110m", category="physical", name="land")
    land_geom = unary_union(list(shapereader.Reader(land_shp).geometries()))
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    mask = contains(land_geom, lon_grid, lat_grid)
    _LAND_MASK_CACHE[resolution] = mask
    return mask


def _merge_two(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_map: dict[str, str],
    right_map: dict[str, str],
) -> pd.DataFrame:
    keep_left = ["cell_lat", "cell_lon", "lake_count", *left_map.keys()]
    keep_right = ["cell_lat", "cell_lon", "lake_count", *right_map.keys()]
    left_df = left[keep_left].rename(columns={"lake_count": "lake_count_left", **left_map})
    right_df = right[keep_right].rename(columns={"lake_count": "lake_count_right", **right_map})
    merged = left_df.merge(right_df, on=["cell_lat", "cell_lon"], how="outer")
    merged["lake_count_left"] = merged.get("lake_count_left", 0).fillna(0).astype(int)
    merged["lake_count_right"] = merged.get("lake_count_right", 0).fillna(0).astype(int)
    merged["lake_count"] = np.maximum(merged["lake_count_left"], merged["lake_count_right"]).astype(int)
    for col in merged.columns:
        if col.startswith("left_") or col.startswith("right_"):
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    return merged


def _fetch_quantile_per_lake_stats(provider, resolution, *, refresh=False) -> pd.DataFrame:
    return provider.fetch_grid_agg("quantile.per_lake_stats", resolution, refresh=refresh)


def _fetch_pwm_exceedance(provider, resolution, *, p=0.05, refresh=False) -> pd.DataFrame:
    return provider.fetch_grid_agg("pwm.exceedance", resolution, p_high=p, p_low=p, refresh=refresh)


def _fetch_eot_tail(provider, resolution, *, tail: str, q: float, refresh=False) -> pd.DataFrame:
    return provider.fetch_grid_agg(
        "eot.converged", resolution, tail=tail, threshold_quantile=q, refresh=refresh,
    )


def _fetch_eot_all(provider, resolution, *, q: float, refresh=False) -> pd.DataFrame:
    return provider.fetch_grid_agg(
        "eot.converged_all", resolution, threshold_quantile=q, refresh=refresh,
    )


def _standardize_quantile_vs_pwm(
    provider, resolution, *, refresh=False,
) -> pd.DataFrame:
    quantile = _fetch_quantile_per_lake_stats(provider, resolution, refresh=refresh)
    pwm = _fetch_pwm_exceedance(provider, resolution, p=0.05, refresh=refresh)
    return _merge_two(
        quantile,
        pwm,
        left_map={
            "mean_high": "left_high_mean",
            "median_high": "left_high_median",
            "mean_low": "left_low_mean",
            "median_low": "left_low_median",
            "mean_all": "left_all_mean",
            "median_all": "left_all_median",
        },
        right_map={
            "mean_high_exceedance": "right_high_mean",
            "median_high_exceedance": "right_high_median",
            "mean_low_exceedance": "right_low_mean",
            "median_low_exceedance": "right_low_median",
            "mean_all_exceedance": "right_all_mean",
            "median_all_exceedance": "right_all_median",
        },
    )


def _standardize_pwm_pvalues(
    provider, resolution, *, p1=0.01, p2=0.05, refresh=False,
) -> pd.DataFrame:
    left = _fetch_pwm_exceedance(provider, resolution, p=p1, refresh=refresh)
    right = _fetch_pwm_exceedance(provider, resolution, p=p2, refresh=refresh)
    mapping = {
        "mean_high_exceedance": "high_mean",
        "median_high_exceedance": "high_median",
        "mean_low_exceedance": "low_mean",
        "median_low_exceedance": "low_median",
        "mean_all_exceedance": "all_mean",
        "median_all_exceedance": "all_median",
    }
    return _merge_two(
        left,
        right,
        left_map={k: f"left_{v}" for k, v in mapping.items()},
        right_map={k: f"right_{v}" for k, v in mapping.items()},
    )


def _standardize_eot_quantiles(
    provider, resolution, *, q1=0.95, q2=0.98, refresh=False,
) -> pd.DataFrame:
    high_left = _fetch_eot_tail(provider, resolution, tail="high", q=q1, refresh=refresh)
    high_right = _fetch_eot_tail(provider, resolution, tail="high", q=q2, refresh=refresh)
    low_left = _fetch_eot_tail(provider, resolution, tail="low", q=q1, refresh=refresh)
    low_right = _fetch_eot_tail(provider, resolution, tail="low", q=q2, refresh=refresh)
    all_left = _fetch_eot_all(provider, resolution, q=q1, refresh=refresh)
    all_right = _fetch_eot_all(provider, resolution, q=q2, refresh=refresh)

    merged = _merge_two(
        high_left,
        high_right,
        left_map={
            "mean_extremes_freq": "left_high_mean",
            "median_extremes_freq": "left_high_median",
        },
        right_map={
            "mean_extremes_freq": "right_high_mean",
            "median_extremes_freq": "right_high_median",
        },
    )
    merged = merged.merge(
        _merge_two(
            low_left,
            low_right,
            left_map={
                "mean_extremes_freq": "left_low_mean",
                "median_extremes_freq": "left_low_median",
            },
            right_map={
                "mean_extremes_freq": "right_low_mean",
                "median_extremes_freq": "right_low_median",
            },
        )[[
            "cell_lat", "cell_lon", "left_low_mean", "left_low_median",
            "right_low_mean", "right_low_median",
        ]],
        on=["cell_lat", "cell_lon"],
        how="outer",
    )
    merged = merged.merge(
        _merge_two(
            all_left,
            all_right,
            left_map={
                "mean_all_extremes_freq": "left_all_mean",
                "median_all_extremes_freq": "left_all_median",
            },
            right_map={
                "mean_all_extremes_freq": "right_all_mean",
                "median_all_extremes_freq": "right_all_median",
            },
        )[[
            "cell_lat", "cell_lon", "left_all_mean", "left_all_median",
            "right_all_mean", "right_all_median",
        ]],
        on=["cell_lat", "cell_lon"],
        how="outer",
    )
    for col in merged.columns:
        if col.startswith("left_") or col.startswith("right_"):
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    return merged


def _standardize_pwm_vs_eot(
    provider, resolution, *, pwm_p=0.05, eot_q=0.95, refresh=False,
) -> pd.DataFrame:
    pwm = _fetch_pwm_exceedance(provider, resolution, p=pwm_p, refresh=refresh)
    eot_high = _fetch_eot_tail(provider, resolution, tail="high", q=eot_q, refresh=refresh)
    eot_low = _fetch_eot_tail(provider, resolution, tail="low", q=eot_q, refresh=refresh)
    eot_all = _fetch_eot_all(provider, resolution, q=eot_q, refresh=refresh)
    merged = _merge_two(
        pwm,
        eot_high,
        left_map={
            "mean_high_exceedance": "left_high_mean",
            "median_high_exceedance": "left_high_median",
            "mean_low_exceedance": "left_low_mean",
            "median_low_exceedance": "left_low_median",
            "mean_all_exceedance": "left_all_mean",
            "median_all_exceedance": "left_all_median",
        },
        right_map={
            "mean_extremes_freq": "right_high_mean",
            "median_extremes_freq": "right_high_median",
        },
    )
    merged = merged.merge(
        eot_low[["cell_lat", "cell_lon", "mean_extremes_freq", "median_extremes_freq"]].rename(
            columns={
                "mean_extremes_freq": "right_low_mean",
                "median_extremes_freq": "right_low_median",
            }
        ),
        on=["cell_lat", "cell_lon"],
        how="outer",
    )
    merged = merged.merge(
        eot_all[["cell_lat", "cell_lon", "mean_all_extremes_freq", "median_all_extremes_freq"]].rename(
            columns={
                "mean_all_extremes_freq": "right_all_mean",
                "median_all_extremes_freq": "right_all_median",
            }
        ),
        on=["cell_lat", "cell_lon"],
        how="outer",
    )
    for col in merged.columns:
        if col.startswith("left_") or col.startswith("right_"):
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    return merged


def _standardize_gt10_vs_full(
    resolution: float,
    *,
    domain: str,
    refresh: bool = False,
    gt10_dir: Path,
    full_dir: Path,
) -> pd.DataFrame:
    """Standardize gt10 vs full comparison for a given algorithm domain."""
    gt10_provider = _build_provider_for_parquet(gt10_dir)
    full_provider = _build_provider_for_parquet(full_dir)

    if domain == "quantile":
        gt10 = _fetch_quantile_per_lake_stats(gt10_provider, resolution, refresh=refresh)
        full = _fetch_quantile_per_lake_stats(full_provider, resolution, refresh=refresh)
        q_metrics = ["mean_high", "median_high", "mean_low", "median_low", "mean_all", "median_all"]
        left_map = {m: f"left_{m}" for m in q_metrics}
        right_map = {m: f"right_{m}" for m in q_metrics}
        return _merge_two(gt10, full, left_map=left_map, right_map=right_map)

    elif domain == "pwm":
        gt10 = _fetch_pwm_exceedance(gt10_provider, resolution, p=0.05, refresh=refresh)
        full = _fetch_pwm_exceedance(full_provider, resolution, p=0.05, refresh=refresh)
        mapping = {
            "mean_high_exceedance": "high_mean",
            "median_high_exceedance": "high_median",
            "mean_low_exceedance": "low_mean",
            "median_low_exceedance": "low_median",
            "mean_all_exceedance": "all_mean",
            "median_all_exceedance": "all_median",
        }
        left_map = {k: f"left_{v}" for k, v in mapping.items()}
        right_map = {k: f"right_{v}" for k, v in mapping.items()}
        return _merge_two(gt10, full, left_map=left_map, right_map=right_map)

    elif domain == "eot":
        gt10_high = _fetch_eot_tail(gt10_provider, resolution, tail="high", q=0.95, refresh=refresh)
        gt10_low = _fetch_eot_tail(gt10_provider, resolution, tail="low", q=0.95, refresh=refresh)
        gt10_all = _fetch_eot_all(gt10_provider, resolution, q=0.95, refresh=refresh)
        full_high = _fetch_eot_tail(full_provider, resolution, tail="high", q=0.95, refresh=refresh)
        full_low = _fetch_eot_tail(full_provider, resolution, tail="low", q=0.95, refresh=refresh)
        full_all = _fetch_eot_all(full_provider, resolution, q=0.95, refresh=refresh)

        merged = _merge_two(
            gt10_high, full_high,
            left_map={"mean_extremes_freq": "left_high_mean", "median_extremes_freq": "left_high_median"},
            right_map={"mean_extremes_freq": "right_high_mean", "median_extremes_freq": "right_high_median"},
        )
        merged = merged.merge(
            _merge_two(
                gt10_low, full_low,
                left_map={"mean_extremes_freq": "left_low_mean", "median_extremes_freq": "left_low_median"},
                right_map={"mean_extremes_freq": "right_low_mean", "median_extremes_freq": "right_low_median"},
            )[["cell_lat", "cell_lon", "left_low_mean", "left_low_median", "right_low_mean", "right_low_median"]],
            on=["cell_lat", "cell_lon"], how="outer",
        )
        merged = merged.merge(
            _merge_two(
                gt10_all, full_all,
                left_map={"mean_all_extremes_freq": "left_all_mean", "median_all_extremes_freq": "left_all_median"},
                right_map={"mean_all_extremes_freq": "right_all_mean", "median_all_extremes_freq": "right_all_median"},
            )[["cell_lat", "cell_lon", "left_all_mean", "left_all_median", "right_all_mean", "right_all_median"]],
            on=["cell_lat", "cell_lon"], how="outer",
        )
        for col in merged.columns:
            if col.startswith("left_") or col.startswith("right_"):
                merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
        return merged

    else:
        raise ValueError(f"Unknown domain for gt10 vs full comparison: {domain}")


def _build_provider_for_parquet(data_dir: Path):
    provider = create_provider(SourceConfig(backend=Backend.PARQUET, data_dir=data_dir))
    # `data/parquet` and `data/parquet_gt10` would otherwise both use `data/cache`.
    # Isolate cache roots so backend-to-backend comparisons can honor `refresh=False`.
    if hasattr(provider, "_cache_dir"):
        provider._cache_dir = data_dir.parent / "cache_comparison" / data_dir.name
        provider._cache_dir.mkdir(parents=True, exist_ok=True)
    return provider


def _standardize_gt10_vs_full(
    resolution: float,
    *,
    domain: str,
    refresh=False,
    gt10_dir: Path,
    full_dir: Path,
) -> pd.DataFrame:
    gt10 = _build_provider_for_parquet(gt10_dir)
    full = _build_provider_for_parquet(full_dir)
    if domain == "quantile":
        left = _fetch_quantile_per_lake_stats(gt10, resolution, refresh=refresh)
        right = _fetch_quantile_per_lake_stats(full, resolution, refresh=refresh)
        mapping = {
            "mean_high": "high_mean",
            "median_high": "high_median",
            "mean_low": "low_mean",
            "median_low": "low_median",
            "mean_all": "all_mean",
            "median_all": "all_median",
        }
    elif domain == "pwm":
        left = _fetch_pwm_exceedance(gt10, resolution, p=0.05, refresh=refresh)
        right = _fetch_pwm_exceedance(full, resolution, p=0.05, refresh=refresh)
        mapping = {
            "mean_high_exceedance": "high_mean",
            "median_high_exceedance": "high_median",
            "mean_low_exceedance": "low_mean",
            "median_low_exceedance": "low_median",
            "mean_all_exceedance": "all_mean",
            "median_all_exceedance": "all_median",
        }
    elif domain == "eot":
        high_left = _fetch_eot_tail(gt10, resolution, tail="high", q=0.95, refresh=refresh)
        high_right = _fetch_eot_tail(full, resolution, tail="high", q=0.95, refresh=refresh)
        low_left = _fetch_eot_tail(gt10, resolution, tail="low", q=0.95, refresh=refresh)
        low_right = _fetch_eot_tail(full, resolution, tail="low", q=0.95, refresh=refresh)
        all_left = _fetch_eot_all(gt10, resolution, q=0.95, refresh=refresh)
        all_right = _fetch_eot_all(full, resolution, q=0.95, refresh=refresh)
        merged = _merge_two(
            high_left,
            high_right,
            left_map={"mean_extremes_freq": "left_high_mean", "median_extremes_freq": "left_high_median"},
            right_map={"mean_extremes_freq": "right_high_mean", "median_extremes_freq": "right_high_median"},
        )
        merged = merged.merge(
            _merge_two(
                low_left,
                low_right,
                left_map={"mean_extremes_freq": "left_low_mean", "median_extremes_freq": "left_low_median"},
                right_map={"mean_extremes_freq": "right_low_mean", "median_extremes_freq": "right_low_median"},
            )[["cell_lat", "cell_lon", "left_low_mean", "left_low_median", "right_low_mean", "right_low_median"]],
            on=["cell_lat", "cell_lon"],
            how="outer",
        )
        merged = merged.merge(
            _merge_two(
                all_left,
                all_right,
                left_map={"mean_all_extremes_freq": "left_all_mean", "median_all_extremes_freq": "left_all_median"},
                right_map={"mean_all_extremes_freq": "right_all_mean", "median_all_extremes_freq": "right_all_median"},
            )[["cell_lat", "cell_lon", "left_all_mean", "left_all_median", "right_all_mean", "right_all_median"]],
            on=["cell_lat", "cell_lon"],
            how="outer",
        )
        for col in merged.columns:
            if col.startswith("left_") or col.startswith("right_"):
                merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
        return merged
    else:
        raise ValueError(f"Unsupported domain: {domain}")
    return _merge_two(
        left,
        right,
        left_map={k: f"left_{v}" for k, v in mapping.items()},
        right_map={k: f"right_{v}" for k, v in mapping.items()},
    )


def _add_row_cbar(fig, right_ax, metas, *, cmap_name, label="", use_int_bounds=False):
    import matplotlib.colors as mcolors
    import matplotlib.ticker as mticker

    from ..style.presets import resolve_cmap

    resolved_cmap = resolve_cmap(cmap_name)
    if use_int_bounds:
        bounds = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        norm = mcolors.BoundaryNorm(bounds, ncolors=len(bounds) + 1, extend="both")
        for meta in metas:
            meta["mesh"].set_norm(norm)
            meta["mesh"].set_cmap(resolved_cmap)
        bbox = right_ax.get_position()
        cbar_ax = fig.add_axes([bbox.x1 + 0.01, bbox.y0, 0.015, bbox.y1 - bbox.y0])
        sm = plt.cm.ScalarMappable(cmap=resolved_cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, cax=cbar_ax, extend="both", extendrect=False)
        cbar.set_ticks(bounds)
        cbar.ax.tick_params(labelsize=8)
        cbar.ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%d"))
        if label:
            cbar.ax.set_ylabel(label, fontsize=9)
        return

    vmin_shared = min(m["vmin"] for m in metas)
    vmax_shared = max(m["vmax"] for m in metas)
    log_scale = metas[0]["log_scale"]
    n_levels = len(metas[0]["bounds"]) - 1
    if log_scale and vmin_shared > 0:
        bounds = np.logspace(np.log10(vmin_shared), np.log10(vmax_shared), n_levels + 1)
    else:
        bounds = np.linspace(vmin_shared, vmax_shared, n_levels + 1)
    norm = mcolors.BoundaryNorm(bounds, ncolors=n_levels)
    for meta in metas:
        meta["mesh"].set_norm(norm)
        meta["mesh"].set_cmap(resolved_cmap)
    bbox = right_ax.get_position()
    cbar_ax = fig.add_axes([bbox.x1 + 0.01, bbox.y0, 0.015, bbox.y1 - bbox.y0])
    sm = plt.cm.ScalarMappable(cmap=resolved_cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cbar_ax, extendrect=True, extendfrac="auto")
    cbar.set_ticks(bounds)
    cbar.ax.tick_params(labelsize=8)
    cbar.ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.2g}"))
    if label:
        cbar.ax.set_ylabel(label, fontsize=9)


def _render_two_panel(
    config: GlobalGridConfig,
    df: pd.DataFrame,
    *,
    left_col: str,
    right_col: str,
    left_title: str,
    right_title: str,
    cmap: str,
    log_scale: bool,
    label: str,
    out_path: Path,
    sigma: float = 1.0,
    target_res: float = 0.1,
    use_int_bounds: bool = False,
    min_lakes: int = 1,
    draw_hatch: bool = False,
) -> Path | None:
    import cartopy.crs as ccrs

    if df.empty or left_col not in df.columns or right_col not in df.columns:
        return None

    coords = agg_to_grid_matrix(df, left_col, config.resolution)
    lons, lats = coords[0], coords[1]

    no_lakes_mask = None
    if "lake_count" in df.columns:
        from scipy.ndimage import gaussian_filter
        _, _, lake_counts = agg_to_grid_matrix(df, "lake_count", config.resolution)
        has_data = (~np.isnan(lake_counts)) & (lake_counts >= min_lakes)
        land_mask = _get_land_mask(lons, lats, config.resolution)
        # Exclude Antarctica (treat as ocean: no hatch)
        land_no_antarctica = land_mask & (lats[:, None] >= -60)
        # Match density boundary: same sigma / threshold as draw_global_density
        weight = gaussian_filter(has_data.astype(float), sigma=sigma, mode="constant", cval=0.0)
        hatch_cells = ~has_data & land_no_antarctica & (weight < 0.05)
        if np.any(hatch_cells):
            no_lakes_mask = hatch_cells

    fig, axes = create_figure(
        [{"name": "left", "row": 0, "col": 0}, {"name": "right", "row": 0, "col": 1}],
        figsize=(12, 5), width_ratios=[1, 1], projection=ccrs.Robinson(),
    )
    actual_log_scale = False if use_int_bounds else log_scale
    metas = []
    for ax_name, value_col, title in (("left", left_col, left_title), ("right", right_col, right_title)):
        _, _, values = agg_to_grid_matrix(df, value_col, config.resolution)
        meta = draw_global_density(
            axes[ax_name], lons, lats, values,
            title=title, cmap=cmap, log_scale=actual_log_scale,
            cbar_label=label, sigma=sigma, target_res=target_res,
            add_cbar=False,
        )
        if meta is not None:
            metas.append(meta)
        if draw_hatch and no_lakes_mask is not None:
            with plt.rc_context({"hatch.linewidth": 0.35}):
                axes[ax_name].contourf(
                    lons,
                    lats,
                    no_lakes_mask.astype(float),
                    levels=[0.5, 1.5],
                    hatches=["///"],
                    colors="none",
                    transform=ccrs.PlateCarree(),
                    zorder=5,
                )
    if metas:
        _add_row_cbar(fig, axes["right"], metas, cmap_name=cmap, label=label, use_int_bounds=use_int_bounds)
    return save(fig, out_path, bbox_inches=None)


def _render_six_panels(
    config: GlobalGridConfig,
    df: pd.DataFrame,
    *,
    sub_dir: str,
    left_label: str,
    right_label: str,
    min_lakes: int = 1,
    use_int_bounds: bool = False,
    draw_hatch: bool = False,
) -> list[Path]:
    if df.empty:
        return []
    specs = [
        ("high", "mean", "sequential_warm", True, "均值"),
        ("high", "median", "sequential_warm", True, "中位数"),
        ("low", "mean", "sequential_cool", True, "均值"),
        ("low", "median", "sequential_cool", True, "中位数"),
        ("all", "mean", "sequential_warm", True, "均值"),
        ("all", "median", "sequential_warm", True, "中位数"),
    ]
    stat_label = {"high": "高值异常", "low": "低值异常", "all": "全部异常"}
    outputs: list[Path] = []
    for stat, agg, cmap, log_scale, agg_label in specs:
        flat_sub_dir = sub_dir.replace("/", "_")
        out_path = config.output_dir / f"{flat_sub_dir}_{stat}_{agg}.png"
        result = _render_two_panel(
            config,
            df,
            left_col=f"left_{stat}_{agg}",
            right_col=f"right_{stat}_{agg}",
            left_title=f"{left_label} {stat_label[stat]} {agg_label}",
            right_title=f"{right_label} {stat_label[stat]} {agg_label}",
            cmap=cmap,
            log_scale=log_scale,
            label=agg_label,
            out_path=out_path,
            use_int_bounds=use_int_bounds,
            min_lakes=min_lakes,
            draw_hatch=draw_hatch,
        )
        if result is not None:
            outputs.append(result)
    return outputs


def plot_pwm_pvalue_panels(
    config: GlobalGridConfig,
    *,
    p1: float = 0.01,
    p2: float = 0.05,
    refresh: bool = False,
    min_lakes: int = 1,
    draw_hatch: bool = False,
) -> list[Path]:
    df = _standardize_pwm_pvalues(config.provider, config.resolution, p1=p1, p2=p2, refresh=refresh)
    return _render_six_panels(
        config, df,
        sub_dir="comparison_pwm_pvalue",
        left_label=f"PWM p={p1}",
        right_label=f"PWM p={p2}",
        min_lakes=min_lakes,
        draw_hatch=draw_hatch,
        use_int_bounds=True,
    )


def plot_eot_quantile_panels(
    config: GlobalGridConfig,
    *,
    q1: float = 0.95,
    q2: float = 0.98,
    refresh: bool = False,
    min_lakes: int = 3,
    draw_hatch: bool = False,
) -> list[Path]:
    df = _standardize_eot_quantiles(config.provider, config.resolution, q1=q1, q2=q2, refresh=refresh)
    return _render_six_panels(
        config, df,
        sub_dir="comparison_eot_quantile",
        left_label=f"EOT q={q1}",
        right_label=f"EOT q={q2}",
        min_lakes=min_lakes,
        draw_hatch=draw_hatch,
    )


def plot_quantile_vs_pwm_panels(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
    draw_hatch: bool = False,
) -> list[Path]:
    df = _standardize_quantile_vs_pwm(config.provider, config.resolution, refresh=refresh)
    return _render_six_panels(
        config, df,
        sub_dir="comparison_quantile_vs_pwm",
        left_label="Quantile",
        right_label="PWM",
        min_lakes=min_lakes,
        use_int_bounds=True,
        draw_hatch=draw_hatch,
    )


def plot_pwm_vs_eot_panels(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
    draw_hatch: bool = False,
) -> list[Path]:
    df = _standardize_pwm_vs_eot(config.provider, config.resolution, refresh=refresh)
    return _render_six_panels(
        config, df,
        sub_dir="comparison_pwm_vs_eot",
        left_label="PWM",
        right_label="EOT",
        min_lakes=min_lakes,
        draw_hatch=draw_hatch,
    )


def plot_gt10_vs_full_panels(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
    gt10_dir: Path,
    full_dir: Path,
    draw_hatch: bool = False,
    domains: tuple[str, ...] = ("quantile", "pwm", "eot"),
) -> list[Path]:
    """Plot gt10 vs full panels for given algorithm domains."""
    outputs: list[Path] = []
    int_bounds_map = {"quantile": True, "pwm": True, "eot": False}
    for domain in domains:
        df = _standardize_gt10_vs_full(
            config.resolution,
            domain=domain,
            refresh=refresh,
            gt10_dir=gt10_dir,
            full_dir=full_dir,
        )
        domain_outputs = _render_six_panels(
            config,
            df,
            sub_dir="comparison_gt10_vs_full",
            left_label="gt10",
            right_label="full",
            min_lakes=min_lakes,
            use_int_bounds=int_bounds_map[domain],
            draw_hatch=draw_hatch,
        )
        renamed_outputs: list[Path] = []
        renamed_outputs.extend(domain_outputs)
        outputs.extend(renamed_outputs)
    return outputs
