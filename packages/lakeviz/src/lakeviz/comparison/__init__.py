"""Comparison visualization panels."""

from .global_map import (
    plot_eot_quantile_panels,
    plot_gt10_vs_full_panels,
    plot_pwm_pvalue_panels,
    plot_pwm_vs_eot_panels,
    plot_quantile_vs_pwm_panels,
)
from .zonal_profile import plot_comparison_zonal_profile

__all__ = [
    "plot_eot_quantile_panels",
    "plot_gt10_vs_full_panels",
    "plot_pwm_pvalue_panels",
    "plot_pwm_vs_eot_panels",
    "plot_quantile_vs_pwm_panels",
    "plot_comparison_zonal_profile",
]
