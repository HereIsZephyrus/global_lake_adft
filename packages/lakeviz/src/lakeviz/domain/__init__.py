"""Domain-level draw functions — thin re-exports for convenience."""

from .eot import (
    draw_mrl,
    draw_parameter_stability,
    draw_extremes_timeline,
    draw_pp,
    draw_qq,
    draw_return_levels,
    draw_location_model,
    draw_eot_extremes,
    draw_extremes_with_hawkes,
)
from .hawkes import (
    draw_event_timeline,
    draw_intensity_decomposition,
    draw_kernel_matrix,
    draw_lrt_summary,
)
from .interpolation import (
    draw_interpolation_timeline,
)
from .quantile import (
    draw_monthly_timeline,
    draw_anomaly_timeline,
    draw_transition_count_summary,
    draw_transition_count_summary_from_cache,
    draw_transition_seasonality_summary,
    draw_transition_seasonality_summary_from_cache,
    draw_adft_fallback,
)
from .basemodel import (
    draw_candidate_scores,
    draw_basis_fit,
    draw_residuals,
)
from .entropy import (
    draw_ae_distribution,
    draw_amplitude_histogram,
    draw_annual_ae,
    draw_trend_summary,
    draw_amplitude_vs_entropy,
)
from .similarity import (
    draw_pearson_distribution,
    draw_acf_cosine_distribution,
    draw_pearson_vs_acf,
)
from .pwm_extreme import (
    draw_quantile_function,
    draw_threshold_summary,
)
from .interpolation import (
    draw_interpolation_timeline,
)

__all__ = [
    "draw_mrl",
    "draw_parameter_stability",
    "draw_extremes_timeline",
    "draw_pp",
    "draw_qq",
    "draw_return_levels",
    "draw_location_model",
    "draw_eot_extremes",
    "draw_extremes_with_hawkes",
    "draw_event_timeline",
    "draw_intensity_decomposition",
    "draw_kernel_matrix",
    "draw_lrt_summary",
    "draw_interpolation_timeline",
    "draw_monthly_timeline",
    "draw_anomaly_timeline",
    "draw_transition_count_summary",
    "draw_transition_count_summary_from_cache",
    "draw_transition_seasonality_summary",
    "draw_transition_seasonality_summary_from_cache",
    "draw_adft_fallback",
    "draw_candidate_scores",
    "draw_basis_fit",
    "draw_residuals",
    "draw_ae_distribution",
    "draw_amplitude_histogram",
    "draw_annual_ae",
    "draw_trend_summary",
    "draw_amplitude_vs_entropy",
    "draw_pearson_distribution",
    "draw_acf_cosine_distribution",
    "draw_pearson_vs_acf",
    "draw_quantile_function",
    "draw_threshold_summary",
    "draw_interpolation_timeline",
]