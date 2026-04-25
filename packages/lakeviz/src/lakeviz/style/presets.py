"""Domain-specific style presets.

Centralises all colour / line / marker defaults so that no domain
``draw_*`` function hard-codes visual parameters.  Users can override
any preset by passing a custom style object.
"""

from __future__ import annotations

from .line import LineStyle
from .scatter import ScatterStyle
from .bar import BarStyle
from .histogram import HistogramStyle
from .fill import FillStyle
from .reference import ReferenceLineStyle
from .base import AxisStyle


# ---------------------------------------------------------------------------
# EOT presets
# ---------------------------------------------------------------------------
EOT_LINE = LineStyle(color="steelblue", linewidth=1.0, label="月序列")
EOT_EXTREME_HIGH = ScatterStyle(
    color="#E74C3C", marker="^", s=55,
    edgecolors="#C0392B", linewidths=0.8,
    label="EOT高值极端", zorder=4,
)
EOT_EXTREME_LOW = ScatterStyle(
    color="#27AE60", marker="v", s=55,
    edgecolors="#1E8449", linewidths=0.8,
    label="EOT低值极端", zorder=4,
)
EOT_THRESHOLD = LineStyle(
    color="tomato", linestyle="--", linewidth=1.2, label="阈值",
)
EOT_LOCATION = LineStyle(
    color="purple", linewidth=1.5, label="mu(t)",
)

# ---------------------------------------------------------------------------
# Hawkes presets
# ---------------------------------------------------------------------------
HAWKES_D_TO_W_BAND = ScatterStyle(color="#8B008B", alpha=0.18)
HAWKES_W_TO_D_BAND = ScatterStyle(color="#D2691E", alpha=0.18)
HAWKES_D_TO_W_LABEL = "Hawkes 旱→涝显著月"
HAWKES_W_TO_D_LABEL = "Hawkes 涝→旱显著月"

# ---------------------------------------------------------------------------
# Quantile presets
# ---------------------------------------------------------------------------
QUANTILE_WATER_AREA = LineStyle(color="steelblue", linewidth=1.5, label="water_area")
QUANTILE_CLIMATOLOGY = LineStyle(
    color="steelblue", linewidth=1.2, linestyle="--", label="monthly_climatology",
)
QUANTILE_EXTREME_HIGH = ScatterStyle(color="tab:red", s=28, label="extreme_high")
QUANTILE_EXTREME_LOW = ScatterStyle(color="tab:blue", s=28, label="extreme_low")
QUANTILE_TRANSITION_L2H = ReferenceLineStyle(
    color="tab:green", linestyle=":", linewidth=0.9, alpha=0.8,
)
QUANTILE_TRANSITION_H2L = ReferenceLineStyle(
    color="tab:orange", linestyle=":", linewidth=0.9, alpha=0.8,
)

# ---------------------------------------------------------------------------
# ADFT fallback presets
# ---------------------------------------------------------------------------
ADFT_LINE = LineStyle(
    color="steelblue", linewidth=2, marker="o", markersize=3,
    label="水域面积", zorder=1, alpha=0.8,
)
ADFT_D_TO_W = LineStyle(color="#8B008B", linewidth=4, zorder=3)
ADFT_W_TO_D = LineStyle(color="#D2691E", linewidth=4, zorder=3)

# ---------------------------------------------------------------------------
# Basemodel presets
# ---------------------------------------------------------------------------
BASEMODEL_ORIGINAL = LineStyle(color="steelblue", linewidth=1.2, label="原始序列")
BASEMODEL_FITTED = LineStyle(color="tomato", linewidth=1.2, label="拟合序列")
BASEMODEL_RESIDUAL = LineStyle(color="purple", linewidth=1.0)

# ---------------------------------------------------------------------------
# Entropy presets
# ---------------------------------------------------------------------------
ENTROPY_AE_LINE = LineStyle(marker="o", markersize=3, linewidth=1, label="年度 AE")
ENTROPY_ANOMALY_POS = BarStyle(color="steelblue")
ENTROPY_ANOMALY_NEG = BarStyle(color="tomato")

# ---------------------------------------------------------------------------
# Similarity presets
# ---------------------------------------------------------------------------
SIMILARITY_SCATTER = ScatterStyle(alpha=0.4, s=10, rasterized=True)

# ---------------------------------------------------------------------------
# Global theme
# ---------------------------------------------------------------------------
class Theme:
    PRIMARY = "steelblue"
    DANGER = "tomato"
    SUCCESS = "seagreen"
    WARNING = "#D2691E"
    EXTREME_HIGH = "#E74C3C"
    EXTREME_LOW = "#27AE60"
    DROUGHT_TO_WET = "#8B008B"
    WET_TO_DROUGHT = "#D2691E"

    @staticmethod
    def apply() -> None:
        import matplotlib.pyplot as plt
        plt.rcParams["font.sans-serif"] = ["Unifont", "DejaVu Sans"]
        plt.rcParams["axes.unicode_minus"] = False