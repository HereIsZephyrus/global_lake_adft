"""Project-level matplotlib configuration (fonts, etc.)."""

from __future__ import annotations

import matplotlib.pyplot as plt


def setup_chinese_font() -> None:
    """Use Unifont for CJK labels and fix minus sign rendering."""
    plt.rcParams["font.sans-serif"] = ["Unifont", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
