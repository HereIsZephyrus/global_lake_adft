"""Anomaly filter protocol and core types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd


FLAG_MEDIAN_ZERO = 1
FLAG_ZERO_QUANTILE = 1
FLAG_FLAT = 2
FLAG_AREA_RATIO = 4
FLAG_OUTSIDE_RANGE = 8
FLAG_PV = 16

FLAG_NAMES: dict[int, str] = {
    FLAG_ZERO_QUANTILE: "zero_quantile",
    FLAG_FLAT: "flat",
    FLAG_AREA_RATIO: "area_ratio",
    FLAG_OUTSIDE_RANGE: "outside_range",
    FLAG_PV: "pv",
}


def decode_anomaly_flags(flags: int) -> dict[str, bool]:
    """Decode an integer anomaly bitmask into a name→bool dict."""
    return {name: bool(flags & bit) for bit, name in FLAG_NAMES.items()}


def encode_anomaly_flags(names: dict[str, bool]) -> int:
    """Encode a name→bool dict into an integer anomaly bitmask."""
    bit_by_name = {v: k for k, v in FLAG_NAMES.items()}
    flags = 0
    for name, is_set in names.items():
        if is_set and name in bit_by_name:
            flags |= bit_by_name[name]
    return flags


@dataclass(frozen=True)
class LakeContext:
    """Single-lake data bundle passed to every filter."""

    df: pd.DataFrame
    df_no_frozen: pd.DataFrame
    rs_area_median: float
    rs_area_mean: float
    rs_area_quantile: float
    atlas_area: float


@dataclass(frozen=True)
class AnomalyFlag:
    """Result from a single anomaly filter."""

    name: str
    is_anomaly: bool
    detail: dict[str, float | bool]


class AnomalyFilter(Protocol):
    """Protocol for anomaly classification filters."""

    name: str

    def classify(self, ctx: LakeContext) -> AnomalyFlag: ...
