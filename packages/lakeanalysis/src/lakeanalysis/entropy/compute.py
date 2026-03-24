"""Pure functions for Apportionment Entropy (AE) computation.

No database dependencies. All functions operate on pandas DataFrames and numpy arrays.

AE formula (equation 2 from the paper):
    AE_k = -sum_{m=1}^{12} (x_{m,k} / X_k) * log2(x_{m,k} / X_k)

where X_k = sum of monthly values in year k.

For lake area data the raw monthly values include a large permanent-water
baseline that causes proportions to be nearly uniform across all months,
pushing AE to its theoretical maximum for every lake.  To isolate the
seasonal signal, the baseline is removed before computing AE:

    x'_{m,k} = x_{m,k} - min_m(x_{m,k})

The result is then normalised to [0, 1] by dividing by log2(12):

    AE_norm = AE(x') / log2(12)

A value of 0 means all seasonal variation is concentrated in a single month;
1 means the variation is spread uniformly across all 12 months.
Lakes with no detectable seasonal variation (x' all-zero) return nan.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pymannkendall as mk

log = logging.getLogger(__name__)

MIN_YEARS_TREND = 20


def ae_from_values(values: np.ndarray) -> float:
    """Compute Apportionment Entropy from an array of non-negative values.

    Zero entries contribute 0 to the sum (lim_{p->0} p*log2(p) = 0).

    Args:
        values: 1-D array of non-negative monthly water area values.

    Returns:
        AE as a float, or nan if the total is zero or non-positive.
    """
    total = float(np.sum(values))
    if total <= 0:
        log.debug("ae_from_values: total <= 0, returning nan")
        return float("nan")
    p = values / total
    p_nonzero = p[p > 0]
    return float(-np.sum(p_nonzero * np.log2(p_nonzero)))


def compute_overall_ae(df: pd.DataFrame) -> float:
    """Compute normalised overall AE from the 12-month climatological mean.

    Assumes inter-annual stationarity: the multi-year mean for each calendar
    month (1–12) is the best estimate of the long-term seasonal signal.
    The permanent-water baseline is removed by subtracting the minimum monthly
    mean before computing AE, then the result is normalised to [0, 1] by
    dividing by log2(12).

    Args:
        df: DataFrame with columns ['month', 'water_area']. Must not be empty.

    Returns:
        Normalised AE in [0, 1]. nan if all baseline-removed values are zero
        (no detectable seasonal variation).
    """
    monthly_mean = df.groupby("month")["water_area"].mean()
    values = monthly_mean.reindex(range(1, 13), fill_value=0.0).to_numpy(dtype=float)
    shifted = values - values.min()
    ae = ae_from_values(shifted)
    if np.isnan(ae):
        return ae
    return ae / np.log2(12)


def compute_annual_ae(df: pd.DataFrame, min_months: int = 10) -> pd.DataFrame:
    """Compute normalised AE for each year, filtering years with insufficient data.

    Applies baseline removal (subtract monthly minimum) and normalisation
    (divide by log2(12)) per year before computing AE.
    Also computes AE_anomaly = AE - mean(AE) over the full valid record.

    Args:
        df: DataFrame with columns ['year', 'month', 'water_area'].
        min_months: Minimum valid months required per year (default 10, per paper).

    Returns:
        DataFrame with columns [year, AE, AE_anomaly], sorted by year.
        AE values are in [0, 1]. Returns empty DataFrame if no year passes
        the filter or all shifted values are zero.
    """
    records: list[dict] = []
    for year, group in df.groupby("year"):
        if len(group) < min_months:
            continue
        values = group["water_area"].to_numpy(dtype=float)
        shifted = values - values.min()
        ae = ae_from_values(shifted)
        if not np.isnan(ae):
            records.append({"year": int(year), "AE": ae / np.log2(12)})

    if not records:
        log.debug("compute_annual_ae: no year passed min_months=%d filter", min_months)
        return pd.DataFrame(columns=["year", "AE", "AE_anomaly"])

    annual = pd.DataFrame(records).sort_values("year").reset_index(drop=True)
    annual["AE_anomaly"] = annual["AE"] - annual["AE"].mean()
    return annual


def compute_trend(annual_df: pd.DataFrame) -> dict:
    """Apply Sen's slope and Mann-Kendall test to an annual AE series.

    Sen's slope change ratio (equation 3 from the paper):
        change_per_decade_pct = (slope * 10 / mean_AE) * 100

    Args:
        annual_df: DataFrame with columns ['year', 'AE'] sorted by year.

    Returns:
        Dict with keys:
            n_years, sens_slope, change_per_decade_pct,
            mk_trend, mk_p, mk_z, mk_significant.
        All numeric values are None if fewer than MIN_YEARS_TREND valid years exist.
    """
    null_result: dict = {
        "n_years": len(annual_df),
        "sens_slope": None,
        "change_per_decade_pct": None,
        "mk_trend": None,
        "mk_p": None,
        "mk_z": None,
        "mk_significant": None,
    }

    valid = annual_df.dropna(subset=["AE"]).sort_values("year")
    n = len(valid)
    null_result["n_years"] = n

    if n < MIN_YEARS_TREND:
        log.debug("compute_trend: n=%d < MIN_YEARS_TREND=%d, returning null result", n, MIN_YEARS_TREND)
        return null_result

    values = valid["AE"].to_numpy(dtype=float)
    result = mk.original_test(values)

    mean_ae = float(np.mean(values))
    change_per_decade = (result.slope * 10 / mean_ae * 100) if mean_ae != 0 else None

    return {
        "n_years": n,
        "sens_slope": float(result.slope),
        "change_per_decade_pct": float(change_per_decade) if change_per_decade is not None else None,
        "mk_trend": result.trend,
        "mk_p": float(result.p),
        "mk_z": float(result.z),
        "mk_significant": bool(result.p < 0.05),
    }
