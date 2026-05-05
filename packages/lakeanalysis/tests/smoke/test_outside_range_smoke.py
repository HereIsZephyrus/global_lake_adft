"""Smoke tests for outside_range detection against parquet data.

Verifies that compute_area_range and classify_outside_range work
end-to-end with real lake_area and area_quality parquet files.

Run with:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
        pytest tests/smoke/test_outside_range_smoke.py -v -s
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def _get_parquet_dir() -> Path:
    d = os.environ.get("SMOKE_PARQUET_DIR") or os.environ.get("PARQUET_DATA_DIR") or os.environ.get("LAKE_DATA_DIR")
    if not d:
        pytest.skip("PARQUET_DATA_DIR or SMOKE_PARQUET_DIR env var required")
    p = Path(d)
    if not p.exists():
        pytest.skip(f"Parquet data dir does not exist: {p}")
    return p


@pytest.fixture(scope="module")
def parquet_dir():
    return _get_parquet_dir()


@pytest.fixture(scope="module")
def area_quality_df(parquet_dir):
    from lakesource.parquet.client import DuckDBClient

    client = DuckDBClient(data_dir=parquet_dir)
    tables = client.list_registered_tables()
    if "area_quality" not in tables:
        pytest.skip("area_quality table not found in parquet data")
    return client.query_df(
        "SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area "
        "FROM area_quality ORDER BY hylak_id LIMIT 500"
    )


@pytest.fixture(scope="module")
def lake_range_df(parquet_dir, area_quality_df):
    from lakesource.parquet.client import DuckDBClient

    client = DuckDBClient(data_dir=parquet_dir)
    tables = client.list_registered_tables()
    if "lake_area" not in tables:
        pytest.skip("lake_area table not found in parquet data")

    hylak_ids = area_quality_df["hylak_id"].astype(int).tolist()
    placeholders = ",".join("?" for _ in hylak_ids)
    return client.query_df(
        f"SELECT hylak_id, "
        f"MIN(water_area) / 1e6 AS min_area_km2, "
        f"MAX(water_area) / 1e6 AS max_area_km2 "
        f"FROM lake_area "
        f"WHERE hylak_id IN ({placeholders}) "
        f"GROUP BY hylak_id "
        f"ORDER BY hylak_id",
        parameters=hylak_ids,
    )


def test_compute_area_range_on_real_data(parquet_dir, area_quality_df):
    from lakesource.parquet.client import DuckDBClient
    from lakeanalysis.quality import compute_area_range

    client = DuckDBClient(data_dir=parquet_dir)
    sample_ids = area_quality_df["hylak_id"].astype(int).head(5).tolist()
    placeholders = ",".join("?" for _ in sample_ids)
    df = client.query_df(
        f"SELECT hylak_id, water_area FROM lake_area "
        f"WHERE hylak_id IN ({placeholders}) "
        f"ORDER BY hylak_id, year_month",
        parameters=sample_ids,
    )
    assert not df.empty, "No lake_area data for sample IDs"

    for hylak_id in sample_ids:
        lake_df = df[df["hylak_id"] == hylak_id]
        result = compute_area_range(lake_df)
        assert "min_area" in result
        assert "max_area" in result
        assert result["min_area"] >= 0
        assert result["max_area"] >= result["min_area"]


def test_classify_outside_range_on_real_data(area_quality_df, lake_range_df):
    from lakeanalysis.quality import classify_outside_range

    merged = area_quality_df.merge(lake_range_df, on="hylak_id", how="inner")
    assert len(merged) > 0, "No matching lakes between area_quality and lake_area"

    results = []
    for _, row in merged.iterrows():
        result = classify_outside_range(
            atlas_area=float(row["atlas_area"]),
            min_area=float(row["min_area_km2"]),
            max_area=float(row["max_area_km2"]),
        )
        results.append(result)

    n_outside = sum(1 for r in results if r["is_outside_range"])
    n_below = sum(1 for r in results if r["is_below_min"])
    n_above = sum(1 for r in results if r["is_above_max"])

    assert n_outside == n_below + n_above
    assert n_outside >= 0

    total = len(results)
    print(f"\nOutside range: {n_outside}/{total} ({n_outside/total*100:.1f}%)")
    print(f"  Below min: {n_below}")
    print(f"  Above max: {n_above}")


def test_outside_range_vs_agreement_overlap(area_quality_df, lake_range_df):
    from lakeanalysis.quality import (
        AgreementConfig,
        classify_agreement,
        classify_outside_range,
        compute_area_ratio,
    )

    merged = area_quality_df.merge(lake_range_df, on="hylak_id", how="inner")
    assert len(merged) > 0

    config = AgreementConfig()
    ratio = compute_area_ratio(merged["rs_area_median"].values, merged["atlas_area"].values)
    agreement = classify_agreement(ratio, config)

    outside_set = set()
    poor_plus_set = set()
    moderate_plus_set = set()

    for idx, row in merged.iterrows():
        result = classify_outside_range(
            atlas_area=float(row["atlas_area"]),
            min_area=float(row["min_area_km2"]),
            max_area=float(row["max_area_km2"]),
        )
        if result["is_outside_range"]:
            outside_set.add(idx)

        level = agreement[idx]
        if level in ("poor", "extreme"):
            poor_plus_set.add(idx)
        if level in ("moderate", "poor", "extreme"):
            moderate_plus_set.add(idx)

    total = len(merged)
    a = outside_set
    b_poor = poor_plus_set
    b_mod = moderate_plus_set

    print(f"\n{'='*60}")
    print(f"Outside range vs Agreement overlap (n={total})")
    print(f"{'='*60}")
    print(f"  A = outside_range:           {len(a):>6} ({len(a)/total*100:5.1f}%)")
    print(f"  B_poor   = poor+extreme:     {len(b_poor):>6} ({len(b_poor)/total*100:5.1f}%)")
    print(f"  B_mod    = moderate+:        {len(b_mod):>6} ({len(b_mod)/total*100:5.1f}%)")
    print(f"{'-'*60}")
    print(f"  A ∩ B_poor:                  {len(a & b_poor):>6}")
    print(f"  A ∩ B_mod:                   {len(a & b_mod):>6}")
    print(f"  A \\ B_poor (only outside):   {len(a - b_poor):>6}")
    print(f"  A \\ B_mod  (only outside):   {len(a - b_mod):>6}")
    print(f"  B_poor \\ A (only ratio):     {len(b_poor - a):>6}")
    print(f"  B_mod  \\ A (only ratio):     {len(b_mod - a):>6}")
    print(f"{'-'*60}")
    if len(a) > 0:
        print(f"  Recall (A∩B_poor / A):       {len(a & b_poor)/len(a)*100:5.1f}%")
        print(f"  Recall (A∩B_mod  / A):       {len(a & b_mod)/len(a)*100:5.1f}%")
    if len(b_poor) > 0:
        print(f"  Precision (A∩B_poor / B):    {len(a & b_poor)/len(b_poor)*100:5.1f}%")
    if len(b_mod) > 0:
        print(f"  Precision (A∩B_mod  / B):    {len(a & b_mod)/len(b_mod)*100:5.1f}%")
    if len(a | b_poor) > 0:
        print(f"  Jaccard (A, B_poor):         {len(a & b_poor)/len(a | b_poor)*100:5.1f}%")
    if len(a | b_mod) > 0:
        print(f"  Jaccard (A, B_mod):          {len(a & b_mod)/len(a | b_mod)*100:5.1f}%")
    print(f"{'='*60}")
