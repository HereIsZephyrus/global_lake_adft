"""Quick debug script: run zonal sampling on ONE tile/date to trigger the error
and collect debug instrumentation logs.

Usage:
    uv run --package hydrofetch python packages/hydrofetch/scripts/debug_sample_one.py
"""
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

REPO_ROOT = Path(__file__).parent.parent.parent.parent

# Use the first available raw TIF and its matching lake geojson
RAW_DIR = REPO_ROOT / "data" / "hydrofetch_full_file_db_raw"
LAKES_DIR = REPO_ROOT / "data" / "continents" / "lakes"

tiles = ["north_america", "oceania", "south_america", "africa", "asia", "europe"]

for tile in tiles:
    tif = RAW_DIR / f"era5_land_daily_image_20010101_{tile}.tif"
    lakes = LAKES_DIR / f"{tile}_lakes.geojson"
    if tif.exists() and lakes.exists():
        print(f"\n=== Sampling {tile} ===")
        from hydrofetch.sample.raster import sample_raster_by_polygons_weighted
        try:
            df = sample_raster_by_polygons_weighted(
                raster_path=tif,
                geometry_path=lakes,
                id_column="hylak_id",
                date_iso="2001-01-01",
            )
            print(f"  OK: {len(df)} rows, NaN count: {df.isna().any(axis=1).sum()}")
        except Exception as e:
            print(f"  ERROR: {e}")
    else:
        print(f"  SKIP {tile}: TIF={tif.exists()} lakes={lakes.exists()}")

print("\nDone. Check debug log at .cursor/debug-6b34d8.log")
