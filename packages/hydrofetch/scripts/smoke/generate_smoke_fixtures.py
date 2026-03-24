"""Generate smoke-test fixtures from one source of truth: ``hylak_id`` + geom.

This script bridges hydrofetch's ``HYDROFETCH_DB_*`` configuration to the
``lakeanalysis`` helpers that expect ``SERIES_DB`` / ``ALTAS_DB`` + ``DB_*``.
The smoke lake ids come from ``area_quality`` in the hydrofetch database, while
the polygon geometries are read from ``LakeATLAS_v10_pol`` in ``ALTAS_DB``.

From that single lake-geometry set, the script writes:

* ``smoke_lakes_polygons.geojson`` – source-of-truth lake polygons
* ``smoke_region.geojson`` – derived export region (buffered union bbox)
* ``smoke_manifest.json`` – manifest referencing the two derived artifacts
"""

from __future__ import annotations

import argparse
import json
import os.path
import os
from pathlib import Path

from dotenv import load_dotenv
from shapely import wkt
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


def _parse_args() -> argparse.Namespace:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        help="Optional .env file to load before resolving database settings.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of lakes to fetch from area_quality (default: 10).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset into area_quality for repeatable fixture slices (default: 0).",
    )
    parser.add_argument(
        "--buffer-deg",
        type=float,
        default=0.05,
        help="Padding added around the union bbox when writing smoke_region.geojson.",
    )
    parser.add_argument(
        "--geometry-output",
        default=fixtures_dir / "smoke_lakes_polygons.geojson",
        type=Path,
        help="Output path for the smoke lake polygons GeoJSON.",
    )
    parser.add_argument(
        "--region-output",
        default=fixtures_dir / "smoke_region.geojson",
        type=Path,
        help="Output path for the buffered smoke bounding box GeoJSON.",
    )
    parser.add_argument(
        "--manifest-output",
        default=fixtures_dir / "smoke_manifest.json",
        type=Path,
        help="Output path for the derived smoke tile manifest JSON.",
    )
    parser.add_argument(
        "--tile-id",
        default="smoke",
        help="Tile id written into the generated manifest (default: smoke).",
    )
    return parser.parse_args()


def _require_env(key: str) -> str:
    value = (os.environ.get(key) or "").strip()
    if not value:
        raise ValueError(f"Required environment variable {key!r} is not set.")
    return value


def _bridge_hydrofetch_env() -> None:
    """Map hydrofetch DB settings onto the env contract used by lakeanalysis."""
    os.environ["SERIES_DB"] = _require_env("HYDROFETCH_DB")
    os.environ["DB_USER"] = _require_env("HYDROFETCH_DB_USER")
    os.environ["DB_PASSWORD"] = _require_env("HYDROFETCH_DB_PASSWORD")
    os.environ["DB_HOST"] = (os.environ.get("HYDROFETCH_DB_HOST") or "localhost").strip() or "localhost"
    os.environ["DB_PORT"] = (os.environ.get("HYDROFETCH_DB_PORT") or "5432").strip() or "5432"
    _require_env("ALTAS_DB")


def _load_lake_geometries(limit: int, offset: int) -> list[tuple[int, BaseGeometry]]:
    from lakeanalysis.dbconnect.client import atlas_db, series_db
    from lakeanalysis.dbconnect.lake import (
        fetch_area_quality_hylak_ids,
        fetch_lake_geometry_wkt_by_ids,
    )

    with series_db.connection_context() as series_conn:
        hylak_ids = fetch_area_quality_hylak_ids(series_conn, limit=limit, offset=offset)
    if not hylak_ids:
        raise ValueError("No hylak_id rows returned from area_quality.")

    with atlas_db.connection_context() as atlas_conn:
        geometry_df = fetch_lake_geometry_wkt_by_ids(atlas_conn, hylak_ids)
    if geometry_df.empty:
        raise ValueError("No lake geometries returned from ALTAS_DB.")

    wkt_by_id = {
        int(row["hylak_id"]): wkt.loads(str(row["wkt"]))
        for _, row in geometry_df.iterrows()
        if row["wkt"]
    }
    missing_ids = [hylak_id for hylak_id in hylak_ids if hylak_id not in wkt_by_id]
    if missing_ids:
        raise ValueError(f"Missing geometries for hylak_id values: {missing_ids}")

    return [(hylak_id, wkt_by_id[hylak_id]) for hylak_id in hylak_ids]


def _feature_collection(features: list[dict]) -> dict:
    return {"type": "FeatureCollection", "features": features}


def _lake_feature_collection(lake_geometries: list[tuple[int, BaseGeometry]]) -> dict:
    features = [
        {
            "type": "Feature",
            "properties": {"hylak_id": hylak_id},
            "geometry": mapping(geom),
        }
        for hylak_id, geom in lake_geometries
    ]
    return _feature_collection(features)


def _derived_region_feature(
    lake_geometries: list[tuple[int, BaseGeometry]],
    *,
    buffer_deg: float,
    tile_id: str,
) -> dict:
    union_geom = unary_union([geom for _, geom in lake_geometries])
    if union_geom.is_empty:
        raise ValueError("Smoke geometry union is empty.")

    minx, miny, maxx, maxy = union_geom.bounds
    return {
        "type": "Feature",
        "properties": {"tile_id": tile_id},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [minx - buffer_deg, miny - buffer_deg],
                [maxx + buffer_deg, miny - buffer_deg],
                [maxx + buffer_deg, maxy + buffer_deg],
                [minx - buffer_deg, maxy + buffer_deg],
                [minx - buffer_deg, miny - buffer_deg],
            ]],
        },
    }


def _manifest_ref(manifest_path: Path, target_path: Path) -> str:
    return os.path.relpath(target_path, manifest_path.parent)


def _tile_manifest(
    *,
    manifest_path: Path,
    geometry_path: Path,
    region_path: Path,
    tile_id: str,
) -> dict:
    return {
        "tiles": [
            {
                "tile_id": tile_id,
                "region_path": _manifest_ref(manifest_path, region_path),
                "geometry_path": _manifest_ref(manifest_path, geometry_path),
            }
        ]
    }


def _write_geojson(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    args = _parse_args()
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()
    _bridge_hydrofetch_env()

    geometry_output = args.geometry_output.resolve()
    region_output = args.region_output.resolve()
    manifest_output = args.manifest_output.resolve()

    lake_geometries = _load_lake_geometries(limit=args.limit, offset=args.offset)
    lake_payload = _lake_feature_collection(lake_geometries)
    region_payload = _derived_region_feature(
        lake_geometries,
        buffer_deg=args.buffer_deg,
        tile_id=args.tile_id,
    )
    manifest_payload = _tile_manifest(
        manifest_path=manifest_output,
        geometry_path=geometry_output,
        region_path=region_output,
        tile_id=args.tile_id,
    )

    _write_geojson(geometry_output, lake_payload)
    _write_geojson(region_output, region_payload)
    _write_json(manifest_output, manifest_payload)

    print(
        "Generated smoke fixtures:",
        f"{len(lake_geometries)} lakes -> {geometry_output}",
        f"region -> {region_output}",
        f"manifest -> {manifest_output}",
        sep="\n",
    )


if __name__ == "__main__":
    main()
