"""Generate a production continent manifest from ``data/mainland.json``.

Outputs under ``data/continents`` by default:

* ``regions/<tile_id>_region.geojson``
* ``lakes/<tile_id>_lakes.geojson``
* ``continents_manifest.json``
* ``summary.json``

Lake geometries are sourced from the databases via the same env bridge used by
the smoke generator:

* ``HYDROFETCH_DB_*`` -> ``SERIES_DB`` / ``DB_*``
* ``ALTAS_DB`` for polygon geometry reads
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from dotenv import load_dotenv  # pylint: disable=import-error
from shapely import wkt
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry


def _parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repo_root / "packages" / "hydrofetch" / ".env",
        help="hydrofetch env file used for DB access",
    )
    parser.add_argument(
        "--mainland-json",
        type=Path,
        default=repo_root / "data" / "mainland.json",
        help="Input mainland region definition JSON",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repo_root / "data" / "continents",
        help="Output directory for manifest and split GeoJSON files",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="How many hylak_id values to fetch from area_quality per chunk",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit of lake ids for testing (0 = full set)",
    )
    parser.add_argument(
        "--buffer-deg",
        type=float,
        default=8.0,
        help="Expand each mainland region by this many degree units for fallback assignment/output.",
    )
    return parser.parse_args()


def _require_env(key: str) -> str:
    value = (os.environ.get(key) or "").strip()
    if not value:
        raise ValueError(f"Required environment variable {key!r} is not set.")
    return value


def _bridge_hydrofetch_env() -> None:
    os.environ["SERIES_DB"] = _require_env("HYDROFETCH_DB")
    os.environ["DB_USER"] = _require_env("HYDROFETCH_DB_USER")
    os.environ["DB_PASSWORD"] = _require_env("HYDROFETCH_DB_PASSWORD")
    os.environ["DB_HOST"] = (os.environ.get("HYDROFETCH_DB_HOST") or "localhost").strip() or "localhost"
    os.environ["DB_PORT"] = (os.environ.get("HYDROFETCH_DB_PORT") or "5432").strip() or "5432"
    _require_env("ALTAS_DB")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_geojson(path: Path, payload: dict[str, Any]) -> None:
    _write_json(path, payload)


def _load_mainland_regions(mainland_json: Path) -> list[dict[str, Any]]:
    payload = _read_json(mainland_json)
    regions = payload.get("regions") or []
    if not regions:
        raise ValueError(f"No 'regions' list found in {mainland_json}")
    return regions


def _iter_area_quality_hylak_ids(
    *,
    chunk_size: int,
    limit: int,
) -> list[int]:
    from lakeanalysis.dbconnect.client import series_db  # pylint: disable=import-error
    from lakeanalysis.dbconnect.lake import fetch_area_quality_hylak_ids  # pylint: disable=import-error

    all_ids: list[int] = []
    offset = 0
    remaining = limit if limit > 0 else None

    with series_db.connection_context() as conn:
        while True:
            batch_limit = chunk_size if remaining is None else min(chunk_size, remaining)
            if batch_limit <= 0:
                break
            batch = fetch_area_quality_hylak_ids(conn, limit=batch_limit, offset=offset)
            if not batch:
                break
            all_ids.extend(batch)
            offset += len(batch)
            if remaining is not None:
                remaining -= len(batch)
    return all_ids


def _fetch_geometry_chunk(hylak_ids: list[int]) -> list[tuple[int, BaseGeometry]]:
    from lakeanalysis.dbconnect.client import atlas_db  # pylint: disable=import-error
    from lakeanalysis.dbconnect.lake import fetch_lake_geometry_wkt_by_ids  # pylint: disable=import-error

    with atlas_db.connection_context() as conn:
        geometry_df = fetch_lake_geometry_wkt_by_ids(conn, hylak_ids)

    if geometry_df.empty:
        return []

    geometries: list[tuple[int, BaseGeometry]] = []
    for _, row in geometry_df.iterrows():
        raw_wkt = row.get("wkt")
        if not raw_wkt:
            continue
        geometries.append((int(row["hylak_id"]), wkt.loads(str(raw_wkt))))
    return geometries


def _feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def _lake_feature_collection(lakes: list[tuple[int, BaseGeometry]]) -> dict[str, Any]:
    return _feature_collection(
        [
            {
                "type": "Feature",
                "properties": {"hylak_id": hylak_id},
                "geometry": mapping(geom),
            }
            for hylak_id, geom in lakes
        ]
    )


def _buffered_region_feature(region: dict[str, Any], buffer_deg: float) -> dict[str, Any]:
    if buffer_deg <= 0:
        return region["region_geojson"]

    feature = dict(region["region_geojson"])
    feature["geometry"] = mapping(shape(region["region_geojson"]["geometry"]).buffer(buffer_deg))
    return feature


def _region_shapes(
    regions: list[dict[str, Any]],
    *,
    buffer_deg: float,
) -> list[tuple[str, BaseGeometry]]:
    return [
        (
            str(region["tile_id"]),
            shape(_buffered_region_feature(region, buffer_deg)["geometry"]),
        )
        for region in regions
    ]


def _assign_tile_id(geom: BaseGeometry, region_shapes: list[tuple[str, BaseGeometry]]) -> str | None:
    point = geom.representative_point()
    for tile_id, geom_obj in region_shapes:
        if geom_obj.covers(point):
            return tile_id
    return None


def _manifest_entries(
    *,
    manifest_path: Path,
    tile_ids: list[str],
    lakes_dir: Path,
    regions_dir: Path,
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for tile_id in tile_ids:
        entries.append(
            {
                "tile_id": tile_id,
                "region_path": os.path.relpath(regions_dir / f"{tile_id}_region.geojson", manifest_path.parent),
                "geometry_path": os.path.relpath(lakes_dir / f"{tile_id}_lakes.geojson", manifest_path.parent),
            }
        )
    return entries


def main() -> None:
    args = _parse_args()
    load_dotenv(args.env_file)
    _bridge_hydrofetch_env()

    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be >= 1")

    output_dir = args.output_dir.resolve()
    regions_dir = output_dir / "regions"
    lakes_dir = output_dir / "lakes"
    manifest_path = output_dir / "continents_manifest.json"
    summary_path = output_dir / "summary.json"
    unassigned_path = output_dir / "unassigned_hylak_ids.json"

    regions = _load_mainland_regions(args.mainland_json.resolve())
    regions_by_id = {str(region["tile_id"]): region for region in regions}
    region_shapes = _region_shapes(regions, buffer_deg=0.0)
    buffered_region_shapes = _region_shapes(regions, buffer_deg=args.buffer_deg)

    # Always write region files, optionally expanded by a configurable buffer.
    for tile_id, region in regions_by_id.items():
        _write_geojson(
            regions_dir / f"{tile_id}_region.geojson",
            _buffered_region_feature(region, args.buffer_deg),
        )

    hylak_ids = _iter_area_quality_hylak_ids(chunk_size=args.chunk_size, limit=args.limit)
    if not hylak_ids:
        raise ValueError("No hylak_id values available from area_quality.")

    grouped: dict[str, list[tuple[int, BaseGeometry]]] = defaultdict(list)
    unassigned: list[int] = []
    rescued_by_buffer = 0

    for start in range(0, len(hylak_ids), args.chunk_size):
        batch_ids = hylak_ids[start:start + args.chunk_size]
        for hylak_id, geom in _fetch_geometry_chunk(batch_ids):
            tile_id = _assign_tile_id(geom, region_shapes)
            if tile_id is None and args.buffer_deg > 0:
                tile_id = _assign_tile_id(geom, buffered_region_shapes)
                if tile_id is not None:
                    rescued_by_buffer += 1
            if tile_id is None:
                unassigned.append(hylak_id)
                continue
            grouped[tile_id].append((hylak_id, geom))

    # Only include tiles that actually have lakes.
    active_tile_ids = [tile_id for tile_id in regions_by_id if grouped.get(tile_id)]
    if not active_tile_ids:
        raise ValueError("No lakes were assigned to any mainland region.")

    for tile_id in active_tile_ids:
        _write_geojson(lakes_dir / f"{tile_id}_lakes.geojson", _lake_feature_collection(grouped[tile_id]))

    manifest = {
        "tiles": _manifest_entries(
            manifest_path=manifest_path,
            tile_ids=active_tile_ids,
            lakes_dir=lakes_dir,
            regions_dir=regions_dir,
        )
    }
    _write_json(manifest_path, manifest)
    _write_json(unassigned_path, {"hylak_ids": unassigned})

    summary = {
        "input_mainland_json": str(args.mainland_json.resolve()),
        "output_dir": str(output_dir),
        "buffer_deg": args.buffer_deg,
        "buffer_strategy": "prefer_unbuffered_then_buffered_fallback",
        "total_hylak_ids": len(hylak_ids),
        "assigned_hylak_ids": sum(len(grouped[tile_id]) for tile_id in active_tile_ids),
        "unassigned_hylak_ids": len(unassigned),
        "buffer_rescued_hylak_ids": rescued_by_buffer,
        "active_tiles": {tile_id: len(grouped[tile_id]) for tile_id in active_tile_ids},
        "unassigned_output": str(unassigned_path),
        "unassigned_example_ids": unassigned[:50],
    }
    _write_json(summary_path, summary)

    print("Generated production continent assets:")
    print(f"- regions: {regions_dir}")
    print(f"- lakes: {lakes_dir}")
    print(f"- manifest: {manifest_path}")
    print(f"- summary: {summary_path}")
    print(f"- active tiles: {', '.join(active_tile_ids)}")
    print(f"- assigned lakes: {summary['assigned_hylak_ids']} / {summary['total_hylak_ids']}")


if __name__ == "__main__":
    main()
