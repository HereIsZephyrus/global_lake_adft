"""Hydrofetch command-line interface.

Usage examples::

    # Tile-manifest mode (recommended): one job per date × tile
    hydrofetch era5 \\
        --start 2020-01-01 --end 2020-01-08 \\
        --tile-manifest tiles.json \\
        --run

    # Legacy single-tile mode
    hydrofetch era5 \\
        --start 2020-01-01 --end 2020-01-08 \\
        --geometry lakes.geojson \\
        --run

    hydrofetch status
    hydrofetch status --verbose
    hydrofetch retry --job-id era5_land_daily_image_20200103_europe
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Top-level parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hydrofetch",
        description=(
            "GEE image export, Drive download, local raster zonal sampling, "
            "and lake forcing pipeline."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    parser.add_argument(
        "--job-dir",
        metavar="DIR",
        help="Override HYDROFETCH_JOB_DIR environment variable.",
    )
    parser.add_argument(
        "--env-file",
        metavar="FILE",
        help="Path to a .env file (default: .env in current directory).",
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    _add_era5_parser(sub)
    _add_status_parser(sub)
    _add_retry_parser(sub)

    return parser


# ---------------------------------------------------------------------------
# `era5` sub-command
# ---------------------------------------------------------------------------


def _add_era5_parser(sub: argparse._SubParsersAction) -> None:  # pylint: disable=protected-access
    p = sub.add_parser("era5", help="Enqueue ERA5-Land daily image export jobs.")
    p.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="First date (inclusive).")
    p.add_argument(
        "--end",
        required=True,
        metavar="YYYY-MM-DD",
        help="Last date (exclusive).",
    )

    # Mutually exclusive: tile-manifest (primary) vs. legacy single-tile geometry
    input_grp = p.add_mutually_exclusive_group(required=True)
    input_grp.add_argument(
        "--tile-manifest",
        metavar="FILE",
        help=(
            "JSON manifest describing spatial tiles.  Each tile entry must have "
            "``tile_id`` and ``geometry_path``; ``region_path`` is optional "
            "(omit to export the full ERA5-Land global footprint for that tile).  "
            "Relative paths are resolved relative to the manifest file."
        ),
    )
    input_grp.add_argument(
        "--geometry",
        metavar="FILE",
        help=(
            "GeoJSON file with lake Polygon / MultiPolygon features "
            "(property ``hylak_id`` required).  Legacy single-tile mode."
        ),
    )

    p.add_argument(
        "--region",
        metavar="FILE",
        help=(
            "GeoJSON file defining the area of interest.  "
            "Only used with ``--geometry`` (single-tile mode).  "
            "Omit to export the full ERA5-Land global footprint."
        ),
    )
    p.add_argument(
        "--output-dir",
        default="",
        metavar="DIR",
        help=(
            "Directory where sampled output files are written. "
            "Required when --sink includes 'file' (the default)."
        ),
    )
    p.add_argument(
        "--id-column",
        default="hylak_id",
        metavar="COL",
        help="Lake identifier column in the geometry file (default: hylak_id).",
    )
    p.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output file format for the file sink (default: parquet).",
    )
    p.add_argument(
        "--sink",
        nargs="+",
        choices=["file", "db"],
        default=["file"],
        metavar="SINK",
        help=(
            "One or more output sinks: 'file' (default), 'db', or both. "
            "Example: --sink file db"
        ),
    )
    p.add_argument(
        "--db-table",
        default="era5_forcing",
        metavar="TABLE",
        help="PostgreSQL table name for the db sink (default: era5_forcing).",
    )
    p.add_argument(
        "--catalog",
        metavar="FILE",
        help="Override the bundled ERA5 catalog JSON.",
    )
    p.add_argument(
        "--run",
        action="store_true",
        help="After enqueueing, immediately start the monitor loop.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be enqueued without creating job files.",
    )
    p.add_argument(
        "--retry-failed",
        action="store_true",
        help="Reset all FAILED jobs in the job dir to HOLD before enqueueing/running.",
    )


# ---------------------------------------------------------------------------
# `status` sub-command
# ---------------------------------------------------------------------------


def _add_status_parser(sub: argparse._SubParsersAction) -> None:  # pylint: disable=protected-access
    p = sub.add_parser("status", help="Show current job states.")
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print one line per job (default: summary only).",
    )


# ---------------------------------------------------------------------------
# `retry` sub-command
# ---------------------------------------------------------------------------


def _add_retry_parser(sub: argparse._SubParsersAction) -> None:  # pylint: disable=protected-access
    p = sub.add_parser("retry", help="Reset a failed job to HOLD so it can be re-tried.")
    p.add_argument("--job-id", required=True, metavar="ID", help="Job ID to reset.")


# ---------------------------------------------------------------------------
# Tile loading helpers
# ---------------------------------------------------------------------------


def _load_tiles(args: argparse.Namespace) -> list[dict]:
    """Return a list of tile dicts from ``--tile-manifest`` or legacy ``--geometry``.

    Each returned dict has the keys:
        ``tile_id``       – identifier string (empty string for legacy mode)
        ``region_geojson`` – parsed GeoJSON dict or ``None``
        ``geometry_path`` – absolute path string to lake polygon GeoJSON
    """
    if args.tile_manifest:
        return _load_manifest_tiles(Path(args.tile_manifest))
    return _load_legacy_tile(args)


def _load_manifest_tiles(manifest_path: Path) -> list[dict]:
    """Parse a tile-manifest JSON file and return resolved tile dicts."""
    if not manifest_path.is_file():
        print(f"Error: tile-manifest file not found: {manifest_path}", file=sys.stderr)
        sys.exit(1)

    with manifest_path.open(encoding="utf-8") as fh:
        manifest = json.load(fh)

    tiles_raw = manifest.get("tiles")
    if not tiles_raw:
        print("Error: tile-manifest has no 'tiles' list.", file=sys.stderr)
        sys.exit(1)

    result: list[dict] = []
    for entry in tiles_raw:
        tile_id = entry.get("tile_id", "").strip()
        if not tile_id:
            print("Error: each tile entry must have a non-empty 'tile_id'.", file=sys.stderr)
            sys.exit(1)

        # Resolve geometry path (relative to manifest dir)
        raw_gp = entry.get("geometry_path", "").strip()
        if not raw_gp:
            print(
                f"Error: tile '{tile_id}' has no 'geometry_path'.", file=sys.stderr
            )
            sys.exit(1)
        gp = Path(raw_gp)
        if not gp.is_absolute():
            gp = manifest_path.parent / gp
        gp = gp.resolve()
        if not gp.is_file():
            print(f"Error: geometry_path not found for tile '{tile_id}': {gp}", file=sys.stderr)
            sys.exit(1)

        # Resolve optional region path
        region_geojson = None
        raw_rp = entry.get("region_path", "").strip()
        if raw_rp:
            rp = Path(raw_rp)
            if not rp.is_absolute():
                rp = manifest_path.parent / rp
            rp = rp.resolve()
            if not rp.is_file():
                print(
                    f"Error: region_path not found for tile '{tile_id}': {rp}", file=sys.stderr
                )
                sys.exit(1)
            with rp.open(encoding="utf-8") as fh:
                region_geojson = json.load(fh)

        result.append(
            {
                "tile_id": tile_id,
                "region_geojson": region_geojson,
                "geometry_path": str(gp),
            }
        )

    return result


def _load_legacy_tile(args: argparse.Namespace) -> list[dict]:
    """Construct a single-tile dict from legacy ``--geometry`` / ``--region`` args."""
    gp = Path(args.geometry).resolve()
    if not gp.is_file():
        print(f"Error: geometry file not found: {gp}", file=sys.stderr)
        sys.exit(1)

    region_geojson = None
    if args.region:
        rp = Path(args.region)
        if not rp.is_file():
            print(f"Error: region file not found: {rp}", file=sys.stderr)
            sys.exit(1)
        with rp.open(encoding="utf-8") as fh:
            region_geojson = json.load(fh)

    return [
        {
            "tile_id": "",
            "region_geojson": region_geojson,
            "geometry_path": str(gp),
        }
    ]


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def _cmd_era5(args: argparse.Namespace) -> None:
    from hydrofetch.catalog.parser import bundled_spec_path, load_image_spec  # pylint: disable=import-outside-toplevel
    from hydrofetch.config import (  # pylint: disable=import-outside-toplevel
        get_drive_folder_name,
        get_job_dir,
        get_max_concurrent,
        get_poll_interval,
        get_raw_dir,
        get_sample_dir,
    )
    from hydrofetch.export.namer import (  # pylint: disable=import-outside-toplevel
        image_day_prefix,
        image_day_tile_prefix,
        iter_daily_date_range,
    )
    from hydrofetch.gee.client import init_earth_engine  # pylint: disable=import-outside-toplevel
    from hydrofetch.jobs.models import (  # pylint: disable=import-outside-toplevel
        GeeExportParams,
        JobRecord,
        JobSpec,
        SampleParams,
        WriteParams,
    )
    from hydrofetch.jobs.store import JobStore  # pylint: disable=import-outside-toplevel

    catalog_path = Path(args.catalog) if args.catalog else bundled_spec_path()
    spec = load_image_spec(catalog_path)

    # Validate sink / output-dir combination.
    sinks: list[str] = list(args.sink)
    if "file" in sinks and not args.output_dir:
        print(
            "Error: --output-dir is required when using the 'file' sink.",
            file=sys.stderr,
        )
        sys.exit(1)

    tiles = _load_tiles(args)

    job_dir = Path(args.job_dir) if args.job_dir else get_job_dir()
    store = JobStore(job_dir)
    drive_folder = get_drive_folder_name()

    reset_count = 0
    if args.retry_failed and not args.dry_run:
        reset_count = _reset_failed_jobs(store)

    dates = list(iter_daily_date_range(args.start, args.end))
    if not dates:
        print("No dates in range; nothing to enqueue.", file=sys.stderr)
        sys.exit(0)

    enqueued = 0
    skipped = 0
    for d in dates:
        for tile in tiles:
            tile_id: str = tile["tile_id"]
            region_geojson = tile["region_geojson"]
            geometry_path: str = tile["geometry_path"]

            # Job id / export name includes tile_id when in manifest mode.
            if tile_id:
                export_name = image_day_tile_prefix(spec.spec_id, d, tile_id)
            else:
                export_name = image_day_prefix(spec.spec_id, d)
            job_id = export_name

            if store.is_completed(job_id):
                log.debug("Skipping completed job %s", job_id)
                skipped += 1
                continue

            if args.dry_run:
                tile_label = f" [{tile_id}]" if tile_id else ""
                print(f"[dry-run] would enqueue: {job_id}{tile_label}")
                enqueued += 1
                continue

            if store.exists(job_id):
                existing = store.load(job_id)
                if existing and not existing.state.is_terminal:
                    log.debug(
                        "Job %s already active (state=%s), skipping",
                        job_id,
                        existing.state,
                    )
                    skipped += 1
                    continue

            job_spec = JobSpec(
                job_id=job_id,
                export_name=export_name,
                date_iso=d.isoformat(),
                gee=GeeExportParams(
                    spec_id=spec.spec_id,
                    asset_id=spec.asset_id,
                    bands=spec.band_names(),
                    scale=spec.native_scale_m,
                    crs=spec.crs,
                    max_pixels=spec.max_pixels,
                    region_geojson=region_geojson,
                    drive_folder=drive_folder,
                    tile_id=tile_id,
                ),
                sample=SampleParams(
                    geometry_path=geometry_path,
                    id_column=args.id_column,
                    tile_id=tile_id,
                ),
                write=WriteParams(
                    output_dir=str(Path(args.output_dir).resolve()) if args.output_dir else "",
                    output_format=args.output_format,
                    sinks=sinks,
                    db_table=args.db_table,
                ),
            )
            record = JobRecord(spec=job_spec)
            store.save(record)
            enqueued += 1

    print(f"Enqueued {enqueued} job(s), skipped {skipped}.")
    if args.retry_failed and not args.dry_run:
        print(f"Reset {reset_count} failed job(s) to HOLD.")

    if args.run and not args.dry_run:
        init_earth_engine()
        from hydrofetch.drive.client import DriveClient  # pylint: disable=import-outside-toplevel
        from hydrofetch.monitor.runner import JobRunner  # pylint: disable=import-outside-toplevel

        drive = DriveClient.from_config()
        runner = JobRunner.from_config(
            job_dir=job_dir,
            raw_dir=get_raw_dir(),
            sample_dir=get_sample_dir(),
            drive=drive,
            max_concurrent=get_max_concurrent(),
            poll_interval=get_poll_interval(),
        )
        runner.run_until_done()


def _cmd_status(args: argparse.Namespace) -> None:
    from hydrofetch.config import get_job_dir  # pylint: disable=import-outside-toplevel
    from hydrofetch.jobs.store import JobStore  # pylint: disable=import-outside-toplevel

    job_dir = Path(args.job_dir) if args.job_dir else get_job_dir()
    store = JobStore(job_dir)
    verbose = getattr(args, "verbose", False)
    if verbose:
        store.print_all()
    else:
        store.print_summary()


def _cmd_retry(args: argparse.Namespace) -> None:
    from hydrofetch.config import get_job_dir  # pylint: disable=import-outside-toplevel
    from hydrofetch.jobs.models import JobState  # pylint: disable=import-outside-toplevel
    from hydrofetch.jobs.store import JobStore  # pylint: disable=import-outside-toplevel

    job_dir = Path(args.job_dir) if args.job_dir else get_job_dir()
    store = JobStore(job_dir)
    record = store.load(args.job_id)
    if record is None:
        print(f"Job not found: {args.job_id}", file=sys.stderr)
        sys.exit(1)
    if record.state != JobState.FAILED:
        print(
            f"Job {args.job_id} is in state {record.state.value}, not FAILED. Nothing to do."
        )
        return
    reset = _reset_failed_record(record)
    store.save(reset)
    print(f"Reset job {args.job_id} to HOLD.")


def _reset_failed_record(record):
    from hydrofetch.jobs.models import JobState  # pylint: disable=import-outside-toplevel

    return dataclasses.replace(
        record,
        state=JobState.HOLD,
        attempt=0,
        last_error=None,
        task_id=None,
        drive_file_id=None,
        updated_at=datetime.now(tz=timezone.utc).isoformat(),
    )


def _reset_failed_jobs(store) -> int:
    from hydrofetch.jobs.models import JobState  # pylint: disable=import-outside-toplevel

    reset_count = 0
    for record in store.load_all():
        if record.state != JobState.FAILED:
            continue
        store.save(_reset_failed_record(record))
        reset_count += 1
    return reset_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Entry point for the ``hydrofetch`` CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    from hydrofetch.config import load_env  # pylint: disable=import-outside-toplevel
    from hydrofetch.utils.logger import setup_logging  # pylint: disable=import-outside-toplevel

    load_env(args.env_file if hasattr(args, "env_file") else None)
    setup_logging(verbose=getattr(args, "verbose", False))

    dispatch = {
        "era5": _cmd_era5,
        "status": _cmd_status,
        "retry": _cmd_retry,
    }
    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)
    handler(args)


if __name__ == "__main__":
    main()
