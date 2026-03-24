"""Hydrofetch command-line interface.

Usage examples::

    hydrofetch era5 \\
        --start 2020-01-01 --end 2020-01-08 \\
        --region region.geojson \\
        --geometry lakes_centroids.csv \\
        --output-dir ./results

    hydrofetch status
    hydrofetch status --verbose
    hydrofetch retry --job-id era5_land_daily_image_20200103
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Top-level parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hydrofetch",
        description=(
            "GEE image export, Drive download, local raster sampling, "
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
    p.add_argument(
        "--region",
        required=True,
        metavar="FILE",
        help="GeoJSON file defining the area of interest.",
    )
    p.add_argument(
        "--geometry",
        required=True,
        metavar="FILE",
        help="CSV or GeoJSON file with lake centroids (columns: hylak_id, lon, lat).",
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
    from hydrofetch.export.namer import image_day_prefix, iter_daily_date_range  # pylint: disable=import-outside-toplevel
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

    region_path = Path(args.region)
    if not region_path.is_file():
        print(f"Error: region file not found: {region_path}", file=sys.stderr)
        sys.exit(1)
    with region_path.open(encoding="utf-8") as fh:
        region_geojson = json.load(fh)

    job_dir = Path(args.job_dir) if args.job_dir else get_job_dir()
    store = JobStore(job_dir)
    drive_folder = get_drive_folder_name()

    dates = list(iter_daily_date_range(args.start, args.end))
    if not dates:
        print("No dates in range; nothing to enqueue.", file=sys.stderr)
        sys.exit(0)

    enqueued = 0
    skipped = 0
    for d in dates:
        export_name = image_day_prefix(spec.spec_id, d)
        job_id = export_name

        if store.is_completed(job_id):
            log.debug("Skipping completed job %s", job_id)
            skipped += 1
            continue

        if args.dry_run:
            print(f"[dry-run] would enqueue: {job_id}")
            enqueued += 1
            continue

        if store.exists(job_id):
            existing = store.load(job_id)
            if existing and not existing.state.is_terminal:
                log.debug("Job %s already active (state=%s), skipping", job_id, existing.state)
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
            ),
            sample=SampleParams(
                geometry_path=str(Path(args.geometry).resolve()),
                id_column=args.id_column,
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
    import dataclasses  # pylint: disable=import-outside-toplevel

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
    reset = dataclasses.replace(
        record,
        state=JobState.HOLD,
        last_error=None,
        task_id=None,
        drive_file_id=None,
    )
    store.save(reset)
    print(f"Reset job {args.job_id} to HOLD.")


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
