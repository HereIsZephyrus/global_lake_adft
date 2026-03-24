#!/usr/bin/env python3
"""Verify Google Earth Engine and Google Drive API connectivity for hydrofetch.

Run from the repository root::

    uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py

Use a specific env file::

    uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py --env-file /path/to/.env

Requires ``HYDROFETCH_GEE_PROJECT``, ``HYDROFETCH_CREDENTIALS_FILE``, and prior
``earthengine authenticate`` for GEE. Drive may open a browser on first run.
"""

from __future__ import annotations

# Imports are deferred until after ``load_env`` / argument parsing.
# pylint: disable=import-error,import-outside-toplevel

import argparse
import sys


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", maxsplit=1)[0])
    p.add_argument(
        "--env-file",
        metavar="FILE",
        help="Path to .env (default: dotenv default lookup from cwd).",
    )
    p.add_argument(
        "--skip-gee",
        action="store_true",
        help="Only test Google Drive.",
    )
    p.add_argument(
        "--skip-drive",
        action="store_true",
        help="Only test Earth Engine.",
    )
    return p.parse_args()


def _test_gee() -> None:
    from hydrofetch.gee.client import init_earth_engine

    import ee

    init_earth_engine()
    value = ee.Number(1).getInfo()
    if value != 1:
        raise RuntimeError(f"unexpected ee.Number(1).getInfo() = {value!r}")


def _test_drive() -> None:
    from hydrofetch.drive.client import DriveClient

    drive = DriveClient.from_config()
    drive.find_files_by_name_prefix("__hydrofetch_connectivity_probe__")


def main() -> int:
    """Run connectivity checks; return 0 if all attempted checks pass."""
    args = _parse_args()
    if args.skip_gee and args.skip_drive:
        print("Error: cannot use --skip-gee and --skip-drive together.", file=sys.stderr)
        return 2

    from hydrofetch.config import load_env  # pylint: disable=import-outside-toplevel

    load_env(args.env_file)

    ok = True
    if not args.skip_gee:
        try:
            _test_gee()
            print("Earth Engine: OK")
        except Exception as exc:  # pylint: disable=broad-except
            ok = False
            print(f"Earth Engine: FAILED — {exc}", file=sys.stderr)

    if not args.skip_drive:
        try:
            _test_drive()
            print("Google Drive: OK")
        except Exception as exc:  # pylint: disable=broad-except
            ok = False
            print(f"Google Drive: FAILED — {exc}", file=sys.stderr)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
