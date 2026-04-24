"""Prepare warehouse layout and optionally download public water-balance data."""

from __future__ import annotations

import argparse
from pathlib import Path

from lakesource.env import load_env
from lakesource.water_balance import (
    auth_config_lines,
    build_storage_report,
    dataset_plan_lines,
    download_authenticated_datasets,
    download_gleam_monthly_datasets,
    download_public_datasets,
    ensure_dataset_directories,
    load_auth_config,
    storage_report_lines,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Prepare /mnt/warehouse layout for water-balance inputs."
    )
    parser.add_argument(
        "--warehouse-root",
        type=Path,
        default=Path("/mnt/warehouse"),
        help="Warehouse root for raw data storage.",
    )
    parser.add_argument(
        "--download-public",
        action="store_true",
        help="Download datasets that do not require authentication.",
    )
    parser.add_argument(
        "--download-auth",
        action="store_true",
        help="Download datasets that require credentials from the env file.",
    )
    parser.add_argument(
        "--download-gleam-monthly",
        action="store_true",
        help=(
            "Download all GLEAM monthly files (v4.2a) from SFTP subdirectories. "
            "Requires GLEAM_SFTP_* and GLEAM_REMOTE_DIR to be set in the env file."
        ),
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path("packages/lakesource/.env"),
        help="Env file containing download credentials.",
    )
    return parser.parse_args()


def main() -> None:
    """Create dataset directories, report capacity, and optionally download public data."""
    args = parse_args()
    load_env(dotenv_path=args.env_file)
    warehouse_root = args.warehouse_root
    ensure_dataset_directories(warehouse_root)
    report = build_storage_report(warehouse_root)
    auth_config = load_auth_config()

    print("Water-balance dataset plan")
    for line in dataset_plan_lines(warehouse_root):
        print(line)
    print()
    for line in auth_config_lines(auth_config):
        print(line)
    print()
    print("Storage report")
    for line in storage_report_lines(report):
        print(line)

    if not args.download_public and not args.download_auth and not args.download_gleam_monthly:
        return

    if args.download_public and not report.enough_for_public_downloads:
        print()
        print("Public download skipped: insufficient storage.")
    elif args.download_public:
        downloaded_paths = download_public_datasets(warehouse_root)
        print()
        if not downloaded_paths:
            print("Public downloads already present.")
        else:
            print("Downloaded public files")
            for path in downloaded_paths:
                print(f"- {path}")

    if args.download_gleam_monthly:
        print()
        print("Downloading GLEAM monthly datasets...")
        downloaded_gleam = download_gleam_monthly_datasets(
            warehouse_root, auth_config.gleam
        )
        if not downloaded_gleam:
            print("No GLEAM monthly files were downloaded (all present or incomplete credentials).")
        else:
            total_files = sum(len(v) for v in downloaded_gleam.values())
            print(f"Downloaded {total_files} GLEAM monthly file(s):")
            for var, paths in downloaded_gleam.items():
                print(f"  {var}: {len(paths)} file(s)")

    if not args.download_auth:
        return

    downloaded_auth_paths = download_authenticated_datasets(warehouse_root, auth_config)
    print()
    if not downloaded_auth_paths:
        print(
            "Authenticated downloads skipped: credentials are incomplete or files exist."
        )
        return
    print("Downloaded authenticated files")
    for dataset_name, dataset_paths in downloaded_auth_paths.items():
        if not dataset_paths:
            continue
        print(f"- {dataset_name}")
        for path in dataset_paths:
            print(f"  {path}")


if __name__ == "__main__":
    main()
