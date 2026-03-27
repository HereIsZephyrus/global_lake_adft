#!/usr/bin/env bash
# Full smoke workflow: write to both file and db.
#
# This is a thin wrapper around run_smoke.sh that:
# - forces sinks to "file db"
# - defaults all smoke outputs and intermediate state into repo-root data/
# - defaults HYDROFETCH_ENV_FILE to packages/hydrofetch/.env when unset
#
# Usage:
#   ./run_smoke_file_db.sh
#   ./run_smoke_file_db.sh --dry-run
#   HYDROFETCH_SMOKE_DAYS=3 ./run_smoke_file_db.sh
#
# Optional env:
#   HYDROFETCH_SMOKE_DATA_DIR   base directory for all smoke data
#                               (default: <repo>/data)
#   HYDROFETCH_ENV_FILE         hydrofetch env file
#                               (default: <repo>/packages/hydrofetch/.env)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

DATA_DIR="${HYDROFETCH_SMOKE_DATA_DIR:-$REPO_ROOT/data}"
mkdir -p "$DATA_DIR"

export HYDROFETCH_ENV_FILE="${HYDROFETCH_ENV_FILE:-$REPO_ROOT/packages/hydrofetch/.env}"
export HYDROFETCH_SMOKE_SINK="file db"
export HYDROFETCH_SMOKE_OUTPUT="${HYDROFETCH_SMOKE_OUTPUT:-$DATA_DIR/hydrofetch_smoke_file_db_out}"
export HYDROFETCH_SMOKE_JOB_DIR="${HYDROFETCH_SMOKE_JOB_DIR:-$DATA_DIR/hydrofetch_smoke_file_db_jobs}"
export HYDROFETCH_SMOKE_RAW_DIR="${HYDROFETCH_SMOKE_RAW_DIR:-$DATA_DIR/hydrofetch_smoke_file_db_raw}"
export HYDROFETCH_SMOKE_SAMPLE_DIR="${HYDROFETCH_SMOKE_SAMPLE_DIR:-$DATA_DIR/hydrofetch_smoke_file_db_sample}"

exec "$SCRIPT_DIR/run_smoke.sh" "$@"
