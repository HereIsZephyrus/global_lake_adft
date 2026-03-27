#!/usr/bin/env bash
# Full hydrofetch workflow: tiled ERA5 export + local zonal sampling + DB sink.
#
# This script is the production-oriented counterpart to the smoke wrappers:
# - requires an explicit tile manifest
# - requires explicit start/end dates
# - defaults to writing only the db sink
# - keeps all working state under repo-root data/
#
# Usage:
#   ./run_full_file_db.sh --tile-manifest path/to/tiles.json --start 2020-01-01 --end 2020-02-01
#   ./run_full_file_db.sh --tile-manifest path/to/tiles.json --start 2020-01-01 --end 2020-02-01 --dry-run
#
# Optional env:
#   HYDROFETCH_ENV_FILE            hydrofetch env file
#                                  (default: <repo>/packages/hydrofetch/.env)
#   HYDROFETCH_FULL_DATA_DIR       base directory for intermediate data
#                                  (default: <repo>/data)
#   HYDROFETCH_FULL_JOB_DIR        job metadata directory
#   HYDROFETCH_FULL_RAW_DIR        raw GeoTIFF directory
#   HYDROFETCH_FULL_SAMPLE_DIR     staged sample Parquet directory
#   HYDROFETCH_FULL_DB_TABLE       target DB table (default: era5_forcing)

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_full_file_db.sh --tile-manifest FILE --start YYYY-MM-DD --end YYYY-MM-DD [extra hydrofetch args]

Required:
  --tile-manifest FILE   JSON manifest with tile_id / geometry_path / region_path entries
  --start YYYY-MM-DD     inclusive start date
  --end YYYY-MM-DD       exclusive end date

Examples:
  run_full_file_db.sh --tile-manifest data/continents.json --start 2020-01-01 --end 2020-02-01
  run_full_file_db.sh --tile-manifest data/continents.json --start 2020-01-01 --end 2020-02-01 --dry-run
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

TILE_MANIFEST=""
START=""
END=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tile-manifest)
      [[ $# -ge 2 ]] || { echo "error: --tile-manifest requires a value" >&2; exit 1; }
      TILE_MANIFEST="$2"
      shift 2
      ;;
    --start)
      [[ $# -ge 2 ]] || { echo "error: --start requires a value" >&2; exit 1; }
      START="$2"
      shift 2
      ;;
    --end)
      [[ $# -ge 2 ]] || { echo "error: --end requires a value" >&2; exit 1; }
      END="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$TILE_MANIFEST" || -z "$START" || -z "$END" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -f "$TILE_MANIFEST" ]]; then
  echo "error: tile manifest not found: $TILE_MANIFEST" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv not found" >&2
  exit 1
fi

DATA_DIR="${HYDROFETCH_FULL_DATA_DIR:-$REPO_ROOT/data}"
mkdir -p "$DATA_DIR"

export HYDROFETCH_ENV_FILE="${HYDROFETCH_ENV_FILE:-$REPO_ROOT/packages/hydrofetch/.env}"
export HYDROFETCH_JOB_DIR="${HYDROFETCH_FULL_JOB_DIR:-$DATA_DIR/hydrofetch_full_file_db_jobs}"
export HYDROFETCH_RAW_DIR="${HYDROFETCH_FULL_RAW_DIR:-$DATA_DIR/hydrofetch_full_file_db_raw}"
export HYDROFETCH_SAMPLE_DIR="${HYDROFETCH_FULL_SAMPLE_DIR:-$DATA_DIR/hydrofetch_full_file_db_sample}"

DB_TABLE="${HYDROFETCH_FULL_DB_TABLE:-era5_forcing}"

cd "$REPO_ROOT"

exec uv run --package hydrofetch   --env-file "$HYDROFETCH_ENV_FILE"   hydrofetch era5   --start "$START"   --end "$END"   --tile-manifest "$TILE_MANIFEST"   --db-table "$DB_TABLE"   --sink db   --run   "${EXTRA_ARGS[@]}"
