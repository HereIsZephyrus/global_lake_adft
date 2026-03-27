#!/usr/bin/env bash
# Minimal hydrofetch ERA5-Land smoke run: 10 days, real lake polygons, tiled export.
#
# Before enqueueing jobs, this script generates one source-of-truth lake dataset
# from the database, then derives per-tile geometry/region files and a manifest.
# The smoke workflow therefore exercises the same tile expansion path as the
# main hydrofetch CLI.
#
# Usage:
#   ./run_smoke.sh              # enqueue + run monitor (needs GEE + Drive auth)
#   ./run_smoke.sh --dry-run    # only print jobs that would be enqueued
#   HYDROFETCH_SMOKE_SINK="db"  ./run_smoke.sh   # write to PostgreSQL instead
#
# Env (optional):
#   HYDROFETCH_SMOKE_START   first date inclusive (default: 2020-01-01)
#   HYDROFETCH_SMOKE_DAYS    number of days (default: 10) → end = start + days
#   HYDROFETCH_SMOKE_OUTPUT  output dir for Parquet (default: ./hydrofetch_smoke_out)
#   HYDROFETCH_SMOKE_SINK    one or more sinks: file | db  (default: file)
#   HYDROFETCH_ENV_FILE      passed as hydrofetch --env-file if set
#   HYDROFETCH_SMOKE_LIMIT   number of lakes to include (default: 10)
#   HYDROFETCH_SMOKE_OFFSET  SQL offset into area_quality (default: 0)
#   HYDROFETCH_SMOKE_BUFFER_DEG  per-tile bbox padding in degrees (default: 0.05)
#   Fixtures are regenerated on every run so the smoke dataset always matches
#   the current database slice.
#
# Isolation overrides (all optional):
#   HYDROFETCH_SMOKE_JOB_DIR / _RAW_DIR / _SAMPLE_DIR
#       If set, exported as HYDROFETCH_JOB_DIR / _RAW_DIR / _SAMPLE_DIR so
#       smoke state does not mix with production jobs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# packages/hydrofetch/scripts/smoke → monorepo root
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

MANIFEST="$SCRIPT_DIR/fixtures/smoke_manifest.json"
SMOKE_GEOMETRY="$SCRIPT_DIR/fixtures/smoke_lakes_polygons.geojson"
SMOKE_TILES_DIR="$SCRIPT_DIR/fixtures/tiles"
FIXTURE_SCRIPT="$SCRIPT_DIR/generate_smoke_fixtures.py"

START="${HYDROFETCH_SMOKE_START:-2020-01-01}"
DAYS="${HYDROFETCH_SMOKE_DAYS:-10}"
OUTPUT_DIR="${HYDROFETCH_SMOKE_OUTPUT:-$REPO_ROOT/hydrofetch_smoke_out}"
SMOKE_LIMIT="${HYDROFETCH_SMOKE_LIMIT:-10}"
SMOKE_OFFSET="${HYDROFETCH_SMOKE_OFFSET:-0}"
read -r -a SINKS <<< "${HYDROFETCH_SMOKE_SINK:-file}"

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv not found" >&2
  exit 1
fi

END_ISO="$(uv run python -c "from datetime import date, timedelta; \
s=date.fromisoformat('${START}'); \
print((s + timedelta(days=int('${DAYS}'))).isoformat())")"

ENV_ARGS=()
if [[ -n "${HYDROFETCH_ENV_FILE:-}" ]]; then
  ENV_ARGS=(--env-file "$HYDROFETCH_ENV_FILE")
fi

# Export isolation overrides when provided.
[[ -n "${HYDROFETCH_SMOKE_JOB_DIR:-}"    ]] && export HYDROFETCH_JOB_DIR="$HYDROFETCH_SMOKE_JOB_DIR"
[[ -n "${HYDROFETCH_SMOKE_RAW_DIR:-}"    ]] && export HYDROFETCH_RAW_DIR="$HYDROFETCH_SMOKE_RAW_DIR"
[[ -n "${HYDROFETCH_SMOKE_SAMPLE_DIR:-}" ]] && export HYDROFETCH_SAMPLE_DIR="$HYDROFETCH_SMOKE_SAMPLE_DIR"

cd "$REPO_ROOT"

if [[ ! -f "$FIXTURE_SCRIPT" ]]; then
  echo "error: fixture generator not found: $FIXTURE_SCRIPT" >&2
  exit 1
fi

FIXTURE_ARGS=(
  "$FIXTURE_SCRIPT"
  --limit "$SMOKE_LIMIT"
  --offset "$SMOKE_OFFSET"
  --buffer-deg "${HYDROFETCH_SMOKE_BUFFER_DEG:-0.05}"
  --geometry-output "$SMOKE_GEOMETRY"
  --tiles-dir "$SMOKE_TILES_DIR"
  --manifest-output "$MANIFEST"
)
if [[ -n "${HYDROFETCH_ENV_FILE:-}" ]]; then
  FIXTURE_ARGS+=(--env-file "$HYDROFETCH_ENV_FILE")
fi
uv run --all-packages python "${FIXTURE_ARGS[@]}"

if [[ ! -f "$MANIFEST" ]]; then
  echo "error: generated manifest not found: $MANIFEST" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

exec uv run --package hydrofetch "${ENV_ARGS[@]}" hydrofetch era5 \
  --start "$START" \
  --end "$END_ISO" \
  --tile-manifest "$MANIFEST" \
  --output-dir "$OUTPUT_DIR" \
  --sink "${SINKS[@]}" \
  --run \
  "$@"
