#!/usr/bin/env bash
# Run PWM extreme full-batch with 16 MPI workers.
#
# Usage:
#   bash scripts/run_pwm_extreme_full.sh [--dry-run]
#
# Prerequisites:
#   - mpi4py installed (uv pip install mpi4py)
#   - PostgreSQL accessible (SERIES_DB, DB_USER, DB_PASSWORD env vars)
#   - 16+ CPU cores recommended
#
# --dry-run: only print the command, do not execute

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

NP=16
CHUNK_SIZE=10000
IO_BUDGET=4

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
fi

cd "$PROJECT_DIR"

CMD=(
    mpiexec
    -np "$NP"
    python
    scripts/run_pwm_extreme.py
    --chunk-size "$CHUNK_SIZE"
    --io-budget "$IO_BUDGET"
)

echo "=== PWM Extreme Full Batch ==="
echo "Workers:    $NP"
echo "Chunk size: $CHUNK_SIZE"
echo "IO budget:  $IO_BUDGET"
echo "Command:    ${CMD[*]}"
echo "=============================="

if $DRY_RUN; then
    echo "[dry-run] Skipping execution."
    exit 0
fi

exec "${CMD[@]}"