#!/bin/bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  printf 'source this file from a smoke script\n' >&2
  exit 1
fi

SMOKE_MPI_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE_MPI_NAMESPACE="${SMOKE_MPI_NAMESPACE:-smoke_mpi}"
SMOKE_MPI_FILTER="${SMOKE_MPI_FILTER:-}"
SMOKE_MPI_OUTPUT_ROOT="${SMOKE_MPI_OUTPUT_ROOT:-$SMOKE_MPI_ROOT/output/$SMOKE_MPI_NAMESPACE}"
SMOKE_MPI_LOG_DIR="${SMOKE_MPI_LOG_DIR:-$SMOKE_MPI_ROOT/logs}"

export DATA_BACKEND="${DATA_BACKEND:-parquet}"
export PARQUET_DATA_DIR="${PARQUET_DATA_DIR:-$SMOKE_MPI_ROOT/data}"
if [[ -z "$SMOKE_MPI_FILTER" ]]; then
  printf 'SMOKE_MPI_FILTER must be set explicitly\n' >&2
  return 1
fi
export OUTPUT_DIR="${OUTPUT_DIR:-$SMOKE_MPI_OUTPUT_ROOT/$SMOKE_MPI_FILTER}"
export LAKE_FILTER="$SMOKE_MPI_FILTER"

NP="${NP:-4}"
CHUNK_SIZE="${CHUNK_SIZE:-3}"
ID_START="${ID_START:-0}"
ID_END="${ID_END:-2000}"
SMOKE_MPI_TIMESTAMP="${SMOKE_MPI_TIMESTAMP:-$(date +%Y%m%d_%H%M%S)}"
SMOKE_MPI_LOG_FILE="${SMOKE_MPI_LOG_FILE:-}"

smoke_require_commands() {
  command -v mpiexec >/dev/null
  command -v uv >/dev/null
}

smoke_range_args() {
  printf '%s\n' \
    --filter "$SMOKE_MPI_FILTER" \
    --chunk-size "$CHUNK_SIZE" \
    --id-start "$ID_START" \
    --id-end "$ID_END"
}

run_mpi() {
  mpiexec --oversubscribe -np "$NP" "$@"
}

smoke_cleanup_tables() {
  local table
  mkdir -p "$OUTPUT_DIR"
  for table in "$@"; do
    rm -f "$OUTPUT_DIR/${table}.parquet"
  done
}

smoke_print_config() {
  cat <<EOF
[smoke-mpi]
root=$SMOKE_MPI_ROOT
namespace=$SMOKE_MPI_NAMESPACE
filter=$SMOKE_MPI_FILTER
data_backend=$DATA_BACKEND
parquet_data_dir=$PARQUET_DATA_DIR
output_dir=$OUTPUT_DIR
np=$NP
chunk_size=$CHUNK_SIZE
id_start=$ID_START
id_end=$ID_END
log_dir=$SMOKE_MPI_LOG_DIR
log_file=${SMOKE_MPI_LOG_FILE:-<unset>}
EOF
}

smoke_init_logging() {
  local script_name
  script_name="$1"
  mkdir -p "$SMOKE_MPI_LOG_DIR"
  if [[ -z "$SMOKE_MPI_LOG_FILE" ]]; then
    SMOKE_MPI_LOG_FILE="$SMOKE_MPI_LOG_DIR/${script_name}_${SMOKE_MPI_TIMESTAMP}.log"
  fi
  export SMOKE_MPI_LOG_FILE
  exec > >(tee -a "$SMOKE_MPI_LOG_FILE") 2>&1
}
