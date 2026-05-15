#!/bin/bash
set -euo pipefail

SMOKE_MPI_FILTER="${SMOKE_MPI_FILTER:-full}"

source "$(dirname "$0")/smoke_mpi_common.sh"

smoke_init_logging "smoke_mpi_quantile"
smoke_require_commands
smoke_print_config

mapfile -t RANGE_ARGS < <(smoke_range_args)

smoke_cleanup_tables \
  quantile_labels \
  quantile_extremes \
  quantile_abrupt_transitions \
  quantile_run_status

run_mpi uv run lake_adft eot quantile \
  "${RANGE_ARGS[@]}" \
  --method stl
