#!/bin/bash
set -euo pipefail

SMOKE_MPI_FILTER="${SMOKE_MPI_FILTER:-full}"

source "$(dirname "$0")/smoke_mpi_common.sh"

smoke_init_logging "smoke_mpi_pwm"
smoke_require_commands
smoke_print_config

mapfile -t RANGE_ARGS < <(smoke_range_args)

smoke_cleanup_tables \
  pwm_extreme_thresholds \
  pwm_extreme_labels \
  pwm_extreme_extremes \
  pwm_extreme_abrupt_transitions \
  pwm_extreme_run_status

run_mpi python -m lakeanalysis.cli pwm run \
  "${RANGE_ARGS[@]}" \
  --method stl
