#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

WORKERS="${HAWKES_WORKERS:-$(nproc)}"
CHUNK_SIZE="${HAWKES_CHUNK_SIZE:-10000}"
HAWKES_WINDOW_MONTHS="${HAWKES_WINDOW_MONTHS:-4}"
EOT_INTEGRATION_POINTS="${EOT_INTEGRATION_POINTS:-256}"
EOT_MAX_RESTARTS="${EOT_MAX_RESTARTS:-4}"
MONTHLY_SIGNIFICANCE_QUANTILE="${MONTHLY_SIGNIFICANCE_QUANTILE:-0.95}"

echo "Running full Hawkes batch workflow:"
echo "  workers=${WORKERS}"
echo "  chunk_size=${CHUNK_SIZE}"
echo "  hawkes_window_months=${HAWKES_WINDOW_MONTHS}"
echo "  eot_integration_points=${EOT_INTEGRATION_POINTS}"
echo "  eot_max_restarts=${EOT_MAX_RESTARTS}"
echo "  monthly_significance_quantile=${MONTHLY_SIGNIFICANCE_QUANTILE}"
echo "  plot_mode=none"
echo "  to_file=true"

uv run python scripts/run_hawkes_batch.py \
  --workers "${WORKERS}" \
  --chunk-size "${CHUNK_SIZE}" \
  --plot-mode none \
  --to-file \
  --hawkes-window-months "${HAWKES_WINDOW_MONTHS}" \
  --eot-integration-points "${EOT_INTEGRATION_POINTS}" \
  --eot-max-restarts "${EOT_MAX_RESTARTS}" \
  --monthly-significance-quantile "${MONTHLY_SIGNIFICANCE_QUANTILE}" \
  "$@"