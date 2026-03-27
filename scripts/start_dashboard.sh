#!/usr/bin/env bash
# Start the Hydrofetch Dashboard (FastAPI backend + React/Vite frontend)
# Usage: bash scripts/start_dashboard.sh
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$REPO_ROOT/packages/hydrofetch-dashboard-web"

echo "==> Starting FastAPI backend on http://127.0.0.1:8050 ..."
uv run --package hydrofetch-dashboard-api \
    python -m hydrofetch_dashboard_api.main &
API_PID=$!

echo "==> Starting React/Vite frontend on http://localhost:5170 ..."
cd "$WEB_DIR" && npm run dev &
WEB_PID=$!

trap "kill $API_PID $WEB_PID 2>/dev/null; exit" INT TERM

echo ""
echo "  Dashboard: http://localhost:5170"
echo "  API docs:  http://127.0.0.1:8050/docs"
echo ""
echo "Press Ctrl+C to stop."
wait
