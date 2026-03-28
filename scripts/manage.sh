#!/usr/bin/env bash
# Manage the Hydrofetch Dashboard (API backend + Web frontend).
#
# Usage:
#   bash scripts/manage.sh start        # Start both API and Web
#   bash scripts/manage.sh stop         # Stop both
#   bash scripts/manage.sh restart      # Restart both
#   bash scripts/manage.sh restart-api  # Restart API only (hydrofetch workers keep running)
#   bash scripts/manage.sh restart-web  # Restart Web dev server only
#   bash scripts/manage.sh status       # Show running status
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$REPO_ROOT/packages/hydrofetch-dashboard-web"
RUN_DIR="$REPO_ROOT/.run"

API_PORT="${HYDROFETCH_DASHBOARD_API_PORT:-8050}"
WEB_PORT=5170

API_PID_FILE="$RUN_DIR/api.pid"
WEB_PID_FILE="$RUN_DIR/web.pid"

mkdir -p "$RUN_DIR"

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_pid_alive() { kill -0 "$1" 2>/dev/null; }

_read_pid() {
    local file="$1"
    [ -f "$file" ] && cat "$file" || echo ""
}

_stop_by_pid_file() {
    local label="$1" file="$2"
    local pid; pid=$(_read_pid "$file")
    if [ -n "$pid" ] && _pid_alive "$pid"; then
        echo "  Stopping $label (pid=$pid) ..."
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 30); do
            _pid_alive "$pid" || break
            sleep 0.5
        done
        if _pid_alive "$pid"; then
            echo "  Force-killing $label ..."
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$file"
}

_stop_by_port() {
    local label="$1" port="$2"
    local pids; pids=$(lsof -ti :"$port" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "  Killing leftover $label on port $port (pids: $pids) ..."
        echo "$pids" | xargs kill 2>/dev/null || true
        sleep 1
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        [ -n "$pids" ] && echo "$pids" | xargs kill -9 2>/dev/null || true
    fi
}

# ------------------------------------------------------------------
# Actions
# ------------------------------------------------------------------

do_start_api() {
    local pid; pid=$(_read_pid "$API_PID_FILE")
    if [ -n "$pid" ] && _pid_alive "$pid"; then
        echo "  API already running (pid=$pid)"
        return
    fi
    _stop_by_port "API" "$API_PORT"
    echo "==> Starting API on http://127.0.0.1:$API_PORT ..."
    cd "$REPO_ROOT"
    nohup uv run --package hydrofetch-dashboard-api \
        python -m hydrofetch_dashboard_api.main \
        > "$RUN_DIR/api.log" 2>&1 &
    echo $! > "$API_PID_FILE"
    echo "  API started (pid=$!)"
}

do_start_web() {
    local pid; pid=$(_read_pid "$WEB_PID_FILE")
    if [ -n "$pid" ] && _pid_alive "$pid"; then
        echo "  Web already running (pid=$pid)"
        return
    fi
    _stop_by_port "Web" "$WEB_PORT"
    echo "==> Starting Web on http://localhost:$WEB_PORT ..."
    cd "$WEB_DIR"
    nohup npm run dev > "$RUN_DIR/web.log" 2>&1 &
    echo $! > "$WEB_PID_FILE"
    echo "  Web started (pid=$!)"
}

do_stop_api() {
    _stop_by_pid_file "API" "$API_PID_FILE"
    _stop_by_port "API" "$API_PORT"
}

do_stop_web() {
    _stop_by_pid_file "Web" "$WEB_PID_FILE"
    _stop_by_port "Web" "$WEB_PORT"
}

do_status() {
    echo "=== Dashboard Status ==="
    local pid

    pid=$(_read_pid "$API_PID_FILE")
    if [ -n "$pid" ] && _pid_alive "$pid"; then
        echo "  API:  RUNNING (pid=$pid, port=$API_PORT)"
    else
        echo "  API:  STOPPED"
    fi

    pid=$(_read_pid "$WEB_PID_FILE")
    if [ -n "$pid" ] && _pid_alive "$pid"; then
        echo "  Web:  RUNNING (pid=$pid, port=$WEB_PORT)"
    else
        echo "  Web:  STOPPED"
    fi

    local hf_count; hf_count=$(pgrep -c -f 'hydrofetch era5' 2>/dev/null || echo 0)
    echo "  Hydrofetch workers: $hf_count process(es)"
}

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

case "${1:-}" in
    start)
        do_start_api
        do_start_web
        echo ""
        echo "  Dashboard: http://localhost:$WEB_PORT"
        echo "  API docs:  http://127.0.0.1:$API_PORT/docs"
        ;;
    stop)
        echo "==> Stopping dashboard ..."
        do_stop_api
        do_stop_web
        echo "  Done. (hydrofetch workers are NOT affected)"
        ;;
    restart)
        echo "==> Restarting dashboard ..."
        do_stop_api
        do_stop_web
        sleep 1
        do_start_api
        do_start_web
        echo ""
        echo "  Dashboard: http://localhost:$WEB_PORT"
        echo "  API docs:  http://127.0.0.1:$API_PORT/docs"
        ;;
    restart-api)
        echo "==> Restarting API only (hydrofetch workers keep running) ..."
        do_stop_api
        sleep 1
        do_start_api
        ;;
    restart-web)
        echo "==> Restarting Web only ..."
        do_stop_web
        sleep 1
        do_start_web
        ;;
    status)
        do_status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|restart-api|restart-web|status}"
        exit 1
        ;;
esac
