"""Background cache for low-frequency database size metrics."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone

from hydrofetch_dashboard_api import config
from hydrofetch_dashboard_api.sources import database as db_src

log = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DBMetricsManager:
    """Refresh database size metrics on background intervals."""

    def __init__(
        self,
        *,
        table_names: list[str],
        total_refresh_seconds: int,
        table_refresh_seconds: int,
        poll_seconds: int = 60,
    ) -> None:
        self._table_names = list(table_names)
        self._total_refresh_seconds = max(60, int(total_refresh_seconds))
        self._table_refresh_seconds = max(60, int(table_refresh_seconds))
        self._poll_seconds = max(15, int(poll_seconds))
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = db_src.DBSizeStats(
            available=False,
            message="数据库体积统计初始化中…",
        )
        self._last_total_refresh = 0.0
        self._last_table_refresh = 0.0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="dashboard-db-metrics",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None

    def get_stats(self) -> db_src.DBSizeStats:
        with self._lock:
            snapshot = replace(self._state)
            snapshot.tables = [dict(row) for row in self._state.tables]
            return snapshot

    def refresh_once(self, *, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            need_total = force or (
                self._state.total_updated_at is None
                or now - self._last_total_refresh >= self._total_refresh_seconds
            )
            need_tables = force or (
                self._state.tables_updated_at is None
                or now - self._last_table_refresh >= self._table_refresh_seconds
            )
        if not need_total and not need_tables:
            return

        total_payload: tuple[str, int, str] | None = None
        tables_payload: list[dict] | None = None
        try:
            with db_src._connection() as conn:  # pylint: disable=protected-access
                with conn.cursor() as cur:
                    if need_total:
                        total_payload = db_src.query_db_total_size(cur)
                    if need_tables:
                        tables_payload = db_src.query_table_sizes(
                            cur,
                            table_names=self._table_names,
                        )
        except Exception:
            log.exception("Failed to refresh database size metrics")
            with self._lock:
                if not self._state.available:
                    self._state = db_src.DBSizeStats(
                        available=False,
                        message="数据库体积统计刷新失败",
                    )
            return

        refreshed_at = _utc_now_iso()
        with self._lock:
            next_state = replace(self._state)
            next_state.tables = [dict(row) for row in self._state.tables]
            if total_payload is not None:
                next_state.db_name = total_payload[0]
                next_state.db_size_bytes = total_payload[1]
                next_state.db_size_pretty = total_payload[2]
                next_state.total_updated_at = refreshed_at
                self._last_total_refresh = now
            if tables_payload is not None:
                next_state.tables = tables_payload
                next_state.tables_updated_at = refreshed_at
                self._last_table_refresh = now
            next_state.available = bool(
                next_state.total_updated_at or next_state.tables_updated_at
            )
            next_state.message = "ok"
            self._state = next_state

    def _run(self) -> None:
        self.refresh_once(force=True)
        while not self._stop_event.wait(self._poll_seconds):
            self.refresh_once()


manager = DBMetricsManager(
    table_names=[config.DB_TABLE],
    total_refresh_seconds=config.DB_SIZE_TOTAL_REFRESH_SECONDS,
    table_refresh_seconds=config.DB_SIZE_TABLE_REFRESH_SECONDS,
)


__all__ = ["DBMetricsManager", "manager"]
