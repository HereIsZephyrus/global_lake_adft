"""Writer ABC and DBWriter implementation."""

from __future__ import annotations

import logging

from lakesource.postgres import series_db
from ..engine import Writer

log = logging.getLogger(__name__)

_UPSERT_FNS = {
    "quantile_labels": "upsert_quantile_labels",
    "quantile_extremes": "upsert_quantile_extremes",
    "quantile_abrupt_transitions": "upsert_quantile_abrupt_transitions",
    "quantile_run_status": "upsert_quantile_run_status",
    "pwm_extreme_thresholds": "upsert_pwm_extreme_thresholds",
    "pwm_extreme_run_status": "upsert_pwm_extreme_run_status",
    "eot_results": "upsert_eot_results",
    "eot_extremes": "upsert_eot_extremes",
    "eot_run_status": "upsert_eot_run_status",
}


class DBWriter(Writer):
    def __init__(self, algorithm: str, *, conn_source=None) -> None:
        self._algorithm = algorithm
        self._conn_source = conn_source or series_db

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        if not any(rows_by_table.values()):
            return
        with self._conn_source.connection_context() as conn:
            try:
                for table_name, rows in rows_by_table.items():
                    if not rows:
                        continue
                    fn_name = _UPSERT_FNS.get(table_name)
                    if fn_name is None:
                        log.warning("No upsert function for table %s", table_name)
                        continue
                    fn = getattr(__import__("lakesource.postgres", fromlist=[fn_name]), fn_name)
                    fn(conn, rows, commit=False)
                conn.commit()
            except Exception:
                conn.rollback()
                raise