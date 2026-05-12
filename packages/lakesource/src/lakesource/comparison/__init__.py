"""Comparison experiment: shared schemas and DB adapters."""

from .schema import RUN_STATUS_DONE, RUN_STATUS_ERROR

__all__ = [
    "RUN_STATUS_DONE",
    "RUN_STATUS_ERROR",
]


def __getattr__(name: str):
    if name in ("ensure_comparison_tables", "make_run_status_row", "upsert_comparison_run_status"):
        from . import store
        return getattr(store, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
