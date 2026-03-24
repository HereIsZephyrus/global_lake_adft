"""Earth Engine initialisation (idempotent, project-aware)."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

_initialised: bool = False


def is_initialised() -> bool:
    """Return True if ``init_earth_engine`` succeeded in this process."""
    return _initialised


def init_earth_engine(*, project: str | None = None, force: bool = False) -> None:
    """Initialise the EE Python client (idempotent).

    Credentials must already exist in the gcloud/application-default location or
    the path set by the EE client.  Call ``ee.Authenticate()`` interactively once
    if this is the first run on a new machine.

    Args:
        project: GEE cloud project ID.  Defaults to ``HYDROFETCH_GEE_PROJECT``.
        force: Re-initialise even if already done in this process.
    """
    global _initialised  # pylint: disable=global-statement
    if _initialised and not force:
        return

    from hydrofetch.config import get_gee_project  # pylint: disable=import-outside-toplevel

    import ee  # pylint: disable=import-outside-toplevel,import-error

    proj = project if project is not None else get_gee_project()
    log.debug("Initialising Earth Engine with project=%s", proj)
    ee.Initialize(project=proj)
    _initialised = True
    log.info("Earth Engine initialised (project=%s)", proj)


def check_task_status(task_id: str) -> dict:
    """Return the raw GEE task status dict for *task_id*.

    Args:
        task_id: The identifier returned by ``ee.batch.Task.id`` at submission.

    Returns:
        Dict with at minimum ``{"state": "COMPLETED"|"FAILED"|"RUNNING"|...}``.

    Raises:
        RuntimeError: If the status list is unexpectedly empty.
    """
    import ee  # pylint: disable=import-outside-toplevel,import-error

    results = ee.data.getTaskStatus([task_id])
    if not results:
        raise RuntimeError(f"GEE returned empty status for task_id={task_id!r}")
    return results[0]


__all__ = [
    "check_task_status",
    "init_earth_engine",
    "is_initialised",
]
