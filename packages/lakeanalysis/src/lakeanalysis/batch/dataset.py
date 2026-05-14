"""Dataset: quality-filtered lake candidate set shared by all run scripts.

Reads ``area_quality.parquet`` once via ``LakeProvider.fetch_rows`` and
applies an optional ``LakeFilter`` to produce the final candidate ID set.
All run scripts should instantiate this class instead of constructing
filters manually.
"""

from __future__ import annotations

import logging

from lakesource.config import SourceConfig
from lakesource.provider import create_provider
from .domain import LakeFilter
from .filter import IdSetFilter

log = logging.getLogger(__name__)


class Dataset:
    """Lazily loads ``area_quality`` IDs, applies optional filter."""

    def __init__(
        self,
        config: SourceConfig,
        *,
        lake_filter: LakeFilter | None = None,
    ) -> None:
        self._config = config
        self._lake_filter = lake_filter
        self._ids: set[int] | None = None

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    @property
    def ids(self) -> set[int]:
        """Candidate hylak_id set after quality + filter."""
        if self._ids is None:
            self._ids = self._resolve()
        return self._ids

    def as_filter(self) -> IdSetFilter:
        """Return an ``IdSetFilter`` that passes only candidate IDs."""
        return IdSetFilter(ids=self.ids)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve(self) -> set[int]:
        provider = create_provider(self._config)
        rows = provider.fetch_rows("area_quality", 0, 2 ** 31)
        if not rows:
            raise RuntimeError(
                "area_quality is empty — run quality pipeline first"
            )
        ids = {int(r["hylak_id"]) for r in rows}
        log.info("Dataset loaded %d candidates from area_quality", len(ids))

        if self._lake_filter is not None:
            ids = self._lake_filter(ids)
            if not ids:
                raise ValueError(
                    "LakeFilter yielded 0 candidates after quality filter"
                )
            log.info(
                "Dataset filtered to %d candidates after LakeFilter", len(ids)
            )

        return ids
