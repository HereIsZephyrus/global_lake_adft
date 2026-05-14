"""Build dense worker-scoped lake datasets from a provider-backed source."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from lakesource.config import SourceConfig
from typing import TYPE_CHECKING

from .lake_dataset import LakeDataset
from .lake_dataset_query import LakeDatasetQuery
from .task_spec import get_batch_task_spec

if TYPE_CHECKING:
    from lakesource.provider.base import LakeProvider


_ALL_IDS_END = 2 ** 31


@dataclass(frozen=True)
class LakeDatasetFactory:
    provider: "LakeProvider"
    raw_source: str = "lake_info"

    @classmethod
    def from_config(
        cls,
        config: SourceConfig,
        *,
        raw_source: str = "lake_info",
    ) -> "LakeDatasetFactory":
        from lakesource.provider.factory import create_provider

        return cls(provider=create_provider(config), raw_source=raw_source)

    def build(self, query: LakeDatasetQuery) -> LakeDataset:
        candidate_ids = self._resolve_candidate_ids(query)
        if query.exclude_done and query.algorithm:
            candidate_ids = self._exclude_done_ids(candidate_ids, query.algorithm)

        id_list = sorted(candidate_ids)
        lake_map = self.provider.fetch_lake_area_by_ids(id_list)
        frozen_map = self.provider.fetch_frozen_year_months_by_ids(id_list)

        values, year_months, present_ids = self._materialize_values(id_list, lake_map)
        frozen_mask = self._materialize_frozen_mask(present_ids, year_months, frozen_map)
        extra = self._materialize_extra(present_ids, query.fields)
        return LakeDataset(
            hylak_ids=np.asarray(present_ids, dtype=np.int64),
            year_months=year_months,
            values=values,
            frozen_mask=frozen_mask,
            extra=extra,
        )

    def _resolve_candidate_ids(self, query: LakeDatasetQuery) -> set[int]:
        ids = self._fetch_id_set(self.raw_source)
        if query.require_quality:
            ids &= self._fetch_id_set("area_quality")
        if query.id_range is not None:
            start, end = query.id_range
            ids = {hid for hid in ids if start <= hid < end}
        if query.id_subset is not None:
            ids &= set(query.id_subset)
        return ids

    def _exclude_done_ids(self, candidate_ids: set[int], algorithm: str) -> set[int]:
        if not candidate_ids:
            return set()
        spec = get_batch_task_spec(algorithm)
        if spec.done_table is None:
            return candidate_ids
        start = min(candidate_ids)
        end = max(candidate_ids) + 1
        status = "done" if spec.done_requires_status else None
        done_ids = self.provider.fetch_done_ids(spec.done_table, start, end, status=status)
        return candidate_ids - done_ids

    def _fetch_id_set(self, table_name: str) -> set[int]:
        rows = self.provider.fetch_rows(table_name, 0, _ALL_IDS_END)
        return {int(row["hylak_id"]) for row in rows if row.get("hylak_id") is not None}

    def _materialize_values(
        self,
        id_list: list[int],
        lake_map: dict[int, pd.DataFrame],
    ) -> tuple[np.ndarray, np.ndarray, list[int]]:
        present_ids = [hid for hid in id_list if hid in lake_map]
        if not present_ids:
            return (
                np.empty((0, 0), dtype=float),
                np.empty((0,), dtype=np.int64),
                [],
            )

        month_index = self._year_month_index(lake_map[present_ids[0]])
        values = np.empty((len(present_ids), len(month_index)), dtype=float)
        for row_idx, hid in enumerate(present_ids):
            df = lake_map[hid]
            candidate_index = self._year_month_index(df)
            if not np.array_equal(month_index, candidate_index):
                raise ValueError(
                    f"lake {hid} has inconsistent year_month axis relative to batch"
                )
            values[row_idx] = df["water_area"].to_numpy(dtype=float)
        return values, month_index, present_ids

    def _materialize_frozen_mask(
        self,
        present_ids: list[int],
        year_months: np.ndarray,
        frozen_map: dict[int, set[int]],
    ) -> np.ndarray:
        if not present_ids or len(year_months) == 0:
            return np.empty((len(present_ids), len(year_months)), dtype=bool)
        frozen_mask = np.zeros((len(present_ids), len(year_months)), dtype=bool)
        for row_idx, hid in enumerate(present_ids):
            frozen = frozen_map.get(hid, set())
            if not frozen:
                continue
            frozen_mask[row_idx] = np.isin(year_months, np.fromiter(frozen, dtype=np.int64))
        return frozen_mask

    def _materialize_extra(
        self,
        present_ids: list[int],
        fields: tuple[str, ...],
    ) -> dict[str, np.ndarray] | None:
        extra: dict[str, np.ndarray] = {}
        if "atlas_area" in fields:
            area_map = self.provider.fetch_atlas_area_by_ids(present_ids)
            extra["atlas_area"] = np.asarray(
                [float(area_map.get(hid, 0.0)) for hid in present_ids],
                dtype=float,
            )
        return extra or None

    @staticmethod
    def _year_month_index(df: pd.DataFrame) -> np.ndarray:
        if "year_month" in df.columns:
            series = pd.to_datetime(df["year_month"])
            return (series.dt.year * 100 + series.dt.month).to_numpy(dtype=np.int64)
        if "year" not in df.columns or "month" not in df.columns:
            raise ValueError("series dataframe must contain year/month or year_month columns")
        return (df["year"].astype(int) * 100 + df["month"].astype(int)).to_numpy(
            dtype=np.int64
        )
