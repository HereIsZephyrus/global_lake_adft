from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.batch import LakeDatasetFactory, LakeDatasetQuery


def _series(values: list[float]) -> pd.DataFrame:
    rows = []
    for idx, value in enumerate(values, start=1):
        rows.append({"year": 2000, "month": idx, "water_area": value})
    return pd.DataFrame(rows)


class _FakeProvider:
    def __init__(self) -> None:
        self._quality_ids = {2, 3}
        self._done_ids = {3}
        self._series = {
            1: _series([10.0, 11.0, 12.0]),
            2: _series([20.0, 21.0, 22.0]),
            3: _series([30.0, 31.0, 32.0]),
        }
        self._atlas_area = {1: 100.0, 2: 200.0, 3: 300.0}
        self._frozen = {2: {200002}, 3: {200001, 200003}}

    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        raise NotImplementedError

    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
        return {hid: self._series[hid].copy() for hid in id_list if hid in self._series}

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        raise NotImplementedError

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]) -> dict[int, set[int]]:
        return {hid: set(self._frozen.get(hid, set())) for hid in id_list}

    def fetch_max_hylak_id(self) -> int:
        return 3

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict]:
        if table_name == "lake_info":
            ids = {1, 2, 3}
        elif table_name == "area_quality":
            ids = self._quality_ids
        else:
            ids = set()
        return [
            {"hylak_id": hid}
            for hid in sorted(ids)
            if chunk_start <= hid < chunk_end
        ]

    def fetch_done_ids(
        self,
        table_name: str,
        chunk_start: int,
        chunk_end: int,
        *,
        status: str | None = None,
    ) -> set[int]:
        del table_name, status
        return {hid for hid in self._done_ids if chunk_start <= hid < chunk_end}

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        return {hid: self._atlas_area[hid] for hid in id_list if hid in self._atlas_area}

    def fetch_lake_geometry_wkt_by_ids(self, hylak_ids, *, simplify_tolerance_meters=None):
        raise NotImplementedError

    def fetch_grid_agg(self, query_name, resolution=0.5, *, refresh=False, **kwargs):
        raise NotImplementedError

    @property
    def backend_name(self):
        return "fake"

    @property
    def cache_dir(self):
        return None


def test_lake_dataset_factory_applies_quality_subset_and_done_filters() -> None:
    factory = LakeDatasetFactory(provider=_FakeProvider())

    dataset = factory.build(
        LakeDatasetQuery(
            algorithm="pwm_extreme",
            id_range=(0, 4),
            id_subset=frozenset({1, 2, 3}),
            require_quality=True,
            exclude_done=True,
        )
    )

    assert dataset.hylak_ids.tolist() == [2]
    assert dataset.year_months.tolist() == [200001, 200002, 200003]
    assert dataset.values.shape == (1, 3)
    assert dataset.values.tolist() == [[20.0, 21.0, 22.0]]
    assert dataset.frozen_mask is not None
    assert dataset.frozen_mask.tolist() == [[False, True, False]]


def test_lake_dataset_factory_can_keep_unfiltered_ids_and_attach_extra_fields() -> None:
    factory = LakeDatasetFactory(provider=_FakeProvider())

    dataset = factory.build(
        LakeDatasetQuery(
            require_quality=False,
            exclude_done=False,
            id_range=(1, 3),
            fields=("series", "frozen_mask", "atlas_area"),
        )
    )

    assert dataset.hylak_ids.tolist() == [1, 2]
    assert dataset.extra is not None
    assert np.allclose(dataset.extra["atlas_area"], np.asarray([100.0, 200.0]))


def test_lake_dataset_supports_slice_and_take_views() -> None:
    factory = LakeDatasetFactory(provider=_FakeProvider())
    dataset = factory.build(LakeDatasetQuery(require_quality=False, exclude_done=False))

    sliced = dataset.slice(1, 3)
    taken = dataset.take([2, 0])

    assert sliced.hylak_ids.tolist() == [2, 3]
    assert taken.hylak_ids.tolist() == [3, 1]
    assert taken.values.tolist() == [[30.0, 31.0, 32.0], [10.0, 11.0, 12.0]]


def test_lake_dataset_factory_raises_on_inconsistent_time_axis() -> None:
    provider = _FakeProvider()
    provider._series[3] = pd.DataFrame(
        [
            {"year": 2000, "month": 1, "water_area": 30.0},
            {"year": 2000, "month": 3, "water_area": 32.0},
        ]
    )
    factory = LakeDatasetFactory(provider=provider)

    try:
        factory.build(LakeDatasetQuery(require_quality=False, exclude_done=False))
    except ValueError as exc:
        assert "inconsistent year_month axis" in str(exc)
    else:
        raise AssertionError("expected inconsistent year_month axis to raise")
