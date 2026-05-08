"""Lake area reading port."""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class LakeAreaReadPort(Protocol):
    def fetch_lake_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, pd.DataFrame]: ...
    def fetch_lake_area_by_ids(
        self, id_list: list[int]
    ) -> dict[int, pd.DataFrame]: ...
