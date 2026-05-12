"""Grid map factory closure tests (P0).

Validates the behaviour of make_grid_map / make_density_map closures
without requiring a live LakeProvider.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lakeviz.grid_map_factory import make_density_map, make_grid_map
from lakeviz.config import GlobalGridConfig


class FakeProvider:
    """Returns pre-defined DataFrames for each query name."""

    def __init__(self, responses: dict | None = None) -> None:
        self._responses = responses or {}
        self._calls: list[dict] = []

    def fetch_grid_agg(self, query_name: str, resolution: float = 0.5, *, refresh: bool = False, **kwargs) -> pd.DataFrame:
        self._calls.append({"query_name": query_name, "resolution": resolution, "refresh": refresh, **kwargs})
        return self._responses.get(query_name, pd.DataFrame())


def _make_agg_df(
    n_cells: int = 10,
    value_col: str = "my_value",
    lake_count: int = 10,
    event_count: int = 50,
) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "cell_lat": rng.uniform(-60, 60, size=n_cells),
        "cell_lon": rng.uniform(-180, 180, size=n_cells),
        value_col: rng.uniform(1, 100, size=n_cells),
        "lake_count": [lake_count] * n_cells,
        "event_count": [event_count] * n_cells,
    })


def _make_config(output_dir: Path) -> GlobalGridConfig:
    return GlobalGridConfig(
        provider=FakeProvider(),
        resolution=0.5,
        output_dir=output_dir,
    )


class TestMakeGridMap:
    def test_empty_data_returns_empty_path(self, tmp_path: Path) -> None:
        provider = FakeProvider({"mydata": pd.DataFrame()})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_grid_map(fetch, "my_value", title="Test", filename="map.png")
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        result = fn(cfg)
        assert result == Path()

    def test_min_lakes_filter_returns_empty(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=1, lake_count=2)  # 2 < 3
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_grid_map(fetch, "my_value", title="Test", filename="map.png")
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        result = fn(cfg, min_lakes=3)
        assert result == Path()

    def test_pre_filter_fn_applied(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=4)
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        calls = []

        def my_filter(agg: pd.DataFrame) -> pd.DataFrame:
            calls.append(len(agg))
            return agg.head(2)

        fn = make_grid_map(
            fetch, "my_value", title="Test", filename="map.png",
            pre_filter_fn=my_filter,
        )
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        fn(cfg)
        assert len(calls) == 1
        assert calls[0] == 4  # all 4 rows passed to filter

    def test_extra_fetch_kwargs_passed(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=4)
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            record = {"resolution": res, "refresh": refresh, **kw}
            provider._calls.append(record)
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_grid_map(
            fetch, "my_value", title="Test", filename="map.png",
            extra_fetch_kwargs={"tail": "high", "quantile": 0.95},
        )
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        fn(cfg)
        assert provider._calls[1]["tail"] == "high"
        assert provider._calls[1]["quantile"] == 0.95

    def test_normal_flow_saves_file(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=4)
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            record = {"resolution": res, "refresh": refresh, **kw}
            provider._calls.append(record)
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_grid_map(
            fetch, "my_value", title="Test", filename="map.png",
            extra_fetch_kwargs={"tail": "high", "quantile": 0.95},
        )
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        fn(cfg)
        # First call is fetch_grid_agg, second is fetch
        assert provider._calls[1]["tail"] == "high"
        assert provider._calls[1]["quantile"] == 0.95

    def test_normal_flow_saves_file(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=3)
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_grid_map(fetch, "my_value", title="My Map", filename="my_map.png")
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        result = fn(cfg)
        assert result == tmp_path / "my_map.png"
        assert result.exists()

    def test_sub_dir_created(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=4)
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_grid_map(
            fetch, "my_value", title="Test", filename="map.png",
            sub_dir="sub",
        )
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        result = fn(cfg)
        assert result == tmp_path / "sub" / "map.png"
        assert result.exists()


class TestMakeDensityMap:
    def test_normal_flow_saves_file(self, tmp_path: Path) -> None:
        df = _make_agg_df(n_cells=3)
        provider = FakeProvider({"mydata": df})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_density_map(fetch, "my_value", title="Density Map", filename="density.png")
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        result = fn(cfg)
        assert result == tmp_path / "density.png"
        assert result.exists()

    def test_empty_data_returns_empty_path(self, tmp_path: Path) -> None:
        provider = FakeProvider({"mydata": pd.DataFrame()})

        def fetch(prov, res, *, refresh, **kw):
            return prov.fetch_grid_agg("mydata", res, refresh=refresh, **kw)

        fn = make_density_map(fetch, "my_value", title="Test", filename="density.png")
        cfg = GlobalGridConfig(provider=provider, resolution=0.5, output_dir=tmp_path)
        result = fn(cfg)
        assert result == Path()
