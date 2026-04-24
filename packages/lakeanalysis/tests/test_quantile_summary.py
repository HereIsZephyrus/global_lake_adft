from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from lakeanalysis.quantile import (
    load_summary_cache,
    save_summary_plots_from_cache,
    write_summary_cache,
)


def test_summary_cache_roundtrip_and_plots(tmp_path: Path) -> None:
    cache_root = tmp_path / "summary_cache"
    output_root = tmp_path / "summary"

    cache_paths = write_summary_cache(
        cache_root,
        transition_counts=pd.DataFrame(
            [
                {"transition_type": "low_to_high", "count": 3},
                {"transition_type": "high_to_low", "count": 2},
            ]
        ),
        transition_seasonality=pd.DataFrame(
            [
                {"to_month": 1, "count": 2},
                {"to_month": 7, "count": 3},
            ]
        ),
        lake_transition_counts=pd.DataFrame(
            [
                {"hylak_id": 101, "transition_count": 2},
                {"hylak_id": 202, "transition_count": 3},
            ]
        ),
        lake_extreme_counts=pd.DataFrame(
            [
                {"hylak_id": 101, "extreme_count": 8},
                {"hylak_id": 202, "extreme_count": 9},
            ]
        ),
        run_metadata={"labels_rows": 50, "extremes_rows": 17, "transitions_rows": 5},
    )
    for path in cache_paths.values():
        assert path.exists()
        assert path.stat().st_size > 0

    loaded = load_summary_cache(cache_root)
    assert set(loaded["transition_counts"]["transition_type"]) == {"low_to_high", "high_to_low"}
    assert len(loaded["transition_seasonality"]) == 12

    plot_paths = save_summary_plots_from_cache(cache_root, output_root)
    for path in plot_paths.values():
        assert path.exists()
        assert path.stat().st_size > 0
