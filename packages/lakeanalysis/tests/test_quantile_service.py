import pandas as pd

from lakeanalysis.quantile import QuantileServiceConfig, run_single_lake_service


def _build_series() -> pd.DataFrame:
    rows = []
    for year, offset in ((2000, -10.0), (2001, 0.0), (2002, 10.0)):
        for month in range(1, 13):
            rows.append(
                {
                    "year": year,
                    "month": month,
                    "water_area": 100.0 + month + offset,
                }
            )
    return pd.DataFrame(rows)


def test_service_can_ignore_frozen_mask() -> None:
    series_df = _build_series()
    frozen_keys = {200001, 200002, 200003}
    config = QuantileServiceConfig(min_valid_per_month=2, min_valid_observations=33)

    with_mask = run_single_lake_service(
        series_df,
        hylak_id=101,
        config=config,
        frozen_year_months=frozen_keys,
        use_frozen_mask=True,
    )
    without_mask = run_single_lake_service(
        series_df,
        hylak_id=101,
        config=config,
        frozen_year_months=frozen_keys,
        use_frozen_mask=False,
    )

    assert len(without_mask.labels_df) == 36
    assert len(with_mask.labels_df) == 33
