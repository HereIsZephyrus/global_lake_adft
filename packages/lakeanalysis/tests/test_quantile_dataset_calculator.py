from __future__ import annotations

import numpy as np

from lakeanalysis.batch import LakeDataset
from lakeanalysis.batch.calculator.quantile import QuantileCalculator


def test_quantile_calculator_runs_lake_dataset_batch() -> None:
    dataset = LakeDataset(
        hylak_ids=np.asarray([1, 2], dtype=np.int64),
        year_months=np.asarray([200001, 200002, 200003, 200004], dtype=np.int64),
        values=np.asarray(
            [
                [100.0, 101.0, 102.0, 103.0],
                [200.0, 201.0, 202.0, 203.0],
            ],
            dtype=float,
        ),
    )

    calculator = QuantileCalculator(method="legacy")

    rows_by_table, success_lakes, error_lakes = calculator.run_dataset(
        dataset, error_chunk=(10, 11)
    )

    assert success_lakes + error_lakes == 2
    assert "quantile_run_status" in rows_by_table
    assert len(rows_by_table["quantile_run_status"]) == 2
    assert {row["hylak_id"] for row in rows_by_table["quantile_run_status"]} == {1, 2}
