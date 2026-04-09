from argparse import Namespace
import importlib.util
from pathlib import Path

import pandas as pd

RUNNER_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_monthly_anomaly_transition.py"
SPEC = importlib.util.spec_from_file_location("run_monthly_anomaly_transition", RUNNER_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
run = MODULE.run


def test_runner_filters_csv_rows_by_hylak_id(tmp_path: Path) -> None:
    csv_path = tmp_path / "series.csv"
    rows = []
    for year, offset in ((2000, -10.0), (2001, 0.0), (2002, 10.0)):
        for month in range(1, 13):
            rows.append(
                {
                    "hylak_id": 101,
                    "year": year,
                    "month": month,
                    "water_area": 100.0 + month + offset,
                }
            )
            rows.append(
                {
                    "hylak_id": 202,
                    "year": year,
                    "month": month,
                    "water_area": 300.0 + month + offset,
                }
            )
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    output_root = tmp_path / "output"
    args = Namespace(
        hylak_id=101,
        csv=csv_path,
        frozen_csv=None,
        output_root=output_root,
        min_valid_per_month=3,
        min_valid_observations=36,
        no_plots=True,
    )

    outputs = run(args)

    labels_df = pd.read_csv(outputs["lake_dir"] / "month_labels.csv")
    assert set(labels_df["hylak_id"]) == {101}
