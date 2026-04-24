"""Run the quantile anomaly transition workflow for one lake."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakeanalysis.logger import Logger
from lakeanalysis.quantile import (
    QuantileServiceConfig,
    run_single_lake_service,
    save_lake_plots,
    save_summary_plots,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "quantile"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Detect monthly anomaly extremes and abrupt transitions for one lake."
    )
    parser.add_argument(
        "--hylak-id",
        type=int,
        default=None,
        help="Lake ID. Required for DB mode and optional in CSV mode.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Local CSV with columns year, month, water_area and optional hylak_id.",
    )
    parser.add_argument(
        "--frozen-csv",
        type=Path,
        default=None,
        help="Optional frozen-month CSV used only when --use-frozen-mask is set.",
    )
    parser.add_argument(
        "--use-frozen-mask",
        action="store_true",
        help="Explicitly exclude frozen months. Default is to keep them.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DATA_DIR,
        help="Root directory for CSV outputs and plots.",
    )
    parser.add_argument(
        "--min-valid-per-month",
        type=int,
        default=20,
        help="Minimum valid observations required for each calendar month.",
    )
    parser.add_argument(
        "--min-valid-observations",
        type=int,
        default=240,
        help="Minimum valid observations required overall.",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip figure generation.",
    )
    return parser.parse_args()


def _infer_hylak_id(series_df: pd.DataFrame, explicit_hylak_id: int | None) -> int | None:
    if explicit_hylak_id is not None:
        return explicit_hylak_id
    if "hylak_id" not in series_df.columns:
        return None
    unique_ids = pd.unique(series_df["hylak_id"].dropna())
    if len(unique_ids) != 1:
        raise ValueError("CSV input must contain exactly one hylak_id when --hylak-id is omitted")
    return int(unique_ids[0])


def _load_frozen_keys(path: Path | None) -> set[int]:
    if path is None:
        return set()
    frozen_df = pd.read_csv(path)
    if "year_month_key" in frozen_df.columns:
        return set(pd.to_numeric(frozen_df["year_month_key"], errors="raise").astype(int))
    if {"year", "month"}.issubset(frozen_df.columns):
        years = pd.to_numeric(frozen_df["year"], errors="raise").astype(int)
        months = pd.to_numeric(frozen_df["month"], errors="raise").astype(int)
        return set((years * 100 + months).tolist())
    raise ValueError("Frozen CSV must contain year_month_key or year/month columns")


def _load_series(args: argparse.Namespace) -> tuple[int | None, pd.DataFrame, set[int]]:
    use_frozen_mask = bool(getattr(args, "use_frozen_mask", False))
    frozen_keys = _load_frozen_keys(args.frozen_csv) if use_frozen_mask else set()
    if args.csv is not None:
        series_df = pd.read_csv(args.csv)
        hylak_id = _infer_hylak_id(series_df, args.hylak_id)
        if hylak_id is not None and "hylak_id" in series_df.columns:
            series_df = series_df.loc[series_df["hylak_id"] == hylak_id].copy()
            if series_df.empty:
                raise ValueError(f"No CSV rows found for hylak_id={hylak_id}")
        return hylak_id, series_df, frozen_keys

    if args.hylak_id is None:
        raise ValueError("Either --csv or --hylak-id must be provided")

    from lakesource.postgres import (  # local import keeps DB mode optional
        fetch_frozen_year_months_by_ids,
        fetch_lake_area_by_ids,
        series_db,
    )

    with series_db.connection_context() as conn:
        series_map = fetch_lake_area_by_ids(conn, [args.hylak_id])
        frozen_map = (
            fetch_frozen_year_months_by_ids(conn, [args.hylak_id])
            if use_frozen_mask
            else {}
        )

    series_df = series_map.get(args.hylak_id)
    if series_df is None:
        raise ValueError(f"No lake_area series found for hylak_id={args.hylak_id}")
    return args.hylak_id, series_df, frozen_map.get(args.hylak_id, set())


def _write_outputs(output_root: Path, result) -> Path:
    lake_name = "unknown" if result.hylak_id is None else str(result.hylak_id)
    lake_dir = output_root / "lakes" / lake_name
    lake_dir.mkdir(parents=True, exist_ok=True)
    result.climatology_df.to_csv(lake_dir / "climatology.csv", index=False)
    result.labels_df.to_csv(lake_dir / "month_labels.csv", index=False)
    result.extremes_df.to_csv(lake_dir / "extreme_events.csv", index=False)
    result.transitions_df.to_csv(lake_dir / "abrupt_transitions.csv", index=False)
    return lake_dir


def run(args: argparse.Namespace) -> dict[str, Path]:
    """Run the workflow and persist outputs."""
    hylak_id, series_df, frozen_year_months = _load_series(args)
    result = run_single_lake_service(
        series_df,
        hylak_id=hylak_id,
        config=QuantileServiceConfig(
            min_valid_per_month=args.min_valid_per_month,
            min_valid_observations=args.min_valid_observations,
        ),
        frozen_year_months=frozen_year_months,
        use_frozen_mask=bool(getattr(args, "use_frozen_mask", False)),
    )

    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    lake_dir = _write_outputs(output_root, result)

    outputs = {"lake_dir": lake_dir}
    if not args.no_plots:
        outputs.update(save_lake_plots(result.labels_df, result.transitions_df, output_root, hylak_id=hylak_id))
        outputs.update(save_summary_plots(result.transitions_df, output_root))
    return outputs


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    Logger("run_quantile")
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
    outputs = run(args)
    for key, path in outputs.items():
        log.info("%s: %s", key, path)


if __name__ == "__main__":
    main()
