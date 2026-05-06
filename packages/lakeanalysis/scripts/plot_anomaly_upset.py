"""Plot UpSet diagram of anomaly set intersections.

Reads anomaly_flags from area_anomalies (postgres or parquet backend),
decodes into boolean columns, and produces an UpSet plot.

Usage:
    uv run python scripts/plot_anomaly_upset.py
    uv run python scripts/plot_anomaly_upset.py --output-dir data/figures/upset
    uv run python scripts/plot_anomaly_upset.py --min-size 5
    uv run python scripts/plot_anomaly_upset.py --limit 5000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakeanalysis.logger import Logger
from lakeanalysis.quality.filters import decode_anomaly_flags
from lakeviz.quality import plot_anomaly_upset
from lakeviz.layout import save

log = logging.getLogger(__name__)

_SET_COLS = ["is_median_zero", "is_flat_or_pv", "is_area_mismatch"]

_FLAG_TO_SET = {
    "median_zero": "is_median_zero",
    "flat": "is_flat",
    "area_ratio": "is_area_ratio",
    "outside_range": "is_outside_range",
    "pv": "is_pv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot UpSet diagram of anomaly set intersections."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/figures/upset",
        metavar="DIR",
        help="Output directory (default: data/figures/upset).",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=0,
        metavar="N",
        help="Minimum intersection size to display (default: 0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit number of lakes (for testing).",
    )
    return parser.parse_args()


def _decode_flags_df(df: pd.DataFrame) -> pd.DataFrame:
    """Decode anomaly_flags column into merged boolean columns."""
    decoded = df["anomaly_flags"].apply(lambda f: decode_anomaly_flags(int(f)))
    flags_df = pd.DataFrame(decoded.tolist(), index=df.index)

    result = pd.DataFrame({"hylak_id": df["hylak_id"].astype(int)})
    for flag_name, set_col in _FLAG_TO_SET.items():
        result[set_col] = flags_df.get(flag_name, False)

    result["is_flat_or_pv"] = result["is_flat"] | result["is_pv"]
    result["is_area_mismatch"] = result["is_area_ratio"] | result["is_outside_range"]

    result = result[["hylak_id"] + _SET_COLS]
    return result


def _load_flags_from_postgres(limit: int | None = None) -> pd.DataFrame:
    """Read area_anomalies from PostgreSQL and decode anomaly_flags."""
    from lakesource.postgres import series_db

    limit_sql = f"LIMIT {limit}" if limit else ""
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT hylak_id, anomaly_flags "
                f"FROM area_anomalies "
                f"ORDER BY hylak_id "
                f"{limit_sql}"
            )
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=colnames)
    return _decode_flags_df(df)


def _load_flags_from_parquet(
    source: SourceConfig,
    limit: int | None = None,
) -> pd.DataFrame:
    """Read area_anomalies from parquet and decode anomaly_flags."""
    from lakesource.parquet.client import DuckDBClient

    client = DuckDBClient(data_dir=source.data_dir)

    limit_sql = f"LIMIT {limit}" if limit else ""
    df = client.query_df(
        f"SELECT hylak_id, anomaly_flags "
        f"FROM area_anomalies "
        f"ORDER BY hylak_id "
        f"{limit_sql}"
    )

    if df.empty:
        return pd.DataFrame()

    return _decode_flags_df(df)


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    load_env()
    source = SourceConfig()

    if source.backend.value == "postgres":
        log.info("Using postgres backend")
        df = _load_flags_from_postgres(limit=args.limit)
    else:
        log.info("Using parquet backend: %s", source.data_dir)
        df = _load_flags_from_parquet(source, limit=args.limit)

    if df.empty:
        log.warning("No anomaly data found")
        return

    n_flagged = df[df[_SET_COLS].any(axis=1)].shape[0]
    log.info(
        "Loaded %d records, %d flagged (%.1f%%)",
        len(df), n_flagged, n_flagged / len(df) * 100,
    )
    for col in _SET_COLS:
        n = int(df[col].sum())
        log.info("  %s: %d (%.1f%%)", col, n, n / len(df) * 100)

    fig = plot_anomaly_upset(df, min_size=args.min_size)
    save(fig, output_dir / "anomaly_upset.png")

    log.info("UpSet plot saved to %s", output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_anomaly_upset")
    run(args)


if __name__ == "__main__":
    main()
