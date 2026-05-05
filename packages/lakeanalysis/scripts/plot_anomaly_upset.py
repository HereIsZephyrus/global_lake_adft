"""Plot UpSet diagram of anomaly set intersections.

Reads area_anomalies data via PostgreSQL, decodes anomaly_flags bitmask,
and produces an UpSet plot showing overlaps between anomaly types.

Usage:
    uv run python scripts/plot_anomaly_upset.py
    uv run python scripts/plot_anomaly_upset.py --output-dir data/upset
    uv run python scripts/plot_anomaly_upset.py --min-size 5
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.postgres import series_db
from lakeanalysis.logger import Logger
from lakeanalysis.quality import decode_anomaly_flags
from lakeviz.quality import plot_anomaly_upset
from lakeviz.layout import save

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot UpSet diagram of anomaly set intersections."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/upset",
        metavar="DIR",
        help="Output directory (default: data/upset).",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=0,
        metavar="N",
        help="Minimum intersection size to display (default: 0).",
    )
    return parser.parse_args()


def load_anomaly_flags() -> pd.DataFrame:
    """Load area_anomalies and decode anomaly_flags into boolean columns."""
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags "
                "FROM area_anomalies "
                "ORDER BY hylak_id"
            )
            rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        hid = int(row[0])
        rs_mean = float(row[1]) if row[1] else 0.0
        rs_median = float(row[2]) if row[2] else 0.0
        atlas = float(row[3]) if row[3] else 0.0
        flags = int(row[4]) if row[4] is not None else 0

        decoded = decode_anomaly_flags(flags)
        records.append({
            "hylak_id": hid,
            "rs_area_mean": rs_mean,
            "rs_area_median": rs_median,
            "atlas_area": atlas,
            "anomaly_flags": flags,
            **decoded,
        })

    return pd.DataFrame(records)


def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_anomaly_flags()
    if df.empty:
        log.warning("No area_anomalies data found")
        return

    log.info("Loaded %d anomaly records", len(df))

    fig = plot_anomaly_upset(df, min_size=args.min_size)
    save(fig, output_dir / "anomaly_upset.png")

    log.info("UpSet plot saved to %s", output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_anomaly_upset")
    run(args)


if __name__ == "__main__":
    main()