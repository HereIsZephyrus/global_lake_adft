"""Export area_quality and area_anomalies from PostgreSQL to parquet."""

from __future__ import annotations

import argparse
from pathlib import Path

from lakesource.postgres import series_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export area_quality and area_anomalies to parquet.")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/parquet",
        help="Output directory for parquet files.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=200000,
        help="Chunk size for hylak_id ranges.",
    )
    parser.add_argument(
        "--max-id",
        type=int,
        default=1400000,
        help="Maximum hylak_id to export.",
    )
    return parser.parse_args()


def export_table_to_parquet(
    table_name: str,
    output_dir: Path,
    chunk_size: int,
    max_id: int,
) -> int:
    import pandas as pd

    table_path = output_dir / table_name
    table_path.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for start in range(0, max_id + 1, chunk_size):
        end = start + chunk_size
        with series_db.connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {table_name} WHERE hylak_id >= %s AND hylak_id < %s ORDER BY hylak_id",
                    (start, end),
                )
                rows = cur.fetchall()
                columns = [desc.name for desc in cur.description]

        if not rows:
            continue

        df = pd.DataFrame(rows, columns=columns)
        chunk_name = f"{start:06d}.parquet"
        chunk_file = table_path / chunk_name
        df.to_parquet(chunk_file, index=False)
        print(f"  {table_name}/{chunk_name}: {len(df)} rows")
        total_rows += len(df)

    return total_rows


def main() -> None:
    args = parse_args()
    output_dir = Path(args.data_dir)

    print(f"Exporting to {output_dir}")

    print("\nExporting area_quality...")
    aq_count = export_table_to_parquet("area_quality", output_dir, args.chunk_size, args.max_id)
    print(f"  Total: {aq_count} rows")

    print("\nExporting area_anomalies...")
    aa_count = export_table_to_parquet("area_anomalies", output_dir, args.chunk_size, args.max_id)
    print(f"  Total: {aa_count} rows")


if __name__ == "__main__":
    main()
