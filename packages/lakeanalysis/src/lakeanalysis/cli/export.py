"""CLI commands for data export utilities."""

from __future__ import annotations

import typer

from ._common import setup_logging

app = typer.Typer(help="Data export utilities", no_args_is_help=True)


@app.command()
def tables(
    data_dir: str = typer.Option("data/parquet", help="Output directory for parquet files"),
    chunk_size: int = typer.Option(200_000, "--chunk-size", help="Chunk size for hylak_id ranges"),
    max_id: int = typer.Option(1_400_000, "--max-id", help="Maximum hylak_id to export"),
) -> None:
    """Export area_quality and area_anomalies tables to parquet."""
    setup_logging("export-tables")
    from lakesource.postgres import series_db
    import pandas as pd
    from pathlib import Path

    out = Path(data_dir)

    for table in ("area_quality", "area_anomalies"):
        (out / table).mkdir(parents=True, exist_ok=True)
        with series_db.connection_context() as conn:
            for start in range(0, max_id, chunk_size):
                end = min(start + chunk_size, max_id)
                df = pd.read_sql(
                    f"SELECT * FROM {table} WHERE hylak_id >= {start} AND hylak_id < {end}",
                    conn,
                )
                if df.empty:
                    continue
                df.to_parquet(out / table / f"{start:08d}.parquet", index=False)
                typer.echo(f"  {table}: chunk {start}-{end} ({len(df)} rows)")

    typer.echo(f"Done. Data exported to {out}")
