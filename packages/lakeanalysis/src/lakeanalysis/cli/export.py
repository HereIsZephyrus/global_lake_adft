"""CLI commands for data export utilities."""

import typer

app = typer.Typer(help="Data export utilities", no_args_is_help=True)


@app.command()
def tables() -> None:
    """Export area tables to parquet."""
    typer.echo("export tables — not yet migrated from scripts/export_area_tables.py")
