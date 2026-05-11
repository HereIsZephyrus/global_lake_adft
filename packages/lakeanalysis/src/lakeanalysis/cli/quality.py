"""CLI commands for data quality assessment & anomaly detection."""

import typer

app = typer.Typer(help="Data quality assessment & anomaly detection", no_args_is_help=True)


@app.command()
def run() -> None:
    """Run quality pipeline (filters, anomaly classification)."""
    from ._common import setup_logging
    setup_logging("quality")
    typer.echo("quality run — not yet migrated from scripts/run_quality.py")
