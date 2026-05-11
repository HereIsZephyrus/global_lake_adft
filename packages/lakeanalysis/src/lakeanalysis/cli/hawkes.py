"""CLI commands for Hawkes process modelling."""

import typer

app = typer.Typer(help="Hawkes process modelling", no_args_is_help=True)


@app.command()
def run() -> None:
    """Run single-lake Hawkes fitting."""
    typer.echo("hawkes run — not yet migrated from scripts/run_hawkes.py")
