"""CLI commands for extremes-over-threshold analysis."""

import typer

app = typer.Typer(help="Extremes-over-threshold analysis", no_args_is_help=True)


@app.command()
def run() -> None:
    """Run batch EOT computation."""
    typer.echo("eot run — not yet migrated from scripts/run_eot.py")
