"""CLI commands for structural shift detection."""

import typer

app = typer.Typer(help="Structural shift detection", no_args_is_help=True)


@app.command()
def compute() -> None:
    """Compute structural shift labels."""
    typer.echo("shift compute — not yet migrated from scripts/compute_shift_labels.py")
