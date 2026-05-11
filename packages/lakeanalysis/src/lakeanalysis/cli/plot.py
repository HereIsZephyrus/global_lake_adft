"""CLI commands for visualisation figures."""

import typer

app = typer.Typer(help="Visualisation figure generation", no_args_is_help=True)


@app.command()
def upset() -> None:
    """Generate UpSet + donut anomaly intersection plot."""
    typer.echo("plot upset — not yet migrated from scripts/plot_anomaly_upset.py")
