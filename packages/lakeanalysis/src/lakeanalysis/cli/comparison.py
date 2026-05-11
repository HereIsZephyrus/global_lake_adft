"""CLI commands for algorithm comparison."""

import typer

app = typer.Typer(help="Algorithm comparison pipelines", no_args_is_help=True)


@app.command()
def run() -> None:
    """Run algorithm comparison batch."""
    typer.echo("comparison run — not yet migrated from scripts/run_algorithm_comparison.py")
