"""CLI commands for spatial / topological pipelines."""

import typer

app = typer.Typer(help="Spatial / topological pipelines", no_args_is_help=True)


@app.command()
def pfaf() -> None:
    """Run Pfafstetter basin ID lookup."""
    typer.echo("spatial pfaf — not yet migrated from scripts/run_pfaf.py")
