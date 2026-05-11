"""CLI commands for PWM extreme & PWM-Hawkes analysis."""

import typer

app = typer.Typer(help="PWM extreme & PWM-Hawkes analysis", no_args_is_help=True)


@app.command()
def run() -> None:
    """Run batch PWM extreme computation."""
    typer.echo("pwm run — not yet migrated from scripts/run_pwm_extreme.py")
