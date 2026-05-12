"""``lake`` — unified CLI entry point for lakeanalysis pipelines.

Usage::

    lake entropy run --limit-id 100
    lake quality run --chunk-size 5000
    lake pwm run
    lake --help

Groups are organised by algorithm domain.  Each group is a separate
:class:`typer.Typer` instance registered here.
"""

from __future__ import annotations

from lakesource.env import ensure_env_loaded

ensure_env_loaded()

import typer

app = typer.Typer(
    name="lake_adft",
    help="Lake analysis unified CLI (lake_adft).",
    no_args_is_help=True,
)

# ── Domain subcommand groups ────────────────────────────────────────────────

from . import entropy as _entropy
from . import quality as _quality
from . import hawkes as _hawkes
from . import pwm as _pwm
from . import eot as _eot
from . import comparison as _comparison
from . import spatial as _spatial
from . import shift as _shift
from . import plot as _plot
from . import sync as _sync

app.add_typer(_entropy.app, name="entropy", help="Apportionment Entropy pipeline")
app.add_typer(_quality.app, name="quality", help="Data quality assessment & anomaly detection")
app.add_typer(_hawkes.app, name="hawkes", help="Hawkes process modelling")
app.add_typer(_pwm.app, name="pwm", help="PWM extreme & PWM-Hawkes analysis")
app.add_typer(_eot.app, name="eot", help="Extremes-over-threshold analysis")
app.add_typer(_comparison.app, name="comparison", help="Algorithm comparison")
app.add_typer(_spatial.app, name="spatial", help="Spatial / topological pipelines")
app.add_typer(_shift.app, name="shift", help="Structural shift detection")
app.add_typer(_plot.app, name="plot", help="Visualisation figures")
app.add_typer(_sync.app, name="sync", help="Parquet ↔ PostgreSQL data sync")
