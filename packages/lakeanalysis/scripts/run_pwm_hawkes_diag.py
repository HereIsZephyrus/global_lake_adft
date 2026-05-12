"""Run PWM-Hawkes on a small sample and output diagnostic statistics.

Usage:
    uv run python scripts/run_pwm_hawkes_diag.py --limit-id 200
    uv run python scripts/run_pwm_hawkes_diag.py --limit-id 200 --skip-run
"""

from __future__ import annotations

import argparse
import logging
import os

os.environ.setdefault("NUMBA_NUM_THREADS", "1")

from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakeanalysis.batch import (
    Engine,
    RangeFilter,
    build_provider_batch_reader,
    build_provider_batch_writer,
)
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = ROOT / "data" / "pwm_hawkes" / "diag"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PWM-Hawkes small-sample diagnostic runner.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--limit-id", type=int, default=200)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--skip-run", action="store_true")
    parser.add_argument("--io-budget", type=int, default=4)
    parser.add_argument("--pwm-min-obs-per-month", type=int, default=2)
    parser.add_argument("--pwm-n-pwm", type=int, default=4)
    parser.add_argument("--decluster-run-length", type=int, default=1)
    parser.add_argument("--min-events", type=int, default=5)
    parser.add_argument("--min-event-rate", type=float, default=0.005)
    parser.add_argument("--max-event-rate", type=float, default=0.50)
    return parser.parse_args()


def _run_batch(
    limit_id: int, chunk_size: int, io_budget: int,
    pwm_min_obs: int, pwm_n: int, decluster_rl: int,
    min_events: int, min_rate: float, max_rate: float,
) -> None:
    """Execute pwm_hawkes batch for lakes 0..limit_id."""
    from lakesource.pwm_extreme.schema import PWMExtremeConfig

    source_config = SourceConfig()
    reader = build_provider_batch_reader(
        source_config,
        done_table="pwm_hawkes_run_status",
        done_requires_status=True,
    )
    writer = build_provider_batch_writer(
        source_config,         ensure_tables=["pwm_extreme", "pwm_hawkes"],
    )
    calculator = CalculatorFactory.create(
        "pwm_hawkes",
        pwm_config=PWMExtremeConfig(
            n_pwm=pwm_n, min_observations_per_month=pwm_min_obs,
        ),
        decluster_run_length=decluster_rl,
        min_events=min_events,
        min_event_rate=min_rate,
        max_event_rate=max_rate,
    )
    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        algorithm="pwm_hawkes",
        lake_filter=RangeFilter(start=0, end=limit_id),
        chunk_size=chunk_size,
        io_budget=io_budget,
    )
    report = engine.run()
    if report:
        log.info(
            "Batch done: %d success, %d error, %d skipped",
            report.success_lakes,
            report.error_lakes,
            report.skipped_lakes,
        )


def _fetch_diagnosis(limit_id: int) -> pd.DataFrame:
    """Read hawkes_results from parquet."""
    config = SourceConfig()
    data_dir = config.data_dir
    parquet_path = data_dir / "pwm_hawkes_results.parquet"
    if not parquet_path.exists():
        log.warning("pwm_hawkes_results.parquet not found at %s", parquet_path)
        return pd.DataFrame()
    df = pd.read_parquet(parquet_path)
    df = df[df["hylak_id"] < limit_id]
    return df


def _has_col(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns and df[col].notna().any()


def _print_summary(df: pd.DataFrame) -> None:
    """Print diagnostic summary to stdout."""
    total = len(df)
    qc_pass = df[df["qc_pass"].fillna(False).astype(bool)] if "qc_pass" in df.columns else pd.DataFrame()
    converged = df[df["converged"].fillna(False).astype(bool)] if "converged" in df.columns else pd.DataFrame()

    print("\n" + "=" * 60)
    print("PWM-Hawkes Diagnostic Summary")
    print("=" * 60)
    print(f"Total lakes in range: {total}")
    print(f"QC pass: {len(qc_pass)} ({len(qc_pass) / max(total, 1) * 100:.1f}%)")
    print(f"Converged: {len(converged)} ({len(converged) / max(total, 1) * 100:.1f}%)")
    print()

    if _has_col(df, "n_events"):
        evt = df["n_events"].dropna()
        if len(evt) > 0:
            print(
                "Events per lake (declustered): "
                f"median={evt.median():.0f}, "
                f"Q1={evt.quantile(0.25):.0f}, Q3={evt.quantile(0.75):.0f}, "
                f"min={evt.min():.0f}, max={evt.max():.0f}"
            )
    if _has_col(df, "n_dry_events") and _has_col(df, "n_wet_events"):
        dry = df["n_dry_events"].dropna()
        wet = df["n_wet_events"].dropna()
        if len(dry) > 0 and len(wet) > 0:
            print(
                f"Dry events: median={dry.median():.0f}, "
                f"IQR=({dry.quantile(0.25):.0f}, {dry.quantile(0.75):.0f})"
            )
            print(
                f"Wet events: median={wet.median():.0f}, "
                f"IQR=({wet.quantile(0.25):.0f}, {wet.quantile(0.75):.0f})"
            )
    print()

    if _has_col(df, "lrt_p_d_to_w"):
        d2w = df["lrt_p_d_to_w"].dropna()
        w2d = df["lrt_p_w_to_d"].dropna()
        if len(d2w) > 0:
            sig_d2w = (d2w < 0.05).sum()
            sig_w2d = (w2d < 0.05).sum() if len(w2d) > 0 else 0
            print(
                f"LRT significant (p<0.05): "
                f"D->W={sig_d2w}/{len(d2w)}, W->D={sig_w2d}/{len(w2d)}"
            )
    print()

    if _has_col(df, "spectral_radius"):
        sr = df["spectral_radius"].dropna()
        if len(sr) > 0:
            print(
                f"Spectral radius: median={sr.median():.4f}, "
                f"stable (<=1): {(sr <= 1).sum()}/{len(sr)}"
            )
    print()

    if _has_col(df, "mu_d"):
        mu_d = df["mu_d"].dropna()
        mu_w = df["mu_w"].dropna()
        if len(mu_d) > 0 and len(mu_w) > 0:
            print(f"mu_D: median={mu_d.median():.4f}")
            print(f"mu_W: median={mu_w.median():.4f}")
    print("=" * 60 + "\n")


def main() -> None:
    load_env()
    args = parse_args()
    Logger("run_pwm_hawkes_diag")

    if not args.skip_run:
        log.info("Running PWM-Hawkes batch on lakes 0..%d (pwm_min_obs=%d)",
                 args.limit_id, args.pwm_min_obs_per_month)
        _run_batch(
            args.limit_id, args.chunk_size, args.io_budget,
            args.pwm_min_obs_per_month, args.pwm_n_pwm,
            args.decluster_run_length,
            args.min_events, args.min_event_rate, args.max_event_rate,
        )

    log.info("Fetching hawkes_results for lakes 0..%d", args.limit_id)
    df = _fetch_diagnosis(args.limit_id)
    if df.empty:
        log.warning("No hawkes_results found. Run the batch first (omit --skip-run).")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = DATA_DIR / "diagnosis.csv"
    df.to_csv(csv_path, index=False)
    log.info("Diagnosis written to %s (%d rows)", csv_path, len(df))

    _print_summary(df)


if __name__ == "__main__":
    main()
