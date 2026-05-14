"""Compare PWM-EVT Route A vs Route B on a sampled lake set."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider
from lakeanalysis.batch.calculator.pwm_hawkes import PWMHawkesCalculator
from lakeanalysis.batch.core import LakeTask
from lakeanalysis.logger import Logger
from lakesource.pwm.schema import PWMExtremeConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Route A and Route B PWM-EVT outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--sample-file", type=str, required=True)
    parser.add_argument("--output-csv", type=str, required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--decay-rate", type=float, default=1.0)
    parser.add_argument("--phi-method", choices=["identity", "log1p", "normalize"], default="identity")
    parser.add_argument("--min-events", type=int, default=10)
    parser.add_argument("--min-event-rate", type=float, default=0.01)
    parser.add_argument("--max-event-rate", type=float, default=0.30)
    parser.add_argument("--min-relative-amplitude", type=float, default=0.05)
    parser.add_argument("--min-median-severity", type=float, default=1.0)
    return parser.parse_args()


def _make_calculator(args: argparse.Namespace, route: str) -> PWMHawkesCalculator:
    return PWMHawkesCalculator(
        pwm_config=PWMExtremeConfig(),
        decay_rate=args.decay_rate,
        min_event_rate=args.min_event_rate,
        max_event_rate=args.max_event_rate,
        min_relative_amplitude=args.min_relative_amplitude,
        min_median_severity=args.min_median_severity,
        evt_route=route,
        phi_method=args.phi_method,
    )


def _extract_summary(result) -> dict:
    extra_rows = result.extra_rows_by_table.get("pwm_hawkes_route_summary", [])
    summary = extra_rows[0] if extra_rows else {}
    pipeline = result.core.summary
    return {
        "n_extreme_high": summary.get("n_extreme_high"),
        "n_extreme_low": summary.get("n_extreme_low"),
        "mean_strength_high": summary.get("mean_strength_high"),
        "mean_strength_low": summary.get("mean_strength_low"),
        "n_segments": summary.get("n_segments"),
        "n_transition_segments": summary.get("n_transition_segments"),
        "mean_segment_duration": summary.get("mean_segment_duration"),
        "hawkes_qc_pass": pipeline.get("qc_pass"),
        "hawkes_converged": pipeline.get("converged"),
        "lrt_p_d_to_w": pipeline.get("lrt_p_d_to_w"),
        "lrt_p_w_to_d": pipeline.get("lrt_p_w_to_d"),
        "n_events": pipeline.get("n_events"),
    }


def main() -> None:
    args = parse_args()
    Logger("pwm_evt_compare")

    sample_df = pd.read_parquet(args.sample_file)
    ids = sample_df["hylak_id"].astype(int).tolist()
    if args.limit is not None:
        ids = ids[: args.limit]

    provider = create_provider(SourceConfig())
    lake_map = provider.fetch_lake_area_by_ids(ids)
    frozen_map = provider.fetch_frozen_year_months_by_ids(ids)

    calc_a = _make_calculator(args, "A")
    calc_b = _make_calculator(args, "B")

    rows: list[dict] = []
    for hylak_id in ids:
        if hylak_id not in lake_map:
            continue
        task = LakeTask(
            hylak_id=hylak_id,
            series_df=lake_map[hylak_id],
            frozen_year_months=frozenset(frozen_map.get(hylak_id, set())),
        )
        result_a = calc_a.compute(task)
        result_b = calc_b.compute(task)
        summary_a = _extract_summary(result_a)
        summary_b = _extract_summary(result_b)
        row = {"hylak_id": hylak_id}
        for key, value in summary_a.items():
            row[f"{key}_A"] = value
        for key, value in summary_b.items():
            row[f"{key}_B"] = value
        rows.append(row)

    out_df = pd.DataFrame(rows)
    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
