"""CLI commands for structural shift detection."""

from __future__ import annotations

import typer

from ._common import ChunkSizeOpt, LimitIdOpt, setup_logging

app = typer.Typer(help="Structural shift detection", no_args_is_help=True)


@app.command()
def compute(
    limit_id: LimitIdOpt = None,
    chunk_size: ChunkSizeOpt = 10_000,
    reset: bool = typer.Option(False, "--reset", help="Truncate existing data before run"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Skip DB writes during sync step"),
    skip_compute: bool = typer.Option(False, "--skip-compute", help="Skip batch computation, use existing parquet"),
    skip_sync: bool = typer.Option(False, "--skip-sync", help="Skip sync to anomalies"),
) -> None:
    """Compute structural shift labels via batch engine."""
    setup_logging("shift-labels")
    from lakeanalysis.batch import (  # pylint: disable=no-name-in-module
        Engine,
        RangeFilter,
        build_provider_batch_reader,
        build_provider_batch_writer,
    )
    from lakeanalysis.quality.shift_labels_calculator import ShiftLabelsCalculator
    from lakeanalysis.quality.shift_labels_runner import upsert_shift_labels_from_parquet, sync_shift_to_anomalies
    from lakesource.config import SourceConfig
    from lakesource.provider.factory import create_provider

    config = SourceConfig()
    output_parquet = config.data_dir / "area_shift_labels.parquet"

    if not skip_compute:
        provider = create_provider(config)
        if reset:
            provider.truncate_table("area_shift_labels")
            output_parquet.unlink(missing_ok=True)

        reader = build_provider_batch_reader(config, done_table="area_shift_labels")
        writer = build_provider_batch_writer(config)
        calculator = ShiftLabelsCalculator()
        lake_filter = RangeFilter(end=limit_id) if limit_id else None

        engine = Engine(reader=reader, writer=writer, calculator=calculator,
                        algorithm="shift_labels", lake_filter=lake_filter, chunk_size=chunk_size)
        engine.run()

    if not skip_sync:
        provider = create_provider(config)
        n_upserted = upsert_shift_labels_from_parquet(output_parquet, provider, chunk_size=chunk_size)
        typer.echo(f"Upserted {n_upserted} shift labels")
        sync_shift_to_anomalies(provider, dry_run=dry_run)


@app.command()
def sample(
    limit_id: LimitIdOpt = None,
    chunk_size: ChunkSizeOpt = 10_000,
    top_n: int = typer.Option(100, "--top-n", help="Top N candidates to export"),
    p_value_thresh: float = typer.Option(0.05, "--p-thresh", help="Pettitt p-value threshold"),
    smooth_window: int = typer.Option(12, "--smooth-window", help="Rolling smooth window in months"),
    output_dir: str = typer.Option(None, help="Output directory (default: data/comparison/shift_candidates)"),
) -> None:
    """Scan and export top structural shift candidate lakes."""
    setup_logging("shift-sample")
    from pathlib import Path

    from lakeanalysis.quality.filters.shift import ShiftConfig, ShiftFilter
    from lakeanalysis.quality.filters import LakeContext
    from lakesource.config import SourceConfig
    from lakesource.postgres import fetch_lake_area_chunk, series_db  # pylint: disable=no-name-in-module
    import pandas as pd

    if output_dir is None:
        config = SourceConfig()
        out = config.data_dir.parent / "comparison" / "shift_candidates"
    else:
        out = Path(output_dir)

    sf = ShiftFilter(ShiftConfig(p_value_thresh=p_value_thresh, smooth_window=smooth_window))
    out.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    with series_db.connection_context() as conn:
        for start in range(0, limit_id or 1_400_000, chunk_size):
            end = min(start + chunk_size, limit_id or 1_400_000)
            lake_map = fetch_lake_area_chunk(conn, start, end)
            for hid, df in lake_map.items():
                ctx = LakeContext(df=df, df_no_frozen=df, rs_area_median=0.0, rs_area_mean=0.0,
                                   rs_area_quantile=0.0, atlas_area=0.0)
                flag = sf.classify(ctx)
                all_rows.append({"hylak_id": hid, "label": "degraded" if flag.is_anomaly else "stable",
                                  "p_value": flag.detail.get("pettitt_p", None)})

    df = pd.DataFrame(all_rows)
    top = df[df["label"] == "degraded"].sort_values("p_value").head(top_n)
    df.to_parquet(out / "shift_candidates_all.parquet", index=False)
    top.to_csv(out / "shift_candidates_top.csv", index=False)
    typer.echo(f"Scanned {len(df)} lakes, {len(top)} degraded candidates in top {top_n}")


@app.command()
def inspect(
    hylak_ids: list[int] = typer.Option([170137, 170009], "--hylak-id", help="Lake IDs to inspect"),
    output_dir: str = typer.Option(None, help="Output directory for plots (default: figure)"),
    p_value_thresh: float = typer.Option(0.05, "--p-thresh"),
    smooth_window: int = typer.Option(12, "--smooth-window"),
) -> None:
    """Inspect and plot structural shift for specific lakes."""
    setup_logging("shift-inspect")
    from pathlib import Path

    from lakeanalysis.quality.filters.shift import ShiftConfig, ShiftFilter
    from lakeanalysis.quality.filters import LakeContext
    from lakesource.config import SourceConfig
    from lakesource.postgres import (  # pylint: disable=no-name-in-module
        fetch_lake_area_by_ids,
        fetch_frozen_year_months_by_ids,
        series_db,
    )
    import matplotlib.pyplot as plt

    if output_dir is None:
        config = SourceConfig()
        out = config.data_dir.parent / "figures" / "quality"
    else:
        out = Path(output_dir)

    sf = ShiftFilter(ShiftConfig(p_value_thresh=p_value_thresh, smooth_window=smooth_window))
    out.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        lake_map = fetch_lake_area_by_ids(conn, hylak_ids)
        frozen_map = fetch_frozen_year_months_by_ids(conn, hylak_ids)

    for hid in hylak_ids:
        df = lake_map.get(hid)
        if df is None:
            typer.echo(f"Lake {hid} not found")
            continue
        frozen = set(frozen_map.get(hid, []))
        df_no_frozen = df[~((df["year"].astype(int) * 100 + df["month"].astype(int)).isin(frozen))].copy()
        ctx = LakeContext(df=df, df_no_frozen=df_no_frozen, rs_area_median=0.0, rs_area_mean=0.0,
                           rs_area_quantile=0.0, atlas_area=0.0)
        flag = sf.classify(ctx)
        typer.echo(f"Lake {hid}: is_degraded={flag.is_anomaly}")
        typer.echo(f"  {flag.detail}")

        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(df["water_area"].to_numpy())
        ax.set_title(f"Lake {hid}")
        fig.savefig(out / f"shift_lake_{hid}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)
    typer.echo(f"Saved to {out}")
