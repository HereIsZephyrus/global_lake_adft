"""Shared filter registry and output path helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lakeanalysis.batch import IdSetFilter
from lakesource.config import OutputFilter, SourceConfig


@dataclass(frozen=True)
class FilterSpec:
    """Specification for an output filter with naming and description."""

    name: OutputFilter
    output_name: str
    description: str


FILTER_SPECS: dict[OutputFilter, FilterSpec] = {
    OutputFilter.FULL: FilterSpec(
        name=OutputFilter.FULL,
        output_name="full",
        description="No additional filtering.",
    ),
    OutputFilter.GT10: FilterSpec(
        name=OutputFilter.GT10,
        output_name="gt10",
        description="atlas_area > 10.",
    ),
    OutputFilter.NO_PWM_ERR: FilterSpec(
        name=OutputFilter.NO_PWM_ERR,
        output_name="no_pwm_err",
        description="Exclude PWM error lakes.",
    ),
}


def get_filter_spec(name: OutputFilter | str | None) -> FilterSpec:
    """Resolve an output filter name to its specification."""
    resolved = OutputFilter(name or OutputFilter.FULL.value)
    return FILTER_SPECS[resolved]


def output_root(source: SourceConfig | None = None) -> Path:
    """Return the configured output directory root."""
    config = source or SourceConfig()
    assert config.output_dir is not None
    return config.output_dir


def output_path(*parts: str, source: SourceConfig | None = None) -> Path:
    """Build an output path by joining parts under the configured root."""
    return output_root(source).joinpath(*parts)


def build_lake_filter(source: SourceConfig | None = None):
    """Build a LakeFilter from the configured output filter spec."""
    config = source or SourceConfig()
    spec = get_filter_spec(config.output_filter)
    if spec.name == OutputFilter.FULL:
        return None

    import duckdb

    data_dir = config.data_dir
    if data_dir is None:
        raise ValueError("data_dir is required to build output filters")

    con = duckdb.connect()
    try:
        if spec.name == OutputFilter.GT10:
            rows = con.execute(
                f"SELECT DISTINCT hylak_id FROM read_parquet('{data_dir}/lake_info/*.parquet') WHERE lake_area > 10"
            ).fetchall()
            return IdSetFilter({int(row[0]) for row in rows})

        if spec.name == OutputFilter.NO_PWM_ERR:
            error_file = Path(data_dir) / "pwm_error_lakes.parquet"
            if not error_file.exists():
                return None
            error_rows = con.execute(
                f"SELECT DISTINCT hylak_id FROM read_parquet('{error_file}')"
            ).fetchall()
            error_ids = {int(row[0]) for row in error_rows}
            lake_rows = con.execute(
                f"SELECT DISTINCT hylak_id FROM read_parquet('{data_dir}/lake_info/*.parquet')"
            ).fetchall()
            keep_ids = {int(row[0]) for row in lake_rows} - error_ids
            return IdSetFilter(keep_ids)
    finally:
        con.close()

    return None
