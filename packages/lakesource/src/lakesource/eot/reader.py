"""Read interface for EOT results metadata."""

from __future__ import annotations

import logging

import pandas as pd
from psycopg import sql as psql

from lakesource.config import Backend, SourceConfig
from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)


def _available_quantiles_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT DISTINCT tail, threshold_quantile
FROM   {eot_results}
ORDER  BY tail, threshold_quantile
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
    )


def fetch_available_quantiles(config: SourceConfig) -> pd.DataFrame:
    if config.backend == Backend.PARQUET:
        import duckdb
        con = duckdb.connect(":memory:")
        data_dir = str(config.data_dir)
        try:
            df = con.execute(
                f"SELECT DISTINCT tail, threshold_quantile FROM read_parquet('{data_dir}/eot_results.parquet') ORDER BY 1, 2"
            ).fetchdf()
        finally:
            con.close()
        return df
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(_available_quantiles_sql(config.t))
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=colnames)
