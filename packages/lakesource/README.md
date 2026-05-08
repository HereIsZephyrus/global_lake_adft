# lakesource

Unified data access layer for lake analysis (PostgreSQL + Parquet backends).

## Core semantics

- `lake_info` defines the source lake universe and `hylak_id` range.
- `lake_area` stores time series keyed by `(hylak_id, year_month)`.
- `area_quality` and `area_anomalies` are quality-layer result tables and must not be used as source reads.

## Recommended initialization

`create_provider()` now performs one-time environment bootstrap via `ensure_env_loaded()`.
Callers that use the high-level factory do not need to call `load_env()` manually.

```python
from lakesource.provider import create_provider

provider = create_provider()
```

For explicit configuration:

```python
from pathlib import Path

from lakesource.config import Backend, SourceConfig
from lakesource.provider import create_provider

config = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data/parquet"))
provider = create_provider(config)
```

## Developer notes

- Source/done/quality SQL helper guidance and recommended indexes are documented in `../docs/lakesource-sql-refactor-notes.md`.
- `lakesource.postgres` is a lazy-export facade; exported symbols should always resolve and are covered by tests.
