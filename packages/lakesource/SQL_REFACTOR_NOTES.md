# lakesource SQL refactor notes

## Source semantics

- `lake_info` defines the source lake universe and `hylak_id` upper bound.
- `lake_area` stores source time series keyed by `(hylak_id, year_month)`.
- `area_quality` and `area_anomalies` are quality-layer results and must not define source reads.

## Helper split

- Source helpers should read from `lake_info`.
- Quality helpers should read from `area_quality` / `area_anomalies`.
- Done/status helpers should read from `*_run_status` or the explicit quality union.

## Recommended PostgreSQL indexes

```sql
CREATE INDEX IF NOT EXISTS idx_quantile_run_status_done
ON quantile_run_status (workflow_version, status, hylak_id);

CREATE INDEX IF NOT EXISTS idx_pwm_extreme_run_status_done
ON pwm_extreme_run_status (workflow_version, status, hylak_id);

CREATE INDEX IF NOT EXISTS idx_comparison_run_status_done
ON comparison_run_status (workflow_version, status, hylak_id);

CREATE INDEX IF NOT EXISTS idx_eot_run_status_done
ON eot_run_status (workflow_version, status, hylak_id);

CREATE INDEX IF NOT EXISTS idx_area_quality_hylak_id
ON area_quality (hylak_id);

CREATE INDEX IF NOT EXISTS idx_area_anomalies_hylak_id
ON area_anomalies (hylak_id);

CREATE INDEX IF NOT EXISTS idx_lake_area_hylak_year_month
ON lake_area (hylak_id, year_month);
```

## Query patterns

- Use `lake_info` range scans for source-id counting and discovery.
- Use `UNION` over `area_quality` and `area_anomalies` for quality done checks.
- Use `status = 'done' AND workflow_version = ...` consistently in `*_run_status` range queries.
- Prefer `EXISTS` / `NOT EXISTS` for pending checks over unnecessary aggregate counts.
