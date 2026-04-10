# Monthly Transition Refactor And Batch Execution Spec

## 1. Purpose

This specification defines the next implementation stage for the monthly
anomaly transition workflow.

The goal is to:

- refactor the current single-lake workflow into a clearer application pipeline
- add chunked DB batch execution
- persist monthly labels, extreme events, and abrupt transitions to DB tables
- generate global summary outputs without re-scanning raw DB result tables

## 2. Confirmed Business Contract

The following decisions are fixed for this implementation.

### 2.1 Input Population

- batch execution must process only lakes already accepted by `area_quality`
- the effective batch input contract is therefore aligned with
  `fetch_lake_area_chunk`

### 2.2 Frozen-Month Policy

- frozen months must not be excluded
- batch execution must not read or apply frozen-month masks

### 2.3 Error Policy

- single-lake failures must not stop the batch
- failures must be recorded and the chunk must continue

### 2.4 Output Policy

- results must be written to database tables
- one logical output type must map to one DB table

### 2.5 Visualisation Policy

- batch execution must generate global summary outputs only
- no per-lake plot generation is required in batch mode

### 2.6 Execution Policy

- execution must remain chunked
- execution must support resume / checkpoint semantics

## 3. Problem Statement

The current `monthly_transition` implementation already has a usable pure
compute layer, but the application boundary is still incomplete.

Current limitations:

- only a single-lake runner exists
- DB batch orchestration is missing
- DB persistence is missing
- summary generation is tied to transient in-memory outputs
- single and batch execution do not yet share one explicit workflow service

In the wider package, several `scripts/run_*.py` files also mix CLI parsing,
workflow decisions, DB reads, persistence, plotting, and checkpoint logic in
one place. This makes the entry layer harder to extend and increases the risk
that single and batch modes diverge in hidden ways.

## 4. Refactor Goal

This implementation must separate the monthly transition pipeline into:

1. pure compute logic
2. workflow / application orchestration
3. DB storage adapters
4. summary aggregation and plotting
5. thin CLI scripts

The single-lake and batch paths must share the same one-lake service logic.

## 5. Target Module Layout

The monthly transition package should be refactored toward the following
structure:

```text
packages/lakeanalysis/src/lakeanalysis/monthly_transition/
├── __init__.py
├── compute.py
├── service.py
├── batch.py
├── store.py
├── summary.py
├── plot.py
└── config.py
```

Module responsibilities:

- `compute.py`
  - pure functions only
  - no DB access
  - no filesystem writes
- `service.py`
  - single-lake application service
  - converts one monthly series into standard result objects
- `batch.py`
  - chunk orchestration
  - per-lake exception handling
  - chunk-level aggregation
- `store.py`
  - ensure-table helpers
  - bulk upsert helpers
  - checkpoint / run-status helpers
- `summary.py`
  - build cached summary tables from chunk outputs
  - write summary cache to local files
  - load cache for plotting without re-scanning raw result tables
- `plot.py`
  - plot constructors and save helpers
- `config.py`
  - typed config objects for batch and summary behaviour

## 6. DB Schema Contract

This workflow must persist to four tables.

### 6.1 `monthly_transition_labels`

One row per monthly observation retained by the workflow.

Required columns:

- `hylak_id`
- `year`
- `month`
- `water_area`
- `monthly_climatology`
- `anomaly`
- `q_low`
- `q_high`
- `extreme_label`
- `computed_at`

Recommended primary key:

- `(hylak_id, year, month)`

### 6.2 `monthly_transition_extremes`

One row per extreme month.

Required columns:

- `hylak_id`
- `year`
- `month`
- `event_type`
- `water_area`
- `monthly_climatology`
- `anomaly`
- `threshold`
- `computed_at`

Recommended primary key:

- `(hylak_id, year, month, event_type)`

### 6.3 `monthly_transition_abrupt_transitions`

One row per abrupt transition.

Required columns:

- `hylak_id`
- `from_year`
- `from_month`
- `to_year`
- `to_month`
- `transition_type`
- `from_anomaly`
- `to_anomaly`
- `from_label`
- `to_label`
- `computed_at`

Recommended primary key:

- `(hylak_id, from_year, from_month, to_year, to_month, transition_type)`

### 6.4 `monthly_transition_run_status`

This table is required for safe resume and error tracking.

Required columns:

- `hylak_id`
- `chunk_start`
- `chunk_end`
- `status`
- `error_message`
- `computed_at`

Where `status` is one of:

- `done`
- `error`

Recommended primary key:

- `(hylak_id)`

The status table is authoritative for whether a lake has already been processed
by the current workflow version.

## 7. Batch Input Contract

Batch mode must read lakes by chunk using the quality-filtered path.

Required behaviour:

- use `fetch_lake_area_chunk`
- do not load frozen-month data
- process one lake at a time inside each chunk
- keep lake-level failures isolated

Chunk completion must not be inferred from the three result tables separately.
Instead, completion must be determined from `monthly_transition_run_status`.

## 8. Single-Lake Service Contract

The one-lake application service must:

1. validate the monthly series
2. compute monthly climatology
3. compute anomalies
4. compute `q_low` and `q_high`
5. assign monthly extreme labels
6. extract extreme-event rows
7. detect abrupt transitions
8. return a standard result object suitable for both single and batch use

The service must not know:

- where input came from
- whether the caller is single or batch
- where results will be stored

## 9. Summary Cache Contract

Global summary outputs must be based on a local cache layer instead of repeated
full scans over the raw result tables.

### 9.1 Why The Cache Exists

Directly querying and aggregating the full raw labels / extremes / transitions
tables for every summary render can become expensive and operationally noisy.

The summary path therefore needs a local cache that is cheap to regenerate and
cheap to read.

### 9.2 Cache Scope

The summary cache should store only aggregated statistics needed by the global
plots and summary tables.

Recommended cached outputs:

- transition counts by direction
- transition counts by destination month
- lake-level transition counts
- lake-level extreme counts
- batch run metadata summary

### 9.3 Cache Location

Recommended output root:

```text
packages/lakeanalysis/data/monthly_anomaly_transition/summary_cache/
```

Recommended cache files:

```text
transition_counts.csv
transition_seasonality.csv
lake_transition_counts.csv
lake_extreme_counts.csv
run_metadata.json
```

### 9.4 Cache Build Strategy

The cache may be rebuilt after each chunk or once at the end of the batch run.

The first implementation should prefer:

- write raw DB results during chunk processing
- rebuild the local summary cache at the end of a successful run
- then generate summary figures from the cache

### 9.5 Cache Plot Contract

Global summary plots must read from the local cache when available.

This keeps plotting independent from heavy raw-table aggregation and provides a
stable artifact boundary for later reporting scripts.

## 10. Batch Visualisation Contract

Batch mode must generate only global summary outputs.

Required figures:

- transition count summary
- transition seasonality summary

Optional later figures:

- lake-level transition count histogram
- lake-level extreme count histogram

Rendering rules remain unchanged:

- reuse `setup_chinese_font()`
- save with `dpi=300`
- save with `bbox_inches="tight"`
- close figures after saving

## 11. CLI Refactor Contract

The script layer should become thin and explicit.

Recommended direction:

```text
packages/lakeanalysis/scripts/run_monthly_transition.py
```

Recommended subcommands:

- `single`
- `batch`
- `summary`

If the first implementation keeps separate scripts for delivery speed, the
script bodies must still delegate to shared package modules rather than embed
business logic directly.

## 12. Execution Order

The implementation must follow this order.

### Phase A. Spec-Aligned Refactor

1. keep `compute.py` as the pure core
2. add service / batch / store / summary layers
3. refactor the current single-run script to use the shared service

### Phase B. DB Batch Support

1. create DB ensure/upsert helpers
2. create chunked batch runner
3. create run-status / resume logic
4. create global summary-cache builder
5. create global summary plotting from cache

### Phase C. Verification

1. run unit tests for compute / summary / store helpers
2. run a batch smoke test on a small synthetic or mocked dataset
3. verify single-run path still works

### Phase D. Full Execution Readiness

1. confirm DB env configuration
2. confirm chunk size and trial run parameters
3. run a small live DB smoke batch
4. only then start the full execution

## 13. Required Tests

The implementation should add or update tests for:

- single-lake service outputs
- DB-row shaping helpers
- batch lake-error isolation
- status-table checkpoint logic
- summary-cache build logic
- summary plotting from cache
- thin CLI smoke paths where practical

Tests must not require live DB credentials.

## 14. Completion Criteria

This task is complete when all of the following are true:

- single and batch paths share one single-lake service
- DB storage helpers exist for all four monthly transition tables
- batch execution uses chunked processing with resume semantics
- single-lake failures are recorded and do not stop the batch
- global summary outputs are generated from a local cache layer
- tests covering the new workflow pass locally
- the package is ready for a small live DB smoke run before full execution
