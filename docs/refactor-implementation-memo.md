## Refactor Implementation Memo

This memo records the agreed implementation checklist for the next refactor pass.
The goal is to reduce duplicate-code hotspots without forcing broad architectural churn.

### Scope

This pass focuses on three approved tracks:

1. Introduce a shared parquet grid cache helper in `lakesource/grid_cache.py`
2. Consolidate shared monthly-series normalization under `lakeanalysis/decomposition`
3. Extract a small shared base for quantile/PWM extreme batch calculators

Explicit non-goals for this pass:

1. Do not merge `eot_hawkes` and `pwm_hawkes` event-generation logic
2. Do not do a large `eot.models` / `eot.preprocess` reorganization yet
3. Do not remove legacy monthly climatology support yet; only keep it clearly deprecated

### Track 1: Shared Grid Cache Helper

Objective:
Reduce repeated `_cached_or_compute` implementations in domain grid query modules.

Target files:

1. New: `packages/lakesource/src/lakesource/grid_cache.py`
2. Update: `packages/lakesource/src/lakesource/quantile/grid_queries.py`
3. Update: `packages/lakesource/src/lakesource/pwm/grid_queries.py`
4. Update: `packages/lakesource/src/lakesource/eot/grid_queries.py`
5. Update: `packages/lakesource/src/lakesource/comparison/grid_queries.py`

Implementation steps:

1. Create `cached_or_compute(cache_path, *, refresh, compute_fn, log)` helper
2. Keep the helper strictly DataFrame-cache oriented
3. Move common logging and `mkdir(..., parents=True, exist_ok=True)` behavior into the helper
4. Replace per-module `_cached_or_compute` clones with calls to the shared helper
5. Keep per-module dtype fixups and query-specific transforms local to each module

Verification:

1. Run focused tests for affected readers/providers if available
2. Re-run `uv run pylint` and confirm duplicate-code reports for `_cached_or_compute` clusters shrink

### Track 2: Shared Monthly-Series Normalization in Decomposition

Objective:
Move the common `year/month/water_area` normalization logic out of scattered modules and make decomposition the canonical owner of monthly series preparation.

Boundary decision:

1. Move only the shared monthly-series normalization helper into `decomposition`
2. Keep `assign_extreme_labels`, `extract_extreme_events`, and `detect_abrupt_transitions` under `extreme`
3. Treat those as post-decomposition extreme-processing logic, not decomposition itself

Target files:

1. New: `packages/lakeanalysis/src/lakeanalysis/decomposition/series.py` or `common.py`
2. Update: `packages/lakeanalysis/src/lakeanalysis/decomposition/stl_percentile.py`
3. Update: `packages/lakeanalysis/src/lakeanalysis/decomposition/monthly_climatology.py`
4. Update: `packages/lakeanalysis/src/lakeanalysis/extreme/compute.py`
5. Update: `packages/lakeanalysis/src/lakeanalysis/decomposition/__init__.py`

Implementation steps:

1. Extract the common logic currently represented by `validate_monthly_series(...)`
2. Define a single helper responsible for:
   - required-column validation
   - numeric coercion for `year`, `month`, `water_area`
   - month range validation
   - duplicate removal by `year/month`
   - sorting
   - generating `year_month_key` and `month_ordinal`
3. Update `STLPercentileMethod` to use the shared helper
4. Update `MonthlyClimatologyMethod` to use the shared helper
5. Update `extreme.compute.validate_monthly_series(...)` to delegate to the decomposition helper or replace it with a thin compatibility wrapper

Deprecation steps:

1. Keep `MonthlyClimatologyMethod` available
2. Preserve its runtime `DeprecationWarning`
3. Make its legacy/deprecated status explicit in `decomposition/__init__.py` docs and any nearby module docs
4. Do not delete `method="legacy"` support in this pass

Verification:

1. Re-run decomposition/extreme tests
2. Re-run pylint and confirm the series-normalization duplicate clusters shrink

### Track 3: Shared Extreme Batch Calculator Base

Objective:
Reduce duplicate code between quantile and PWM batch calculators without merging domain-specific row-shaping logic.

Target files:

1. New: `packages/lakeanalysis/src/lakeanalysis/batch/calculator/extreme_base.py`
2. Update: `packages/lakeanalysis/src/lakeanalysis/batch/calculator/quantile.py`
3. Update: `packages/lakeanalysis/src/lakeanalysis/batch/calculator/pwm.py`
4. Update: `packages/lakeanalysis/src/lakeanalysis/batch/calculator/__init__.py` if export changes are needed

Implementation constraints:

1. Keep the base class small
2. Share framework behavior only:
   - calling single-lake service
   - passing frozen months
   - standard run-status success/error row pattern
3. Leave domain-specific output tables injected by subclasses

Recommended structure:

1. Base class owns `compute(...)`
2. Subclass provides service config and service callable
3. Subclass provides `result_to_rows(...)`
4. Base class or injected helper provides a minimal shared error-to-run-status pattern if it stays clean

Verification:

1. Re-run tests for quantile and PWM batch calculators
2. Re-run pylint and confirm duplicate-code reports for `quantile.py` and `pwm.py` shrink

### EOT Follow-up Guidance

These are confirmed observations but postponed to a later pass.

#### `eot.models` and `eot.preprocess`

Confirmed issue:

1. `LocationModel` and `QuantileThresholdModel` duplicate the basis/trend/design-matrix skeleton
2. The missing abstraction is a shared basis-regression layer, not a file merge

Deferred plan:

1. Do not merge files yet
2. Later, extract a shared basis-regression skeleton
3. Then let `LocationModel` and `QuantileThresholdModel` own only their domain-specific behavior

#### `eot_hawkes` vs `pwm_hawkes`

Confirmed status:

1. Current abstraction is acceptable
2. Shared Hawkes orchestration is already factored well enough
3. Event-generation paths are intentionally different and should stay separate

Do not do in this pass:

1. Do not force a shared event-generation abstraction
2. Do not merge EOT and PWM Hawkes calculators beyond framework-level helpers already in place

### Execution Order

Recommended implementation order:

1. Track 1: `lakesource/grid_cache.py`
2. Track 2: decomposition monthly-series normalization helper
3. Track 3: extreme batch calculator base

Reasoning:

1. Track 1 is lowest risk and local to reader/cache code
2. Track 2 resolves an agreed boundary and removes repeated normalization logic
3. Track 3 is safe once shared preprocessing semantics are stable

### Validation Checklist

After each track:

1. Run focused pytest commands for touched modules
2. Run `uv run pylint packages/lakeanalysis/src/lakeanalysis packages/lakesource/src/lakesource packages/lakeviz/src/lakeviz`
3. Check whether the targeted duplicate-code cluster actually disappeared before moving to the next track
