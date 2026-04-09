# Monthly Anomaly Transition Spec

## 1. Purpose

This specification defines a non-EOT event-detection workflow for lake monthly
water-area time series.

The method identifies:

- monthly extreme-high events
- monthly extreme-low events
- abrupt transition events between extreme-low and extreme-high states

The method is intended for event detection and transition analysis. It is not an
EVT / return-level method and must not be interpreted as an EOT replacement for
tail-return inference.

## 2. Scope

This workflow applies to one lake at a time.

Input is a monthly water-area time series for a single `hylak_id`, optionally
with frozen-month marks. The workflow produces month-level labels, extreme-event
records, and abrupt-transition records.

## 3. Input Contract

### 3.1 Required Input Table

Per lake, the source series must provide:

- `year`
- `month`
- `water_area`

The expected time coverage is:

- 23 years
- 12 months per year
- 276 monthly observations per lake

### 3.2 Optional Auxiliary Input

Frozen-month flags may be provided as a set of `YYYYMM` keys.

These flags are used to exclude frozen months from climatology, quantile, and
transition calculations.

## 4. Time Cadence

The execution cadence of this method is fixed and must be treated as part of the
algorithm definition.

### 4.1 Observation Cadence

- The native observation cadence is monthly.
- Each observation represents one calendar month for one lake.

### 4.2 Climatology Cycle

- The seasonal cycle is annual.
- Climatology is estimated separately for each calendar month `m in {1..12}`.
- For each calendar month, the baseline sample size is 23 observations.

### 4.3 Event Evaluation Cycle

- Extreme labels are assigned at monthly resolution.
- Abrupt transitions are evaluated on adjacent monthly steps only:
  `t -> t+1`.

This means the first implementation must not use multi-month windows or
continuous-time interpolation.

## 5. Preprocessing

### 5.1 Series Validation

For each lake:

- require columns `year`, `month`, `water_area`
- require `month` in `1..12`
- sort by `year`, `month`
- coerce `water_area` to float

### 5.2 Frozen-Month Handling

If frozen-month marks are available:

- exclude frozen months from climatology estimation
- exclude frozen months from anomaly-quantile estimation
- exclude frozen months from abrupt-transition detection

If frozen-month marks are unavailable:

- use the full monthly series

### 5.3 Minimum Sample Requirement

The method assumes a nominal 23-year monthly record.

For implementation safety, each lake should satisfy:

- at least 20 valid observations for each calendar month after filtering
- at least 240 valid monthly observations overall after filtering

Lakes below this threshold should be marked as insufficient-data and skipped.

## 6. Monthly Climatology

For each lake and calendar month `m`, define the monthly climatology:

```text
mu_m = mean(x_{y,m}) over all valid years y
```

where:

- `x_{y,m}` is the monthly water area in year `y`, month `m`
- the mean is computed only over valid, non-frozen observations

This produces 12 monthly climatology values per lake.

## 7. Monthly Anomaly

For each valid observation `(y, m)`, define the monthly anomaly:

```text
a_{y,m} = x_{y,m} - mu_m
```

where:

- `x_{y,m}` is the observed water area
- `mu_m` is the climatological mean for calendar month `m`

The anomaly is therefore a month-of-year-adjusted deviation from the lake's
seasonal baseline.

## 8. Extreme Thresholds

For each lake, collect all valid monthly anomalies into one anomaly series:

```text
A = {a_t}
```

Then compute lake-specific empirical thresholds:

- `q_low = Q(0.10)`
- `q_high = Q(0.90)`

Important:

- thresholds are computed over the full valid anomaly series for the lake
- thresholds are not computed separately by month
- thresholds are lake-relative, not globally comparable across lakes

## 9. Extreme Event Definition

For each valid monthly anomaly `a_t`:

- if `a_t >= q_high`, label the month as `extreme_high`
- if `a_t <= q_low`, label the month as `extreme_low`
- otherwise label it as `normal`

By construction:

- high and low extremes are defined by the upper and lower anomaly deciles
- the method identifies relative extremes within each lake

## 10. Abrupt Transition Definition

Abrupt transitions are evaluated on adjacent valid months only.

For two consecutive valid observations `t` and `t+1`:

- if `t` is `extreme_low` and `t+1` is `extreme_high`, record `low_to_high`
- if `t` is `extreme_high` and `t+1` is `extreme_low`, record `high_to_low`

No other transition type is included in the first version.

This implies:

- only one-month sign-flip transitions are counted
- no two-month or longer recovery windows are allowed in v1
- frozen or invalid months break adjacency

## 11. Output Schema

### 11.1 Month-Level Label Table

One row per valid monthly observation:

- `hylak_id`
- `year`
- `month`
- `water_area`
- `monthly_climatology`
- `anomaly`
- `q_low`
- `q_high`
- `extreme_label`

Where `extreme_label` is one of:

- `extreme_low`
- `normal`
- `extreme_high`

### 11.2 Extreme Event Table

One row per extreme month:

- `hylak_id`
- `year`
- `month`
- `event_type`
- `water_area`
- `monthly_climatology`
- `anomaly`
- `threshold`

Where `event_type` is:

- `high`
- `low`

And `threshold` is:

- `q_high` for `high`
- `q_low` for `low`

### 11.3 Abrupt Transition Table

One row per abrupt transition:

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

Where `transition_type` is:

- `low_to_high`
- `high_to_low`

## 12. Visualisation Chain

This workflow must include a plotting layer built on the current project
matplotlib configuration.

### 12.1 Plot Configuration Contract

The plotting layer must reuse:

- `lakeanalysis.plot_config.setup_chinese_font()`

This keeps the visual output aligned with the current package standard:

- matplotlib-based figures
- CJK-safe font rendering
- correct minus-sign rendering

### 12.2 Visualisation Inputs

The visualisation layer consumes the outputs of this workflow:

- month-level label table
- extreme event table
- abrupt transition table

No additional modelling output is required.

### 12.3 Required Figures

The first implementation should provide at least the following figures.

#### A. Single-Lake Monthly Timeline

Purpose:

- show raw `water_area`
- show monthly climatology
- show extreme-high and extreme-low months
- show abrupt transitions on the time axis

Recommended layers:

- line: `water_area`
- reference line or seasonal baseline: `monthly_climatology`
- markers: `extreme_high`, `extreme_low`
- arrows or linked markers: `low_to_high`, `high_to_low`

#### B. Single-Lake Anomaly Timeline

Purpose:

- show anomaly series directly
- show `q_low` and `q_high`
- show where monthly anomalies cross thresholds

Recommended layers:

- line or bar: `anomaly`
- horizontal lines: `q_low`, `q_high`
- colored markers for high/low extremes

#### C. Transition Count Summary

Purpose:

- summarise the number of transitions by direction across all processed lakes

Recommended content:

- counts of `low_to_high`
- counts of `high_to_low`

#### D. Transition Seasonality Summary

Purpose:

- show whether abrupt transitions cluster in certain months

Recommended content:

- month-of-occurrence histogram for the transition destination month
  or the transition start month

### 12.4 Optional Figures

If time permits, add:

- anomaly distribution histogram per lake
- global histogram of lake-level `q_low` and `q_high`
- scatter of transition counts vs valid observation count
- scatter of transition counts vs anomaly variance

### 12.5 Output Directory Convention

The plotting layer should follow the existing package pattern and persist images
under the package-local data directory.

Recommended output root:

```text
packages/lakeanalysis/data/monthly_anomaly_transition/
```

Recommended structure:

```text
packages/lakeanalysis/data/monthly_anomaly_transition/
├── summary/
│   ├── transition_count_summary.png
│   └── transition_seasonality.png
└── lakes/
    └── <hylak_id>/
        ├── monthly_timeline.png
        └── anomaly_timeline.png
```

### 12.6 Rendering Rules

The plotting layer should follow these rendering rules:

- call `setup_chinese_font()` before figure generation
- save with `dpi=300`
- save with `bbox_inches="tight"`
- close figures after saving
- use deterministic file names

These rules match the current plotting usage in the package.

## 13. Interpretation Constraints

This method must be interpreted as:

- an anomaly-based empirical event detector
- a lake-relative thresholding scheme
- a monthly transition detector

This method must not be interpreted as:

- an EOT / NHPP tail model
- a return-level estimator
- a cross-lake absolute intensity measure

## 14. Recommended Implementation Order

1. Build a pure function that computes monthly climatology and anomalies for one lake.
2. Add a pure function that computes `q_low` and `q_high` from valid anomalies.
3. Add a pure function that assigns monthly extreme labels.
4. Add a pure function that scans adjacent months and emits abrupt transitions.
5. Add a plotting module that consumes the produced tables and writes figures with the current plot configuration.
6. Add a batch runner that applies the workflow lake by lake.
7. Add persistence only after the pure-function and plotting layers are stable.

## 15. Initial Validation Targets

The first implementation should validate:

- every processed lake has 12 climatology values
- anomaly mean within each calendar month is approximately zero
- high/low event counts are close to 10% / 10% of valid observations per lake
- abrupt transitions only occur between adjacent valid months
- frozen months are excluded consistently from all stages

For the visualisation layer, also validate:

- plots can be generated for one-lake and multi-lake summary modes
- all figures render correctly under the current font configuration
- saved file paths are deterministic and stable

## 16. Relation To Existing EOT/Hawkes Workflow

This workflow is an alternative event-definition layer.

It differs from EOT in that:

- it does not fit a non-stationary NHPP
- it does not estimate exceedance thresholds from EOT threshold regression
- it defines extremes through empirical anomaly quantiles

If needed later, the resulting high/low monthly event table can be mapped into a
two-type event stream for downstream Hawkes analysis.
