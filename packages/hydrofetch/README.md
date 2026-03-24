# hydrofetch

**Hydrofetch** is a standalone CLI pipeline that:

1. Exports daily ERA5-Land raster images from Google Earth Engine to Google Drive.
2. Downloads the GeoTIFF files to local storage.
3. Deletes the temporary Drive artefacts.
4. Samples each raster at lake centroid locations to produce a compact lake-level forcing table.
5. Writes the output as Parquet (or CSV) files.

Each job is tracked with an explicit serialised state record so the pipeline can be
stopped and resumed at any stage without re-running completed steps.

---

## State machine

```
Hold → Export → Download → Cleanup → Sample → Write → Completed
                                                 ↑
                                           Failed (max retries)
```

- **Hold** – waits for a concurrency slot, then submits the GEE export task.
- **Export** – polls GEE until the task reaches `COMPLETED` or `FAILED`.
- **Download** – locates the file on Google Drive and streams it locally.
- **Cleanup** – deletes the Drive artefact and releases the concurrency slot.
- **Sample** – reads the GeoTIFF and samples it at lake centroid coordinates.
- **Write** – copies the sampled output to the configured output directory.

---

## Quick start

Run the following commands from the monorepo root.

### 1. Install

```bash
uv sync --package hydrofetch --group dev
```

### 2. Authenticate

```bash
# Authenticate with Earth Engine (once per machine)
earthengine authenticate

# First run of hydrofetch triggers a browser OAuth flow for Drive access
# and saves the token automatically.
```

### 3. Configure

```bash
cp packages/hydrofetch/.env.example packages/hydrofetch/.env
# Edit packages/hydrofetch/.env: set HYDROFETCH_GEE_PROJECT, HYDROFETCH_CREDENTIALS_FILE, etc.
```

### 4. Run

```bash
# Enqueue jobs for Jan 2020 and immediately start the monitor loop
uv run --package hydrofetch hydrofetch era5 \
    --start 2020-01-01 \
    --end   2020-02-01 \
    --region        region.geojson \
    --geometry      lake_centroids.csv \
    --output-dir    ./results \
    --run

# Check status without running
uv run --package hydrofetch hydrofetch status
uv run --package hydrofetch hydrofetch status --verbose

# Re-try a failed job
uv run --package hydrofetch hydrofetch retry --job-id era5_land_daily_image_20200115
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `HYDROFETCH_GEE_PROJECT` | *(required)* | GEE cloud project ID |
| `HYDROFETCH_CREDENTIALS_FILE` | *(required)* | Path to OAuth client-secrets JSON |
| `HYDROFETCH_TOKEN_FILE` | `~/.hydrofetch/token.json` | Saved OAuth token |
| `HYDROFETCH_DRIVE_FOLDER_NAME` | *(root)* | Drive folder for GEE exports |
| `HYDROFETCH_JOB_DIR` | `./hydrofetch_jobs` | Serialised job records |
| `HYDROFETCH_RAW_DIR` | `./hydrofetch_raw` | Downloaded GeoTIFF files |
| `HYDROFETCH_SAMPLE_DIR` | `./hydrofetch_sample` | Sampled Parquet files |
| `HYDROFETCH_MAX_CONCURRENT` | `5` | Max concurrent GEE tasks |
| `HYDROFETCH_POLL_INTERVAL` | `15` | Seconds between status polls |

---

## Geometry file format

The `--geometry` argument accepts:

- **CSV** with columns `hylak_id`, `lon`, `lat` (or a custom `--id-column`).
- **GeoJSON** FeatureCollection of Point features with the id in properties.

---

## Recovery after restart

Job records are written atomically as `<job_dir>/<job_id>.json`.  On restart the
runner:

1. Loads all non-terminal records.
2. Initialises the concurrency throttle with the count of already-active jobs.
3. For `Export`-state records, polls GEE using the stored `task_id`.
4. For `Download`-state records, checks whether the local file already exists.

All state handlers are idempotent – re-running them on an already-completed step
is safe.

---

## Project layout

```text
packages/hydrofetch/
├── src/hydrofetch/
│   ├── cli.py               # argparse CLI entry point
│   ├── config.py            # environment config loading
│   ├── catalog/             # JSON image-export spec loader
│   ├── drive/               # Google Drive v3 client
│   ├── export/              # GEE export task creation and naming
│   ├── gee/                 # Earth Engine initialisation
│   ├── jobs/                # serialisable job models and JSON store
│   ├── monitor/             # polling runner and concurrency throttle
│   ├── sample/              # local raster point sampling
│   ├── state_machine/       # per-state handlers
│   └── write/               # output writers (file, future: DB)
└── tests/
```
