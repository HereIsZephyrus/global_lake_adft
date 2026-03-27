# hydrofetch

**Hydrofetch** is a standalone CLI pipeline that:

1. Exports daily ERA5-Land raster images from Google Earth Engine to Google Drive.
2. Downloads the GeoTIFF files to local storage.
3. Deletes the temporary Drive artefacts.
4. Samples each raster against lake polygon geometries to produce a compact lake-level forcing table.
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
- **Sample** – reads the GeoTIFF and computes polygon-based zonal means for each lake.
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
    --tile-manifest continents.json \
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
| `ALTAS_DB` | *(optional)* | Atlas database used by the smoke fixture generator |
| `HYDROFETCH_DB` | *(required for db sink)* | Hydrofetch target database; also reused as SERIES_DB for smoke fixtures |
| `HYDROFETCH_DB_USER` | *(required for db sink)* | PostgreSQL user |
| `HYDROFETCH_DB_PASSWORD` | *(required for db sink)* | PostgreSQL password |
| `HYDROFETCH_DB_HOST` | `localhost` | PostgreSQL host |
| `HYDROFETCH_DB_PORT` | `5432` | PostgreSQL port |

---

## Geometry file format

The `--geometry` argument accepts GeoJSON FeatureCollections of Polygon / MultiPolygon
features with `hylak_id` in properties. Legacy point/CSV inputs are no longer the
primary sampling path.

For tiled daily runs, prefer `--tile-manifest`, where each tile entry declares:

- `tile_id`
- `geometry_path`
- `region_path` (optional; omit to export the full image footprint)

## Smoke test

Run the smoke script from `packages/hydrofetch/scripts/smoke/run_smoke.sh`.
It first generates one source-of-truth lake dataset from:

- `area_quality` in `HYDROFETCH_DB`
- `LakeATLAS_v10_pol` in `ALTAS_DB`

From that same lake set it derives:

- `smoke_lakes_polygons.geojson` as the source-of-truth lake set
- `tiles/<tile_id>_lakes.geojson` for per-tile zonal sampling
- `tiles/<tile_id>_region.geojson` for per-tile clipped export
- `smoke_manifest.json` referencing the derived tile files

By default it uses the first 10 `hylak_id` values and regenerates these
artifacts on every run. The smoke run therefore exercises the same manifest /
tile expansion logic as regular tiled production runs.

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
│   ├── sample/              # local raster zonal sampling
│   ├── state_machine/       # per-state handlers
│   └── write/               # output writers (file, future: DB)
└── tests/
```
