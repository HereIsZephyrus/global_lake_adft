#!/usr/bin/env bash
set -euo pipefail

N_PROC=8
MAX_ID=1427543
DATA_DIR="/mnt/repo/lake/global_lake_adft/data"
SCRIPT="/mnt/repo/lake/global_lake_adft/packages/lakeanalysis/scripts/run_interpolation_detect.py"
OUTPUT_DIR="${DATA_DIR}/interpolation"

CHUNK=$((MAX_ID / N_PROC + 1))

pids=()
for i in $(seq 0 $((N_PROC - 1))); do
    start=$((i * CHUNK))
    end=$(( (i + 1) * CHUNK ))
    if [ $end -gt $MAX_ID ]; then
        end=$MAX_ID
    fi
    suffix="_$(printf '%02d' $i)"
    echo "Launching shard $i: id_start=$start id_end=$end suffix=$suffix"
    uv run python "$SCRIPT" \
        --id-start $start \
        --id-end $end \
        --output-suffix "$suffix" \
        --no-db \
        &
    pids+=($!)
done

echo "Waiting for $N_PROC processes..."
for pid in "${pids[@]}"; do
    wait $pid
    echo "Process $pid done"
done

echo "Merging parquet shards..."
uv run python -c "
import pandas as pd
from pathlib import Path
output_dir = Path('${OUTPUT_DIR}')
shards = sorted(output_dir.glob('interpolation_detect_*.parquet'))
dfs = [pd.read_parquet(s) for s in shards]
merged = pd.concat(dfs, ignore_index=True).sort_values('hylak_id').reset_index(drop=True)
merged.to_parquet(output_dir / 'interpolation_detect.parquet', index=False)
print(f'Merged {len(shards)} shards into {len(merged)} rows')
for s in shards:
    s.unlink()
    print(f'Removed {s.name}')
"

echo "Writing true-linear lakes to PostgreSQL..."
uv run python -c "
import pandas as pd
from pathlib import Path
from lakesource.postgres import series_db, upsert_interpolation_detect, ensure_interpolation_detect_table
output_dir = Path('${OUTPUT_DIR}')
df = pd.read_parquet(output_dir / 'interpolation_detect.parquet')
linear = df[df['n_linear_segments'] > 0]
rows = linear.to_dict('records')
with series_db.connection_context() as conn:
    ensure_interpolation_detect_table(conn)
    upsert_interpolation_detect(conn, rows)
print(f'Wrote {len(rows)} true-linear lakes to PostgreSQL')
"

echo "Done!"