#!/bin/bash
#BSUB -J cmp_grid_agg_tcb
#BSUB -q normal
#BSUB -n 1
#BSUB -R "span[hosts=1]"
#BSUB -M 65536
#BSUB -o %J.out
#BSUB -e %J.err
#BSUB -W 1:00

PROJECT_DIR="/data/users/guxh01/2026_tcb/lake/global_lake_adft"
cd "$PROJECT_DIR"

echo "=== Comparison Grid Aggregation ==="
echo "Start: $(date)"
echo "Host: $(hostname)"

eval "$(conda shell.bash hook)"
conda activate tcb_lake

echo "Python: $(which python)"

export PYTHONPATH="$PROJECT_DIR/packages/lakesource/src:$PROJECT_DIR/packages/lakeanalysis/src:$PROJECT_DIR/packages/lakeviz/src:$PYTHONPATH"
export DATA_BACKEND=parquet
export PARQUET_DATA_DIR=/data/users/guxh01/2026_tcb/lake/lake_data

python packages/lakeviz/scripts/comparison_grid_agg.py \
    --comparison-dir /data/users/guxh01/2026_tcb/lake/lake_data/comparison \
    --sample-file data/comparison/sample_lakes.parquet \
    --output-dir /data/users/guxh01/2026_tcb/lake/lake_data/comparison \
    --resolution 0.5

echo "End: $(date)"
