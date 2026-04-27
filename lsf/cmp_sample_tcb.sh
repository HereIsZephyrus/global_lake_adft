#!/bin/bash
#BSUB -J cmp_sample_tcb
#BSUB -q normal
#BSUB -n 64
#BSUB -R "span[hosts=1]"
#BSUB -M 262144
#BSUB -o %J.out
#BSUB -e %J.err
#BSUB -W 1:00

PROJECT_DIR="/data/users/guxh01/2026_tcb/lake/global_lake_adft"
cd "$PROJECT_DIR"

echo "=== Comparison Sample Run (49K lakes, 63 workers) ==="
echo "Start: $(date)"
echo "Host: $(hostname)"

eval "$(conda shell.bash hook)"
conda activate tcb_lake

echo "Python: $(which python)"

export PYTHONPATH="$PROJECT_DIR/packages/lakesource/src:$PROJECT_DIR/packages/lakeanalysis/src:$PROJECT_DIR/packages/lakeviz/src:$PYTHONPATH"

export DATA_BACKEND=parquet
export PARQUET_DATA_DIR=/data/users/guxh01/2026_tcb/lake/lake_data
export PARQUET_OUTPUT_DIR=/data/users/guxh01/2026_tcb/lake/lake_data/comparison

mkdir -p "$PARQUET_OUTPUT_DIR"

echo "Running comparison on sampled lakes..."
time mpirun -np 64 python packages/lakeanalysis/scripts/run_algorithm_comparison.py \
    --sample-file data/comparison/sample_lakes.parquet \
    --chunk-size 1000 \
    --io-budget 63

echo "End: $(date)"
