#!/bin/bash
#BSUB -J comparison_tcb
#BSUB -q normal
#BSUB -n 128
#BSUB -R "span[hosts=1]"
#BSUB -M 262144
#BSUB -o %J.out
#BSUB -e %J.err
#BSUB -W 72:00

PROJECT_DIR="/data/users/guxh01/2026_tcb/lake/global_lake_adft"
cd "$PROJECT_DIR"

echo "=== Comparison Full Run (Quantile vs PWM Extreme, 1.4M lakes, 127 workers) ==="
echo "Start: $(date)"
echo "Host: $(hostname)"

eval "$(conda shell.bash hook)"
conda activate tcb_lake

echo "Python: $(which python)"
echo "Numba: $(python -c 'import numba; print(numba.__version__)' 2>/dev/null || echo 'not installed')"

export PYTHONPATH="$PROJECT_DIR/packages/lakesource/src:$PROJECT_DIR/packages/lakeanalysis/src:$PROJECT_DIR/packages/lakeviz/src:$PYTHONPATH"

export DATA_BACKEND=parquet
export PARQUET_DATA_DIR=/data/users/guxh01/2026_tcb/lake/lake_data
export PARQUET_OUTPUT_DIR=/data/users/guxh01/2026_tcb/lake/lake_data/comparison

mkdir -p "$PARQUET_OUTPUT_DIR"

echo "Running Comparison full computation..."
time mpirun -np 128 python packages/lakeanalysis/scripts/run_algorithm_comparison.py \
    --chunk-size 10000 \
    --io-budget 127

echo "End: $(date)"
