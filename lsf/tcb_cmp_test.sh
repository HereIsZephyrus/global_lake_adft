#!/bin/bash
#BSUB -J tcb_cmp_test
#BSUB -q normal
#BSUB -n 5
#BSUB -R "span[hosts=1]"
#BSUB -M 8192
#BSUB -o %J.out
#BSUB -e %J.err
#BSUB -W 0:15

PROJECT_DIR="/data/users/guxh01/2026_tcb/lake/global_lake_adft"
cd "$PROJECT_DIR"

echo "=== Smoke Test: Comparison (Quantile vs PWM, 4 workers, 400 lakes) ==="
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

echo "Running Comparison smoke test (4 workers, 400 lakes)..."
time mpirun -np 5 python packages/lakeanalysis/scripts/run_algorithm_comparison.py \
    --chunk-size 100 \
    --id-start 0 \
    --id-end 400 \
    --io-budget 4

echo "End: $(date)"
