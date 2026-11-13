#!/bin/bash

set -eu
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export EXPERIMENT_ROOT=/tmp/legacy-completed-qd9fmrwb/outputs/51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61
export EXPERIMENT_SOURCE=/tmp/legacy-completed-qd9fmrwb/outputs/51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61/snapshot
export PYTHONDONTWRITEBYTECODE=1
export EXPERIMENT_PYTHON=/usr/bin/python3
export EXPERIMENT_STAGE=evaluate_and_plot
export PYTHONUNBUFFERED=1
export EXPERIMENT_PROJECT=''
cd /tmp/legacy-completed-qd9fmrwb/outputs/51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61/snapshot
exec /usr/bin/python3 /tmp/legacy-completed-qd9fmrwb/outputs/51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61/snapshot/recipe.py evaluate_and_plot --output /tmp/legacy-completed-qd9fmrwb/outputs/51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61
