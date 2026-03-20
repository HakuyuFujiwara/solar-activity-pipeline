#!/bin/bash
# Generate daily activity values for an HMI pipeline run.
# Usage: ./generate_run.sh 76
#
# This is a convenience wrapper so users don't need to know
# about Python, virtual environments, or command-line flags.
# It auto-updates from GitHub before each run.

set -e

# Check argument
if [ -z "$1" ]; then
    echo "Usage: ./generate_run.sh <run_number>"
    echo "Example: ./generate_run.sh 76"
    echo ""
    echo "This will generate the daily activity values .dat file"
    echo "for the specified HMI pipeline run."
    exit 1
fi

RUN=$1
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Auto-update from GitHub (skip if offline or if it fails)
echo "Checking for updates..."
git pull --ff-only 2>/dev/null && echo "Updated." || echo "Skipped update (offline or conflict)."

# Limit OpenBLAS threads (login nodes have restricted process limits)
export OPENBLAS_NUM_THREADS=1


# Activate virtual environment
source "$SCRIPT_DIR/.venv/bin/activate"

# Run pipeline
python -m src.pipeline --run "$RUN"

echo ""
echo "Done! Output file is in:"
echo "/project2/erhodes_44/rcf-04/astro10/data/mdi/lnu/comparison/dailyactivityvalueshmirun${RUN}.dat"