# HPC Deployment Guide (USC Discovery / CARC)

This guide is for the Solar Physics Group member maintaining this tool
on the USC Discovery HPC cluster.

## Quick Usage

Generate daily activity values for a run:
```bash
/project2/erhodes_44/dongpoba/solar-activity-pipeline/generate_run.sh 76
```

Output goes to:
`/project2/erhodes_44/rcf-04/astro10/data/mdi/lnu/comparison/dailyactivityvalueshmirun76.dat`

## Adding a New Run

No code changes needed! Run numbers are computed automatically.
Simply run:
```bash
/project2/erhodes_44/dongpoba/solar-activity-pipeline/generate_run.sh 77
```

Any run number works. Dates and JSOC day numbers are calculated
from a known anchor point (Run 74 = 2024-09-19, JSOC 11584).
Each run is exactly 72 days, consecutive with no gaps.

## First-Time Setup (New Machine or New Maintainer)
```bash
cd /project2/erhodes_44/dongpoba
git clone https://github.com/HakuyuFujiwara/solar-activity-pipeline.git
cd solar-activity-pipeline
module load python/3.11
python -m venv .venv
source .venv/bin/activate
pip install -e .
mkdir -p data
```

Test: `python -m src.pipeline --run 74 --dry-run`

## Updating After Code Changes on GitHub
```bash
cd /project2/erhodes_44/dongpoba/solar-activity-pipeline
git pull
source .venv/bin/activate
pip install -e .
```

## If Something Breaks

### "ModuleNotFoundError"
The virtual environment is missing or broken. Rebuild it:
```bash
rm -rf .venv
module load python/3.11
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### "unable to open database file"
The `data/` directory is missing:
```bash
mkdir -p data
```

### "Run XX not found"
The run hasn't been added to `src/run_registry.py`. See "Adding a New Run" above.

### A data source returns no data
This is normal. AAVSO publishes bulletins monthly (usually a few weeks after
the month ends). Other sources may have temporary outages. The pipeline
will use whatever data is available and mark missing sources with -1 in the
output.

### "OpenBLAS blas_thread_init: pthread_create failed"
This happens on login nodes with restricted process limits.
The `generate_run.sh` script handles this automatically with
`OPENBLAS_NUM_THREADS=1`. If running manually, set it first:
```bash
export OPENBLAS_NUM_THREADS=1
python -m src.pipeline --run 76
```

### AAVSO Ra values showing -1
The program automatically scrapes the AAVSO website to find bulletins,
even when filenames are wrong (e.g., December filed as "11_0", or
"AAVO" instead of "AAVSO"). If Ra is still -1, that month's bulletin
has not been published yet. You can check at:
https://www.aavso.org/solar-bulletin

### SEM UV value looks unusually large
The pipeline logs a warning when SEM UV values exceed 8×10¹⁰. This
is usually a data quality issue from LASP, not a bug in our program.
The value is still written to the .dat file as-is.

## Data Sources

The pipeline pulls from 6 sources automatically. No manual downloads needed.

| Source | What it provides | URL |
|--------|-----------------|-----|
| AAVSO | Ra (from monthly PDF) | aavso.org/solar-bulletin |
| SILSO | International Sunspot Number | sidc.be/silso |
| Space Weather Canada | 10.7cm radio flux | spaceweather.gc.ca |
| LASP Colorado | SDO/SOHO SEM UV | lasp.colorado.edu |
| Space Environment Tech | MgII ratio | sol.spacenvironment.net |
| NOAA SWPC | F10.7 monthly avg | swpc.noaa.gov |

## Architecture Overview

This Python program replaces the old C++ DailyActivityValuesUpdater.
Instead of manually downloading 5 files and pasting Ra values, it
fetches everything automatically and generates the identical 19-column
.dat file.

Code is at: https://github.com/HakuyuFujiwara/solar-activity-pipeline

## Contact

Created by Eri Bai (dongpoba@usc.edu)
GitHub: https://github.com/HakuyuFujiwara
