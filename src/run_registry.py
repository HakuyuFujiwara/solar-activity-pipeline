"""HMI Pipeline Run Registry.

Maps run numbers to their date ranges and JSOC day numbers.
This allows the pipeline to be invoked with just a run number:
    python -m src.pipeline --run 76

Two different "day numbers" exist in this system:

1. JSOC MDI Day: epoch ~1993-01-01, used for Stanford JSOC queries
   (e.g., hmi.V_sht_pow[11584d]). This is what the lab calls "day number".

2. DAT file Day (column 1): epoch 2010-04-30, computed automatically
   from the date. This is what appears in the .dat output file.

The registry stores JSOC day numbers. The .dat column 1 values are
calculated by the transformer from dates, no manual input needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class RunInfo:
    """Metadata for a single HMI pipeline run."""

    run_number: int
    start_date: date
    end_date: date
    first_jsoc_day: int  # JSOC MDI day number of the first 3-day set

    @property
    def num_days(self) -> int:
        """Number of days in this run (inclusive)."""
        return (self.end_date - self.start_date).days + 1


# Registry of all known runs
# first_jsoc_day is the JSOC MDI day number used in Stanford queries
RUNS: dict[int, RunInfo] = {
    74: RunInfo(
        run_number=74,
        start_date=date(2024, 9, 19),
        end_date=date(2024, 11, 29),
        first_jsoc_day=11584,
    ),
    75: RunInfo(
        run_number=75,
        start_date=date(2024, 11, 30),
        end_date=date(2025, 2, 9),
        first_jsoc_day=11656,
    ),
    76: RunInfo(
        run_number=76,
        start_date=date(2025, 2, 10),
        end_date=date(2025, 4, 22),
        first_jsoc_day=11728,
    ),
}


def get_run(run_number: int) -> RunInfo:
    """Look up a run by number.

    Args:
        run_number: The HMI pipeline run number.

    Returns:
        RunInfo for that run.

    Raises:
        ValueError: If the run number is not in the registry.
    """
    if run_number not in RUNS:
        available = sorted(RUNS.keys())
        raise ValueError(
            f"Run {run_number} not found. Available runs: {available}"
        )
    return RUNS[run_number]


def list_runs() -> list[RunInfo]:
    """List all registered runs, sorted by run number."""
    return [RUNS[k] for k in sorted(RUNS.keys())]