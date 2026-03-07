"""HMI Pipeline Run Registry.

Automatically computes date ranges and JSOC day numbers for any run.
Each HMI run is exactly 72 days (24 three-day sets), and runs are
consecutive with no gaps.

Two different "day numbers" exist in this system:

1. JSOC MDI Day: epoch ~1993-01-01, used for Stanford JSOC queries
   (e.g., hmi.V_sht_pow[11584d]).

2. DAT file Day (column 1): epoch 2010-04-30, computed automatically
   from the date by the transformer.

This module only needs one anchor point to compute any run's dates.
Leap years and month lengths are handled automatically by Python's
datetime library.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

# Anchor point: one known run from which all others are computed
_ANCHOR_RUN = 74
_ANCHOR_START = date(2024, 9, 19)
_ANCHOR_JSOC_DAY = 11584

# Every run is exactly 72 days
DAYS_PER_RUN = 72

# Lab output directory on Discovery HPC
LAB_OUTPUT_DIR = "/project2/erhodes_44/rcf-04/astro10/data/mdi/lnu/comparison"


@dataclass
class RunInfo:
    """Metadata for a single HMI pipeline run."""

    run_number: int
    start_date: date
    end_date: date
    first_jsoc_day: int

    @property
    def num_days(self) -> int:
        """Number of days in this run (inclusive)."""
        return (self.end_date - self.start_date).days + 1


def get_run(run_number: int) -> RunInfo:
    """Compute dates and JSOC day for any run number.

    Uses the anchor point (Run 74 = 2024-09-19, JSOC 11584) and the fact
    that every run is exactly 72 days to calculate any run's metadata.
    Leap years and month boundaries are handled automatically by
    Python's datetime.

    Args:
        run_number: The HMI pipeline run number (must be >= 1).

    Returns:
        RunInfo with computed dates and JSOC day.

    Raises:
        ValueError: If run number is less than 1.
    """
    if run_number < 1:
        raise ValueError(f"Run number must be >= 1, got {run_number}")

    offset_runs = run_number - _ANCHOR_RUN
    offset_days = offset_runs * DAYS_PER_RUN

    start_date = _ANCHOR_START + timedelta(days=offset_days)
    end_date = start_date + timedelta(days=DAYS_PER_RUN - 1)
    first_jsoc_day = _ANCHOR_JSOC_DAY + offset_days

    return RunInfo(
        run_number=run_number,
        start_date=start_date,
        end_date=end_date,
        first_jsoc_day=first_jsoc_day,
    )


def list_runs(start: int = 1, end: int = 80) -> list[RunInfo]:
    """List a range of runs with computed metadata.

    Args:
        start: First run number.
        end: Last run number (inclusive).

    Returns:
        List of RunInfo objects.
    """
    return [get_run(n) for n in range(start, end + 1)]