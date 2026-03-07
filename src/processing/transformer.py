"""Data transformation and cleaning utilities.

Handles normalization of raw observations into a consistent schema, merges
records from multiple sources by date, and exports to the legacy .dat format
compatible with the DailyActivityValuesUpdater program (full 19 columns).
"""

from __future__ import annotations

import math
import os
from collections import defaultdict
from datetime import date, timedelta

import structlog

from src.config import settings
from src.ingestion.base import SolarObservation

logger = structlog.get_logger()

# Baseline date for MDI Day calculation (April 30, 2010)
_BASELINE = date(2010, 4, 30)


class Transformer:
    """Merges multi-source observations and produces unified daily records.

    Given observations from all sources, the transformer groups them by date
    and creates a single merged record per day. It also handles the export
    to the full 19-column .dat format used by the DailyActivityValuesUpdater.
    """

    def merge_by_date(
        self, observations: list[SolarObservation]
    ) -> dict[date, dict[str, SolarObservation]]:
        """Group observations by date, keyed by source name."""
        grouped: dict[date, dict[str, SolarObservation]] = defaultdict(dict)
        for obs in observations:
            grouped[obs.date][obs.source] = obs
        return dict(grouped)

    def to_unified_records(
        self, observations: list[SolarObservation]
    ) -> list[dict]:
        """Merge multi-source observations into unified daily records.

        For each date, combines available fields from all sources into a
        single dictionary.

        Args:
            observations: Flat list from all sources.

        Returns:
            List of dicts sorted by date.
        """
        grouped = self.merge_by_date(observations)
        records: list[dict] = []

        for obs_date in sorted(grouped.keys()):
            sources = grouped[obs_date]
            record = {
                "date": obs_date,
                "ra": None,
                "isn": None,
                "f10_7": None,
                "f10_7_adj": None,
                "ap_index": None,
                "sem_second_last": None,
                "sem_last": None,
                "mgii": None,
                "sources": list(sources.keys()),
            }

            if "aavso" in sources:
                record["ra"] = sources["aavso"].ra

            if "silso" in sources:
                record["isn"] = sources["silso"].international_sunspot_number

            if "noaa" in sources:
                record["f10_7"] = sources["noaa"].f10_7
                record["ap_index"] = sources["noaa"].ap_index
                if record["isn"] is None:
                    record["isn"] = sources["noaa"].international_sunspot_number

            if "spaceweather_ca" in sources:
                record["f10_7_adj"] = sources["spaceweather_ca"].f10_7

            if "lasp" in sources:
                payload = sources["lasp"].raw_payload
                record["sem_second_last"] = payload.get("sem_second_last")
                record["sem_last"] = payload.get("sem_last")

            if "mgii" in sources:
                payload = sources["mgii"].raw_payload
                record["mgii"] = payload.get("mgii")

            records.append(record)

        logger.info("transformer_merge_complete", days=len(records))
        return records

    def export_dat(
        self,
        records: list[dict],
        run_number: int,
        mdi_day_start: int,
    ) -> str:
        """Export unified records to the full 19-column .dat format.

        Produces a file byte-compatible with DailyActivityValuesUpdater output.

        Column layout:
            1:  MDI Day Number (days since 2010-04-30, +1)
            2:  Offset (col1 + 3772)
            3:  Year
            4:  Month (zero-padded)
            5:  Day (zero-padded)
            6:  Fractional year (YYYY.FFF)
            7:  Placeholder (-1.)
            8:  ISN (right-aligned 3 digits + period)
            9:  Ra (right-aligned 3 digits + period)
            10: F10.7 adjusted flux
            11: Placeholder (-1.0000000)
            12: Placeholder (-1.0000000)
            13: SEM UV last (printed first due to C++ swap)
            14: SEM UV second_last (printed second due to C++ swap)
            15: Placeholder (-1.0000)
            16: Placeholder (-1.0000)
            17: Placeholder (-1.000)
            18: Placeholder (-1.000)
            19: MgII core-to-wing ratio

        Args:
            records: Unified daily records from to_unified_records().
            run_number: HMI pipeline run number (e.g., 76).
            mdi_day_start: Starting MDI day number for this run.

        Returns:
            Path to the written .dat file.
        """
        os.makedirs(settings.dat_output_dir, exist_ok=True)
        filename = f"dailyactivityvalueshmirun{run_number}.dat"
        filepath = os.path.join(settings.dat_output_dir, filename)

        with open(filepath, "w") as f:
            for i, record in enumerate(records):
                obs_date = record["date"]

                # Columns 1-2: MDI Day and Offset
                days_since = (obs_date - _BASELINE).days
                mdi_day = days_since + 1
                offset = mdi_day + 3772

                # Columns 3-5: Year, Month, Day
                year = obs_date.year
                month = f"{obs_date.month:02d}"
                day = f"{obs_date.day:02d}"

                # Column 6: Fractional year
                frac_year = self._fractional_year(obs_date)

                # Column 7: Placeholder
                col7 = "-1."

                # Column 8: ISN
                isn = record.get("isn")
                col8 = f"{int(isn):3d}" if isn is not None else " -1"

                # Column 9: Ra
                ra = record.get("ra")
                col9 = f"{int(ra):3d}" if ra is not None else " -1"

                # Column 10: F10.7 adjusted flux
                f10_7 = record.get("f10_7_adj")
                col10 = f"{f10_7}" if f10_7 is not None else "-1"

                # Columns 11-12: Placeholders
                col11 = "-1.0000000"
                col12 = "-1.0000000"

                # Columns 13-14: SEM UV (swapped in output, matching C++ behavior)
                sem_last = record.get("sem_last")
                sem_second_last = record.get("sem_second_last")
                col13 = sem_last.upper() if sem_last is not None else "-1.0000E+10"
                col14 = sem_second_last.upper() if sem_second_last is not None else "-1.0000E+10"

                # Columns 15-18: Placeholders
                col15 = "-1.0000"
                col16 = "-1.0000"
                col17 = "-1.000"
                col18 = "-1.000"

                # Column 19: MgII
                mgii = record.get("mgii")
                col19 = mgii if mgii is not None else "-1"

                # Format the line matching C++ spacing
                line = (
                    f" {mdi_day}"
                    f" {offset}"
                    f" {year}"
                    f" {month}"
                    f" {day}"
                    f" {frac_year}"
                    f"  {col7}"
                    f" {col8:>3s}."
                    f" {col9:>3s}."
                    f" {col10}"
                    f" {col11}"
                    f" {col12}"
                    f" {col13}"
                    f" {col14}"
                    f"   {col15}"
                    f"   {col16}"
                    f" {col17}"
                    f" {col18}"
                    f" {col19}"
                )
                f.write(line + "\n")

        logger.info("dat_export_complete", filepath=filepath, rows=len(records))
        return filepath

    @staticmethod
    def _fractional_year(d: date) -> str:
        """Compute fractional year string matching C++ fractionalYearString().

        Formula: year + round((day_of_year + 0.5) / days_in_year * 1000) / 1000

        Args:
            d: The date to convert.

        Returns:
            String like "2024.717".
        """
        year = d.year
        is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
        days_in_year = 366 if is_leap else 365

        # day_of_year: Jan 1 = 0 (matching C's tm_yday)
        day_of_year = (d - date(year, 1, 1)).days

        frac = (day_of_year + 0.5) / days_in_year
        frac_thousand = round(frac * 1000)

        return f"{year}.{frac_thousand:03d}"