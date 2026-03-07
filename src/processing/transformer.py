"""Data transformation and cleaning utilities.

Handles normalization of raw observations into a consistent schema, merges
records from multiple sources by date, and exports to the legacy .dat format
compatible with the DailyActivityValuesUpdater program.
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import date

import structlog

from src.config import settings
from src.ingestion.base import SolarObservation

logger = structlog.get_logger()


class Transformer:
    """Merges multi-source observations and produces unified daily records.

    Given observations from AAVSO, NOAA, and SILSO, the transformer groups
    them by date and creates a single merged record per day. It also handles
    the export to the fixed-width .dat format used by the legacy C++ program.
    """

    def merge_by_date(
        self, observations: list[SolarObservation]
    ) -> dict[date, dict[str, SolarObservation]]:
        """Group observations by date, keyed by source name.

        Args:
            observations: Flat list from all sources.

        Returns:
            Mapping of {date: {source_name: observation}}.
        """
        grouped: dict[date, dict[str, SolarObservation]] = defaultdict(dict)
        for obs in observations:
            grouped[obs.date][obs.source] = obs
        return dict(grouped)

    def to_unified_records(
        self, observations: list[SolarObservation]
    ) -> list[dict]:
        """Merge multi-source observations into unified daily records.

        For each date, combines available fields from all sources into a
        single dictionary. Prefers AAVSO for Ra, SILSO for ISN, NOAA for
        F10.7 and Ap.

        Args:
            observations: Flat list from all sources.

        Returns:
            List of dicts with keys: date, ra, isn, f10_7, ap_index, sources.
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
                "ap_index": None,
                "sources": list(sources.keys()),
            }

            if "aavso" in sources:
                record["ra"] = sources["aavso"].ra

            if "silso" in sources:
                record["isn"] = sources["silso"].international_sunspot_number

            if "noaa" in sources:
                record["f10_7"] = sources["noaa"].f10_7
                record["ap_index"] = sources["noaa"].ap_index
                # NOAA also has SSN, use as fallback if SILSO missing
                if record["isn"] is None:
                    record["isn"] = sources["noaa"].international_sunspot_number

            records.append(record)

        logger.info("transformer_merge_complete", days=len(records))
        return records

    def export_dat(
        self,
        records: list[dict],
        run_number: int,
        mdi_day_start: int,
    ) -> str:
        """Export unified records to the legacy .dat format.

        Produces a fixed-width text file compatible with the
        DailyActivityValuesUpdater C++ program. Each line contains the MDI day
        number followed by activity values. Missing values are written as -1.0.

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
                mdi_day = mdi_day_start + i
                ra = record.get("ra")
                isn = record.get("isn")
                f10_7 = record.get("f10_7")

                ra_str = f"{ra:8.1f}" if ra is not None else "    -1.0"
                isn_str = f"{isn:8.1f}" if isn is not None else "    -1.0"
                f10_7_str = f"{f10_7:8.1f}" if f10_7 is not None else "    -1.0"

                f.write(f"{mdi_day:6d}{ra_str}{isn_str}{f10_7_str}\n")

        logger.info("dat_export_complete", filepath=filepath, rows=len(records))
        return filepath