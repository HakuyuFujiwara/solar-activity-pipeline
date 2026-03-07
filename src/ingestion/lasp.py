"""LASP SDO/SOHO SEM UV daily averages adapter.

Fetches daily average solar EUV flux from the LASP (Laboratory for
Atmospheric and Space Physics) at University of Colorado. The data comes
from the SOHO SEM (Solar EUV Monitor) instrument.

The .dat file columns 13 and 14 use the last two values from each data line.
Note: the C++ program swaps these when printing (col14 in position 13,
col13 in position 14). We store them as-is and handle the swap in export.

Data source: https://lasp.colorado.edu/eve/data_access/eve_data/lasp_soho_sem_data/long/daily_avg/
File pattern: YY_v4.day (two-digit year)
"""

from __future__ import annotations

from datetime import date, timedelta

import structlog

from src.ingestion.base import SolarDataSource, SolarObservation

logger = structlog.get_logger()

BASE_URL = (
    "https://lasp.colorado.edu/eve/data_access/eve_data/"
    "lasp_soho_sem_data/long/daily_avg"
)


class LASPSource(SolarDataSource):
    """Adapter for LASP SOHO SEM daily average UV flux.

    Downloads per-year data files and extracts the last two columns
    (First Order Flux and the column before it). These correspond to
    columns 13-14 in the DailyActivityValuesUpdater output.
    """

    def __init__(self) -> None:
        super().__init__(name="lasp")

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch SEM UV values for the given date range.

        Downloads one file per year covered by the date range.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with raw_payload containing
            sem_second_last and sem_last values.

        Raises:
            IngestionError: If no data files can be retrieved.
        """
        years = list(range(start.year, end.year + 1))
        all_observations: list[SolarObservation] = []

        for year in years:
            try:
                observations = self._fetch_year(year, start, end)
                all_observations.extend(observations)
                logger.info("lasp_year_fetched", year=year, count=len(observations))
            except Exception as exc:
                logger.warning("lasp_year_failed", year=year, error=str(exc))

        logger.info(
            "lasp_fetch_complete",
            observations=len(all_observations),
            start=str(start),
            end=str(end),
        )
        return all_observations

    def _fetch_year(self, year: int, start: date, end: date) -> list[SolarObservation]:
        """Download and parse one year's SEM data file.

        Args:
            year: 4-digit year.
            start: Filter start date.
            end: Filter end date.

        Returns:
            Observations for dates within range.
        """
        yy = f"{year % 100:02d}"
        url = f"{BASE_URL}/{yy}_v4.day"
        response = self._get(url)
        return self._parse(response.text, year, start, end)

    def _parse(
        self, text: str, year: int, start: date, end: date
    ) -> list[SolarObservation]:
        """Parse SEM daily average file.

        Lines starting with ';' are headers/comments. Data lines have 16 tokens:
        julian_date, year, doy, CH1, stdev_CH1, CH2, stdev_CH2, CH3, stdev_CH3,
        X, Y, Z, R, R_AU, first_order_flux, last_col

        We extract tokens[-2] (second-to-last) and tokens[-1] (last).

        Args:
            text: Raw file content.
            year: Year of the file.
            start: Filter start date.
            end: Filter end date.

        Returns:
            Parsed observations.
        """
        observations: list[SolarObservation] = []

        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith(";"):
                continue

            tokens = line.split()
            if len(tokens) < 16:
                continue

            try:
                file_year = int(tokens[1])
                doy = int(tokens[2])

                # Convert year + day-of-year to date
                obs_date = date(file_year, 1, 1) + timedelta(days=doy - 1)

                if obs_date < start or obs_date > end:
                    continue

                second_last = tokens[-2]  # stored as col13 in C++
                last = tokens[-1]         # stored as col14 in C++

                # Validate they look like scientific notation numbers
                second_last_f = float(second_last)
                last_f = float(last)

                observations.append(
                    SolarObservation(
                        date=obs_date,
                        source=self.name,
                        raw_payload={
                            "sem_second_last": second_last,
                            "sem_last": last,
                            "sem_second_last_float": second_last_f,
                            "sem_last_float": last_f,
                        },
                    )
                )
            except (ValueError, IndexError) as exc:
                logger.warning("lasp_parse_skip", line=line[:80], error=str(exc))
                continue

        return observations
