"""Space Weather Canada adapter for 10.7cm Solar Radio Flux.

Fetches the daily 10.7cm solar radio flux (F10.7) from the Canadian Space
Weather Forecast Centre. The data file contains multiple readings per day
at different times. Following the DailyActivityValuesUpdater convention,
we select the reading closest to 20:00 UTC for each date and use the
adjusted flux value (column 6).

Data source: https://www.spaceweather.gc.ca/solar_flux_data/daily_flux_values/fluxtable.txt
"""

from __future__ import annotations

from datetime import date

import structlog

from src.ingestion.base import IngestionError, SolarDataSource, SolarObservation

logger = structlog.get_logger()

FLUX_TABLE_URL = (
    "https://www.spaceweather.gc.ca/solar_flux_data/daily_flux_values/fluxtable.txt"
)


class SpaceWeatherCASource(SolarDataSource):
    """Adapter for 10.7cm solar radio flux from Space Weather Canada.

    The flux table has multiple readings per day. We pick the one closest
    to 20:00 UTC (time code 200000), matching the C++ program's behavior.
    The adjusted flux (6th column) is used.
    """

    def __init__(self) -> None:
        super().__init__(name="spaceweather_ca")

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch adjusted F10.7 values for the given date range.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with f10_7 populated (one per day).

        Raises:
            IngestionError: If the data cannot be retrieved or parsed.
        """
        try:
            response = self._get(FLUX_TABLE_URL)
            observations = self._parse(response.text, start, end)
        except IngestionError:
            raise
        except Exception as exc:
            logger.error("spaceweather_ca_fetch_failed", error=str(exc))
            raise IngestionError(f"Failed to fetch Space Weather CA data: {exc}") from exc

        logger.info(
            "spaceweather_ca_fetch_complete",
            observations=len(observations),
            start=str(start),
            end=str(end),
        )
        return observations

    def _parse(self, text: str, start: date, end: date) -> list[SolarObservation]:
        """Parse the flux table, selecting the reading closest to 20:00 UTC per day.

        For each date, we may see entries at 170000, 200000, 230000 etc.
        We pick the one with the smallest abs(time - 200000), matching
        the DailyActivityValuesUpdater logic.

        Args:
            text: Raw response body.
            start: Filter start date.
            end: Filter end date.

        Returns:
            One observation per day with f10_7 set to adjusted flux.
        """
        # best_for_date: {date_obj: (delta_from_2000, adjusted_flux, raw_line)}
        best_for_date: dict[date, tuple[int, float, str]] = {}

        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue

            tokens = line.split()
            if len(tokens) < 6:
                continue

            # Skip header and dashed lines
            if not tokens[0].isdigit():
                continue

            try:
                date_str = tokens[0]   # YYYYMMDD
                time_str = tokens[1]   # HHMMSS
                adj_flux_str = tokens[5]  # adjusted flux (6th column)

                if len(date_str) != 8 or not time_str.isdigit():
                    continue

                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                obs_date = date(year, month, day)

                if obs_date < start or obs_date > end:
                    continue

                hhmmss = int(time_str)
                delta = abs(hhmmss - 200000)

                # Strip leading zeros from flux value
                adj_flux = float(adj_flux_str)

                # Keep the entry closest to 20:00 UTC
                if obs_date not in best_for_date or delta < best_for_date[obs_date][0]:
                    best_for_date[obs_date] = (delta, adj_flux, line)

            except (ValueError, IndexError) as exc:
                logger.warning("spaceweather_ca_parse_skip", line=line[:80], error=str(exc))
                continue

        # Convert to observations
        observations = []
        for obs_date in sorted(best_for_date.keys()):
            _, adj_flux, raw_line = best_for_date[obs_date]
            observations.append(
                SolarObservation(
                    date=obs_date,
                    source=self.name,
                    f10_7=adj_flux,
                    raw_payload={"raw_line": raw_line},
                )
            )

        return observations