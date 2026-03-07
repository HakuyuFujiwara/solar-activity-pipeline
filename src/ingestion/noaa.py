"""NOAA Space Weather Prediction Center adapter.

Fetches solar cycle indices from the NOAA SWPC JSON API, including F10.7 cm
radio flux and sunspot numbers. These serve as secondary activity indices
for cross-validation against AAVSO Ra values.

The SWPC endpoint returns a JSON array of monthly records spanning multiple
solar cycles.
"""

from __future__ import annotations

from datetime import date, datetime

import structlog

from src.config import settings
from src.ingestion.base import IngestionError, SolarDataSource, SolarObservation

logger = structlog.get_logger()


class NOAASource(SolarDataSource):
    """Adapter for NOAA SWPC solar cycle indices.

    Provides F10.7 radio flux and sunspot number data. The SWPC endpoint
    returns monthly averages, so observations are keyed to the first day
    of each month.
    """

    def __init__(self) -> None:
        super().__init__(name="noaa")

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch solar indices from NOAA SWPC for the given date range.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with f10_7 and ap_index populated.
            One record per month (keyed to the 1st of each month).

        Raises:
            IngestionError: If the SWPC API cannot be reached or parsed.
        """
        try:
            response = self._get(settings.noaa_solar_indices_url)
            records = response.json()
        except Exception as exc:
            logger.error("noaa_fetch_failed", error=str(exc))
            raise IngestionError(f"Failed to fetch NOAA data: {exc}") from exc

        observations = self._filter_and_parse(records, start, end)
        logger.info(
            "noaa_fetch_complete",
            observations=len(observations),
            start=str(start),
            end=str(end),
        )
        return observations

    def _filter_and_parse(
        self, records: list[dict], start: date, end: date
    ) -> list[SolarObservation]:
        """Filter NOAA JSON records to the requested range and convert.

        Args:
            records: Raw JSON array from the SWPC API.
            start: Filter start date.
            end: Filter end date.

        Returns:
            Parsed observations within the requested range.
        """
        observations: list[SolarObservation] = []

        for record in records:
            try:
                time_tag = record.get("time-tag", "")
                obs_date = datetime.strptime(time_tag, "%Y-%m").date().replace(day=1)

                if obs_date < start or obs_date > end:
                    continue

                f10_7 = self._safe_float(record.get("f10.7"))
                ssn = self._safe_float(record.get("ssn"))
                ap = self._safe_float(record.get("ap"))

                observations.append(
                    SolarObservation(
                        date=obs_date,
                        source=self.name,
                        f10_7=f10_7,
                        ap_index=ap,
                        international_sunspot_number=ssn,
                        raw_payload=record,
                    )
                )
            except (ValueError, KeyError) as exc:
                logger.warning("noaa_parse_skip_record", record=record, error=str(exc))
                continue

        return observations

    @staticmethod
    def _safe_float(value: object) -> float | None:
        """Convert a value to float, returning None for missing or invalid data.

        Args:
            value: Raw value from JSON (could be str, int, float, or None).

        Returns:
            Float value or None.
        """
        if value is None or value == "":
            return None
        try:
            result = float(value)
            return result if result >= 0 else None
        except (ValueError, TypeError):
            return None