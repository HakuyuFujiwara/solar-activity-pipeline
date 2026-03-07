"""MgII Core-to-Wing ratio adapter.

Fetches the MgII core-to-wing ratio from Space Environment Technologies.
This is a proxy for solar UV variability and corresponds to column 19 in
the DailyActivityValuesUpdater output.

Header lines start with ':' or '#'. Data lines have format:
    YYYY MM DD julian_date MgII source

Data source: https://sol.spacenvironment.net/spacewx/data/mg2_atmos.dat.txt
"""

from __future__ import annotations

from datetime import date

import structlog

from src.ingestion.base import IngestionError, SolarDataSource, SolarObservation

logger = structlog.get_logger()

MGII_URL = "https://sol.spacenvironment.net/spacewx/data/mg2_atmos.dat.txt"


class MgIISource(SolarDataSource):
    """Adapter for MgII Core-to-Wing ratio.

    Provides the MgII index used as a solar UV activity proxy.
    The C++ program reads Year Month Day, skips column 4 (JD),
    and takes column 5 (the MgII value).
    """

    def __init__(self) -> None:
        super().__init__(name="mgii")

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch MgII values for the given date range.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with raw_payload containing mgii value.

        Raises:
            IngestionError: If the data cannot be retrieved or parsed.
        """
        try:
            response = self._get(MGII_URL)
            observations = self._parse(response.text, start, end)
        except IngestionError:
            raise
        except Exception as exc:
            logger.error("mgii_fetch_failed", error=str(exc))
            raise IngestionError(f"Failed to fetch MgII data: {exc}") from exc

        logger.info(
            "mgii_fetch_complete",
            observations=len(observations),
            start=str(start),
            end=str(end),
        )
        return observations

    def _parse(self, text: str, start: date, end: date) -> list[SolarObservation]:
        """Parse MgII data file.

        Lines starting with ':' or '#' are headers/comments.
        Data lines: Year Month Day JulianDate MgII Source

        Args:
            text: Raw file content.
            start: Filter start date.
            end: Filter end date.

        Returns:
            Parsed observations.
        """
        observations: list[SolarObservation] = []

        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith(":") or line.startswith("#"):
                continue

            tokens = line.split()
            if len(tokens) < 5:
                continue

            try:
                year = int(tokens[0])
                month = int(tokens[1])
                day = int(tokens[2])
                obs_date = date(year, month, day)

                if obs_date < start or obs_date > end:
                    continue

                # tokens[3] is JD (skip), tokens[4] is MgII value
                mgii_str = tokens[4]
                mgii = float(mgii_str)

                if mgii == 0.0:
                    mgii = None  # 0.0 means missing per the file header

                observations.append(
                    SolarObservation(
                        date=obs_date,
                        source=self.name,
                        raw_payload={
                            "mgii": mgii_str,
                            "mgii_float": mgii,
                        },
                    )
                )
            except (ValueError, IndexError) as exc:
                logger.warning("mgii_parse_skip", line=line[:80], error=str(exc))
                continue

        return observations
