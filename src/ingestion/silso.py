"""SILSO (Sunspot Index and Long-term Solar Observations) adapter.

Fetches the International Sunspot Number (ISN) from the World Data Center
SILSO at the Royal Observatory of Belgium. The ISN is the authoritative
global sunspot count and serves as the primary cross-validation reference
against AAVSO Ra values.

The SILSO daily CSV format uses semicolons and has columns:
year, month, day, decimal_year, ISN, std_dev, num_observations, provisional_flag
"""

from __future__ import annotations

import csv
import io
from datetime import date

import structlog

from src.config import settings
from src.ingestion.base import IngestionError, SolarDataSource, SolarObservation

logger = structlog.get_logger()


class SILSOSource(SolarDataSource):
    """Adapter for SILSO daily International Sunspot Number.

    Provides ISN values at daily granularity. These are used to cross-validate
    AAVSO Ra values, since both measure sunspot activity but use different
    observer networks and weighting methods.
    """

    def __init__(self) -> None:
        super().__init__(name="silso")

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch daily ISN values from SILSO for the given date range.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with international_sunspot_number populated.

        Raises:
            IngestionError: If the SILSO endpoint cannot be reached or parsed.
        """
        try:
            response = self._get(settings.silso_daily_url)
            observations = self._parse_csv(response.text, start, end)
        except IngestionError:
            raise
        except Exception as exc:
            logger.error("silso_fetch_failed", error=str(exc))
            raise IngestionError(f"Failed to fetch SILSO data: {exc}") from exc

        logger.info(
            "silso_fetch_complete",
            observations=len(observations),
            start=str(start),
            end=str(end),
        )
        return observations

    def _parse_csv(
        self, text: str, start: date, end: date
    ) -> list[SolarObservation]:
        """Parse SILSO semicolon-delimited CSV into observation objects.

        The SILSO daily total sunspot number CSV has the format:
            year;month;day;decimal_year;SNvalue;SNerror;Nb_observations;definitive

        Args:
            text: Raw CSV response body.
            start: Filter start date.
            end: Filter end date.

        Returns:
            Parsed observations within the requested range.
        """
        observations: list[SolarObservation] = []
        reader = csv.reader(io.StringIO(text), delimiter=";")

        for row in reader:
            if not row or len(row) < 5:
                continue

            try:
                year = int(row[0].strip())
                month = int(row[1].strip())
                day = int(row[2].strip())
                obs_date = date(year, month, day)

                if obs_date < start or obs_date > end:
                    continue

                isn_str = row[4].strip()
                isn = float(isn_str) if isn_str and isn_str != "-1" else None

                std_dev_str = row[5].strip() if len(row) > 5 else ""
                std_dev = float(std_dev_str) if std_dev_str and std_dev_str != "-1" else None

                num_obs_str = row[6].strip() if len(row) > 6 else ""
                num_obs = int(num_obs_str) if num_obs_str and num_obs_str != "-1" else None

                provisional = row[7].strip() == "1" if len(row) > 7 else False

                observations.append(
                    SolarObservation(
                        date=obs_date,
                        source=self.name,
                        international_sunspot_number=isn,
                        raw_payload={
                            "isn": isn,
                            "std_dev": std_dev,
                            "num_observations": num_obs,
                            "provisional": provisional,
                        },
                    )
                )
            except (ValueError, IndexError) as exc:
                logger.warning("silso_parse_skip_row", row=row, error=str(exc))
                continue

        return observations