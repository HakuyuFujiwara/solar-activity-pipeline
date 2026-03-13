"""AAVSO Solar Bulletin adapter.

Fetches the Relative Sunspot Number (Ra) from the AAVSO monthly Solar
Bulletin PDF. The bulletin is published as a PDF at a predictable URL:
    https://www.aavso.org/sites/default/files/solar_bulletin/AAVSO_SB_YYYY_MM.pdf

The Ra values are in "Table 2: American Relative Sunspot Numbers (Ra)",
which has columns: Day, Number of Observers, Raw, Ra.

This is the same PDF you would manually download when generating daily
activity values files for the HMI pipeline.
"""

from __future__ import annotations

import io
import re
from datetime import date

import pdfplumber
import structlog

from src.ingestion.base import IngestionError, SolarDataSource, SolarObservation

logger = structlog.get_logger()


class AAVSOSource(SolarDataSource):
    """Adapter for AAVSO Solar Bulletin PDF data.

    Downloads monthly bulletin PDFs and extracts the daily Ra values from
    Table 2. This automates the manual process of opening the PDF and
    reading off Ra values for each day.
    """

    BASE_URL = "https://www.aavso.org/sites/default/files/solar_bulletin"

    def __init__(self) -> None:
        super().__init__(name="aavso")

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch Ra values from AAVSO bulletin PDFs for the given date range.

        Downloads one PDF per month covered by the date range, extracts
        Table 2, and returns daily Ra observations.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with ra populated.

        Raises:
            IngestionError: If no bulletins can be retrieved or parsed.
        """
        months = self._months_in_range(start, end)
        all_observations: list[SolarObservation] = []

        for year, month in months:
            try:
                observations = self._fetch_month(year, month)
                # Filter to requested date range
                filtered = [
                    obs for obs in observations
                    if start <= obs.date <= end
                ]
                all_observations.extend(filtered)
                logger.info(
                    "aavso_month_parsed",
                    year=year,
                    month=month,
                    observations=len(filtered),
                )
            except Exception as exc:
                # Some months may not be published yet, skip them
                logger.warning(
                    "aavso_month_failed",
                    year=year,
                    month=month,
                    error=str(exc),
                )

        logger.info(
            "aavso_fetch_complete",
            observations=len(all_observations),
            start=str(start),
            end=str(end),
        )
        return all_observations

    def _fetch_month(self, year: int, month: int) -> list[SolarObservation]:
        """Download and parse one month's bulletin PDF.

        Args:
            year: 4-digit year.
            month: Month number (1-12).

        Returns:
            List of daily Ra observations for that month.
        """
        # Try standard filename first, then common typos
        urls_to_try = [
            f"{self.BASE_URL}/AAVSO_SB_{year}_{month:02d}.pdf",
            f"{self.BASE_URL}/AAVO_SB_{year}_{month:02d}.pdf",
            f"{self.BASE_URL}/AAVSO_SB_{year}_{month}.pdf",
        ]

        response = None
        for url in urls_to_try:
            try:
                response = self._get(url)
                break
            except Exception:
                continue

        if response is None:
            raise IngestionError(f"Could not fetch AAVSO bulletin for {year}-{month:02d}")
        
        response = self._get(url)

        pdf_bytes = io.BytesIO(response.content)
        ra_values = self._extract_ra_table(pdf_bytes)

        observations = []
        for day, ra in ra_values.items():
            try:
                obs_date = date(year, month, day)
                observations.append(
                    SolarObservation(
                        date=obs_date,
                        source=self.name,
                        ra=float(ra),
                        raw_payload={"year": year, "month": month, "day": day, "ra": ra},
                    )
                )
            except ValueError as exc:
                logger.warning("aavso_bad_date", year=year, month=month, day=day, error=str(exc))

        return observations

    def _extract_ra_table(self, pdf_bytes: io.BytesIO) -> dict[int, float]:
        """Extract daily Ra values from the bulletin PDF.

        Scans all pages for lines matching the Table 2 format:
        day_number  num_observers  raw_wolf  ra_value

        Also handles the "Averages" row at the end of the table.

        Args:
            pdf_bytes: PDF file content as BytesIO.

        Returns:
            Dict mapping day number (1-31) to Ra value.
        """
        ra_values: dict[int, float] = {}

        # Pattern matches rows like: "1 27 106 76" or " 16 26 186 131"
        row_pattern = re.compile(
            r"^\s*(\d{1,2})\s+(\d+)\s+(\d+)\s+(\d+)\s*$"
        )

        with pdfplumber.open(pdf_bytes) as pdf:
            in_ra_table = False

            for page in pdf.pages:
                text = page.extract_text()
                if text is None:
                    continue

                for line in text.split("\n"):
                    # Detect start of Table 2
                    if "American Relative Sunspot Numbers" in line:
                        in_ra_table = True
                        continue

                    # Detect end of table (Averages row or next section)
                    if in_ra_table and "Averages" in line:
                        in_ra_table = False
                        continue

                    if in_ra_table:
                        match = row_pattern.match(line)
                        if match:
                            day = int(match.group(1))
                            ra = float(match.group(4))
                            if 1 <= day <= 31:
                                ra_values[day] = ra

        if not ra_values:
            raise IngestionError("Could not find Ra table in PDF")

        logger.info("aavso_table_extracted", days=len(ra_values))
        return ra_values

    @staticmethod
    def _months_in_range(start: date, end: date) -> list[tuple[int, int]]:
        """Generate list of (year, month) tuples covering the date range.

        Args:
            start: Start date.
            end: End date.

        Returns:
            List of (year, month) tuples.
        """
        months = []
        current = start.replace(day=1)
        while current <= end:
            months.append((current.year, current.month))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return months