"""AAVSO Solar Bulletin adapter.

Fetches the Relative Sunspot Number (Ra) from the AAVSO monthly Solar
Bulletin PDF. Instead of guessing filenames (which AAVSO frequently
misspells or changes), this adapter scrapes the bulletin index page
to discover actual PDF URLs, then reads each PDF's content to determine
which month it contains.

This approach is resilient to AAVSO's naming inconsistencies including:
- Typos (AAVO instead of AAVSO)
- Wrong month in filename (Dec 2025 filed as 2025_11_0)
- Varying separators (underscore vs dash)
- Missing zero-padding (2020_1 instead of 2020_01)
- Completely different paths (Desktop/Solar_Bulletin_May2019.pdf)
- Revision suffixes (_corrected, _r1, _0)
"""

from __future__ import annotations

import io
import re
from datetime import date
from urllib.parse import urljoin

import pdfplumber
import structlog
from bs4 import BeautifulSoup

from src.ingestion.base import IngestionError, SolarDataSource, SolarObservation

logger = structlog.get_logger()

BULLETIN_PAGE = "https://www.aavso.org/solar-bulletin"


class AAVSOSource(SolarDataSource):
    """Adapter for AAVSO Solar Bulletin PDF data.

    Scrapes the bulletin index page to find all PDF links, downloads
    the ones that might contain the months we need, and reads the actual
    month from inside the PDF to handle filename mismatches.
    """

    def __init__(self) -> None:
        super().__init__(name="aavso")
        self._pdf_index: dict[str, str] | None = None  # url -> url, built lazily

    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch Ra values from AAVSO bulletin PDFs for the given date range.

        Args:
            start: First date (inclusive).
            end: Last date (inclusive).

        Returns:
            List of SolarObservation with ra populated.
        """
        months_needed = self._months_in_range(start, end)
        all_pdf_urls = self._discover_pdf_urls()

        all_observations: list[SolarObservation] = []
        months_found: set[tuple[int, int]] = set()

        for year, month in months_needed:
            if (year, month) in months_found:
                continue

            # Find candidate PDFs for this month
            candidates = self._find_candidates(all_pdf_urls, year, month)

            for url in candidates:
                try:
                    observations, actual_month = self._try_pdf(url, start, end)
                    if observations:
                        all_observations.extend(observations)
                        months_found.add(actual_month)
                        # Also mark target month as attempted if different
                        if actual_month != (year, month):
                            months_found.add((year, month))
                        logger.info(
                            "aavso_month_parsed",
                            url=url,
                            actual_year=actual_month[0],
                            actual_month=actual_month[1],
                            observations=len(observations),
                        )
                        break
                except Exception as exc:
                    logger.debug("aavso_candidate_failed", url=url, error=str(exc))
                    continue
            else:
                logger.warning("aavso_month_not_found", year=year, month=month)

        logger.info(
            "aavso_fetch_complete",
            observations=len(all_observations),
            start=str(start),
            end=str(end),
        )
        return all_observations

    def _discover_pdf_urls(self) -> list[str]:
        """Scrape the AAVSO bulletin page for all PDF links.

        Returns:
            List of absolute PDF URLs.
        """
        try:
            response = self._get(BULLETIN_PAGE)
            soup = BeautifulSoup(response.text, "html.parser")

            urls = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href.lower():
                    full_url = urljoin("https://www.aavso.org/", href)
                    urls.append(full_url)

            logger.info("aavso_index_scraped", pdf_count=len(urls))
            return urls
        except Exception as exc:
            logger.error("aavso_index_failed", error=str(exc))
            return []

    def _find_candidates(
        self, all_urls: list[str], year: int, month: int
    ) -> list[str]:
        """Find PDF URLs that might contain data for the given month.

        Looks for the year in the URL and ranks by likelihood. Also
        includes adjacent months since AAVSO sometimes files bulletins
        under the wrong month.

        Args:
            all_urls: All discovered PDF URLs.
            year: Target year.
            month: Target month.

        Returns:
            Candidate URLs, best matches first.
        """
        year_str = str(year)
        month_strs = [f"{month:02d}"]

        # Adjacent months (for misfiled bulletins like Dec filed as Nov_0)
        prev_month = month - 1 if month > 1 else 12
        prev_year = year if month > 1 else year - 1

        exact = []
        adjacent = []
        same_year = []

        for url in all_urls:
            url_lower = url.lower()

            if year_str in url:
                # Exact month match
                if any(f"_{ms}" in url_lower or f"-{ms}" in url_lower for ms in month_strs):
                    exact.append(url)
                # Adjacent month with _0 suffix (CMS versioning artifact)
                elif "_0" in url and (
                    any(f"_{prev_month:02d}" in url or f"_{prev_month}" in url for _ in [1])
                ):
                    adjacent.append(url)
                else:
                    same_year.append(url)
            elif str(prev_year) in url and f"_{prev_month:02d}" in url and "_0" in url:
                adjacent.append(url)

        return exact + adjacent

    def _try_pdf(
        self, url: str, start: date, end: date
    ) -> tuple[list[SolarObservation], tuple[int, int]]:
        """Download a PDF and extract Ra values, detecting the actual month.

        Args:
            url: PDF URL.
            start: Filter start date.
            end: Filter end date.

        Returns:
            Tuple of (observations, (year, month)) where year/month
            are read from inside the PDF, not from the filename.
        """
        response = self._get(url)
        pdf_bytes = io.BytesIO(response.content)

        actual_year, actual_month = self._detect_month(pdf_bytes)
        ra_values = self._extract_ra_table(pdf_bytes)

        observations = []
        for day, ra in ra_values.items():
            try:
                obs_date = date(actual_year, actual_month, day)
                if obs_date < start or obs_date > end:
                    continue
                observations.append(
                    SolarObservation(
                        date=obs_date,
                        source=self.name,
                        ra=float(ra),
                        raw_payload={
                            "year": actual_year,
                            "month": actual_month,
                            "day": day,
                            "ra": ra,
                            "source_url": url,
                        },
                    )
                )
            except ValueError as exc:
                logger.warning(
                    "aavso_bad_date",
                    year=actual_year, month=actual_month, day=day,
                    error=str(exc),
                )

        return observations, (actual_year, actual_month)

    def _detect_month(self, pdf_bytes: io.BytesIO) -> tuple[int, int]:
        """Read the actual month from the PDF content.

        Looks for patterns like "March 2025" or "Volume 81 Number 3"
        on the first page to determine the bulletin's actual month,
        regardless of what the filename says.

        Args:
            pdf_bytes: PDF file content.

        Returns:
            (year, month) as detected from PDF content.

        Raises:
            IngestionError: If month cannot be determined.
        """
        month_names = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }

        pdf_bytes.seek(0)
        with pdfplumber.open(pdf_bytes) as pdf:
            if not pdf.pages:
                raise IngestionError("Empty PDF")

            first_page = pdf.pages[0].extract_text() or ""

            # Pattern: "Month YYYY" (e.g., "December 2025")
            for name, num in month_names.items():
                pattern = re.compile(
                    rf"\b{name}\s+(\d{{4}})\b", re.IGNORECASE
                )
                match = pattern.search(first_page)
                if match:
                    year = int(match.group(1))
                    pdf_bytes.seek(0)
                    return year, num

        pdf_bytes.seek(0)
        raise IngestionError("Could not detect month from PDF content")

    def _extract_ra_table(self, pdf_bytes: io.BytesIO) -> dict[int, float]:
        """Extract daily Ra values from the bulletin PDF.

        Scans all pages for Table 2 rows matching:
            day_number  num_observers  raw_wolf  ra_value

        Args:
            pdf_bytes: PDF file content as BytesIO.

        Returns:
            Dict mapping day number (1-31) to Ra value.
        """
        ra_values: dict[int, float] = {}
        row_pattern = re.compile(
            r"^\s*(\d{1,2})\s+(\d+)\s+(\d+)\s+(\d+)\s*$"
        )

        pdf_bytes.seek(0)
        with pdfplumber.open(pdf_bytes) as pdf:
            in_ra_table = False

            for page in pdf.pages:
                text = page.extract_text()
                if text is None:
                    continue

                for line in text.split("\n"):
                    if "American Relative Sunspot Numbers" in line:
                        in_ra_table = True
                        continue

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
            raise IngestionError(
                "Could not find Ra table in PDF. "
                "AAVSO may have changed their table format. "
                "Look for 'American Relative Sunspot Numbers' in the PDF "
                "and check if the table structure has changed."
            )

        # Sanity check: a valid month should have 28-31 days
        if len(ra_values) < 28:
            logger.warning(
                "aavso_incomplete_table",
                days_found=len(ra_values),
                msg="Expected 28-31 days, got fewer. Table format may have changed.",
            )

        logger.info("aavso_table_extracted", days=len(ra_values))
        return ra_values

    @staticmethod
    def _months_in_range(start: date, end: date) -> list[tuple[int, int]]:
        """Generate list of (year, month) tuples covering the date range."""
        months = []
        current = start.replace(day=1)
        while current <= end:
            months.append((current.year, current.month))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return months