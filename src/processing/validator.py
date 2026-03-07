"""Multi-source cross-validation for solar activity data.

Compares observations from different sources to detect discrepancies that
may indicate data quality issues. The primary check compares AAVSO Ra values
against SILSO International Sunspot Numbers, which should track each other
within a configurable tolerance.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import structlog

from src.config import settings
from src.ingestion.base import SolarObservation

logger = structlog.get_logger()


@dataclass
class ValidationResult:
    """Result of a cross-validation check for a single date."""

    date: date
    is_valid: bool
    checks_passed: list[str] = field(default_factory=list)
    checks_failed: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Aggregate validation report for a batch of observations."""

    total_dates: int
    valid_dates: int
    invalid_dates: int
    results: list[ValidationResult]
    coverage: dict[str, int]  # source_name -> number of dates with data


class CrossValidator:
    """Validates consistency across multiple solar data sources.

    The primary validation compares AAVSO Relative Sunspot Number (Ra) against
    SILSO International Sunspot Number (ISN). While these use different methods,
    they should correlate strongly. Large deviations flag potential data issues.
    """

    def __init__(self, tolerance_pct: float | None = None) -> None:
        self.tolerance_pct = tolerance_pct or settings.ra_isn_tolerance_percent

    def validate(
        self, observations: list[SolarObservation]
    ) -> ValidationReport:
        """Run all validation checks on the observation set.

        Args:
            observations: Flat list from all sources.

        Returns:
            Complete validation report.
        """
        by_date: dict[date, dict[str, SolarObservation]] = {}
        for obs in observations:
            by_date.setdefault(obs.date, {})[obs.source] = obs

        coverage: dict[str, int] = {}
        for obs in observations:
            coverage[obs.source] = coverage.get(obs.source, 0) + 1

        results: list[ValidationResult] = []
        for obs_date in sorted(by_date.keys()):
            sources = by_date[obs_date]
            result = self._validate_date(obs_date, sources)
            results.append(result)

        valid_count = sum(1 for r in results if r.is_valid)
        report = ValidationReport(
            total_dates=len(results),
            valid_dates=valid_count,
            invalid_dates=len(results) - valid_count,
            results=results,
            coverage=coverage,
        )

        logger.info(
            "validation_complete",
            total=report.total_dates,
            valid=report.valid_dates,
            invalid=report.invalid_dates,
        )
        return report

    def _validate_date(
        self, obs_date: date, sources: dict[str, SolarObservation]
    ) -> ValidationResult:
        """Run all checks for a single date."""
        result = ValidationResult(date=obs_date, is_valid=True)
        self._check_ra_isn_consistency(sources, result)
        self._check_value_ranges(sources, result)
        return result

    def _check_ra_isn_consistency(
        self, sources: dict[str, SolarObservation], result: ValidationResult
    ) -> None:
        """Compare AAVSO Ra against SILSO ISN for consistency.

        Both measure sunspot activity, so they should track within tolerance.
        When both are zero (solar minimum), the check automatically passes.
        """
        aavso = sources.get("aavso")
        silso = sources.get("silso")

        if not aavso or not silso:
            result.checks_passed.append("ra_isn_consistency: skipped (missing source)")
            return

        ra = aavso.ra
        isn = silso.international_sunspot_number

        if ra is None or isn is None:
            result.checks_passed.append("ra_isn_consistency: skipped (null values)")
            return

        if ra == 0 and isn == 0:
            result.checks_passed.append("ra_isn_consistency: both zero")
            return

        reference = max(abs(ra), abs(isn), 1.0)
        deviation_pct = abs(ra - isn) / reference * 100

        result.details["ra_isn_deviation_pct"] = round(deviation_pct, 2)

        if deviation_pct <= self.tolerance_pct:
            result.checks_passed.append(
                f"ra_isn_consistency: {deviation_pct:.1f}% (within {self.tolerance_pct}%)"
            )
        else:
            result.is_valid = False
            result.checks_failed.append(
                f"ra_isn_consistency: {deviation_pct:.1f}% "
                f"(exceeds {self.tolerance_pct}%) Ra={ra}, ISN={isn}"
            )

    def _check_value_ranges(
        self, sources: dict[str, SolarObservation], result: ValidationResult
    ) -> None:
        """Verify that values fall within physically plausible ranges.

        Solar activity indices have known bounds. Values outside these ranges
        likely indicate data corruption or parsing errors.
        """
        range_checks = {
            "ra": (0, 500),
            "international_sunspot_number": (0, 500),
            "f10_7": (50, 500),
            "ap_index": (0, 400),
        }

        for obs in sources.values():
            for field_name, (low, high) in range_checks.items():
                value = getattr(obs, field_name, None)
                if value is None:
                    continue

                check_name = f"range_{obs.source}_{field_name}"
                if low <= value <= high:
                    result.checks_passed.append(f"{check_name}: {value} in [{low}, {high}]")
                else:
                    result.is_valid = False
                    result.checks_failed.append(
                        f"{check_name}: {value} outside [{low}, {high}]"
                    )