"""Statistical anomaly detection for solar activity time series.

Uses Z-score analysis to identify observations that deviate significantly
from the recent trend. This catches sudden jumps in reported values that
may indicate instrument issues, reporting errors, or genuine solar events
worth flagging for manual review.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import structlog

from src.config import settings

logger = structlog.get_logger()


@dataclass
class AnomalyFlag:
    """A single flagged anomaly in the time series."""

    date: date
    field: str
    value: float
    zscore: float
    mean: float
    std: float
    severity: str  # "warning" or "critical"


class AnomalyDetector:
    """Detects anomalous values in solar activity time series using Z-scores.

    Computes a rolling window Z-score for each metric. Values exceeding the
    configured threshold are flagged. A secondary "critical" threshold at 2x
    the warning threshold identifies extreme outliers.

    The rolling window approach accounts for the natural ~11-year solar cycle
    variation, so high activity during solar maximum is not flagged as anomalous.
    """

    def __init__(
        self,
        zscore_threshold: float | None = None,
        window_size: int = 30,
    ) -> None:
        """Initialize the anomaly detector.

        Args:
            zscore_threshold: Z-score above which a value is flagged.
            window_size: Number of preceding days for rolling stats.
        """
        self.threshold = zscore_threshold or settings.anomaly_zscore_threshold
        self.critical_threshold = self.threshold * 2
        self.window_size = window_size

    def detect(self, records: list[dict]) -> list[AnomalyFlag]:
        """Scan unified records for anomalous values.

        Args:
            records: Unified daily records, each with keys:
                date, ra, isn, f10_7, ap_index.

        Returns:
            List of anomaly flags, sorted by date.
        """
        if len(records) < self.window_size + 1:
            logger.info("anomaly_detect_skip", reason="insufficient data", count=len(records))
            return []

        flags: list[AnomalyFlag] = []
        fields = ["ra", "isn", "f10_7", "ap_index"]

        for field in fields:
            field_flags = self._detect_field(records, field)
            flags.extend(field_flags)

        flags.sort(key=lambda f: f.date)
        logger.info("anomaly_detection_complete", total_flags=len(flags))
        return flags

    def _detect_field(self, records: list[dict], field: str) -> list[AnomalyFlag]:
        """Run Z-score anomaly detection on a single field.

        For each day, computes the mean and std of the preceding window_size
        days. If the current value is more than threshold standard deviations
        from the mean, it's flagged.

        Args:
            records: Unified daily records.
            field: Field name to analyze.

        Returns:
            Anomaly flags for this field.
        """
        values = [(r["date"], r.get(field)) for r in records]
        non_null = [(d, v) for d, v in values if v is not None]

        if len(non_null) < self.window_size + 1:
            return []

        flags: list[AnomalyFlag] = []
        numeric_values = np.array([v for _, v in non_null], dtype=np.float64)

        for i in range(self.window_size, len(non_null)):
            window = numeric_values[i - self.window_size : i]
            mean = float(np.mean(window))
            std = float(np.std(window))

            if std < 1e-10:
                continue

            current_date, current_value = non_null[i]
            zscore = abs(current_value - mean) / std

            if zscore >= self.critical_threshold:
                flags.append(
                    AnomalyFlag(
                        date=current_date,
                        field=field,
                        value=current_value,
                        zscore=round(zscore, 2),
                        mean=round(mean, 2),
                        std=round(std, 2),
                        severity="critical",
                    )
                )
            elif zscore >= self.threshold:
                flags.append(
                    AnomalyFlag(
                        date=current_date,
                        field=field,
                        value=current_value,
                        zscore=round(zscore, 2),
                        mean=round(mean, 2),
                        std=round(std, 2),
                        severity="warning",
                    )
                )

        return flags