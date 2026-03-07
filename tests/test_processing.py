"""Tests for the processing layer: validation, transformation, and anomaly detection."""

from __future__ import annotations

import os
import tempfile
from datetime import date, timedelta

import pytest

from src.ingestion.base import SolarObservation
from src.processing.anomaly import AnomalyDetector
from src.processing.transformer import Transformer
from src.processing.validator import CrossValidator


class TestCrossValidator:
    """Tests for multi-source cross-validation."""

    def _make_obs(
        self, obs_date: date, source: str, ra: float | None = None, isn: float | None = None
    ) -> SolarObservation:
        return SolarObservation(
            date=obs_date, source=source, ra=ra, international_sunspot_number=isn,
        )

    def test_consistent_values_pass(self):
        """Ra=80 and ISN=82 are within 20% tolerance."""
        observations = [
            self._make_obs(date(2025, 1, 1), "aavso", ra=80.0),
            self._make_obs(date(2025, 1, 1), "silso", isn=82.0),
        ]
        report = CrossValidator(tolerance_pct=20.0).validate(observations)
        assert report.valid_dates == 1
        assert report.invalid_dates == 0

    def test_divergent_values_fail(self):
        """Ra=80 and ISN=20 differ by 75%, should fail."""
        observations = [
            self._make_obs(date(2025, 1, 1), "aavso", ra=80.0),
            self._make_obs(date(2025, 1, 1), "silso", isn=20.0),
        ]
        report = CrossValidator(tolerance_pct=20.0).validate(observations)
        assert report.invalid_dates == 1
        assert any("ra_isn_consistency" in f for f in report.results[0].checks_failed)

    def test_both_zero_passes(self):
        """Solar minimum: both sources report zero."""
        observations = [
            self._make_obs(date(2025, 1, 1), "aavso", ra=0.0),
            self._make_obs(date(2025, 1, 1), "silso", isn=0.0),
        ]
        report = CrossValidator().validate(observations)
        assert report.valid_dates == 1

    def test_missing_source_skips_check(self):
        """Only one source present: consistency check is skipped, not failed."""
        observations = [
            self._make_obs(date(2025, 1, 1), "aavso", ra=80.0),
        ]
        report = CrossValidator().validate(observations)
        assert report.valid_dates == 1

    def test_out_of_range_values_flagged(self):
        """Ra=999 is outside the plausible range [0, 500]."""
        observations = [
            SolarObservation(date=date(2025, 1, 1), source="aavso", ra=999.0),
        ]
        report = CrossValidator().validate(observations)
        assert report.invalid_dates == 1

    def test_coverage_tracking(self):
        """Coverage dict counts how many dates each source covers."""
        observations = [
            self._make_obs(date(2025, 1, 1), "aavso", ra=80.0),
            self._make_obs(date(2025, 1, 1), "silso", isn=82.0),
            self._make_obs(date(2025, 1, 2), "aavso", ra=85.0),
        ]
        report = CrossValidator().validate(observations)
        assert report.coverage["aavso"] == 2
        assert report.coverage["silso"] == 1


class TestTransformer:
    """Tests for data transformation and merging."""

    def test_merge_by_date(self):
        """Two sources on same date get grouped together."""
        observations = [
            SolarObservation(date=date(2025, 1, 1), source="aavso", ra=80.0),
            SolarObservation(date=date(2025, 1, 1), source="silso", international_sunspot_number=82.0),
        ]
        grouped = Transformer().merge_by_date(observations)
        assert len(grouped) == 1
        assert "aavso" in grouped[date(2025, 1, 1)]
        assert "silso" in grouped[date(2025, 1, 1)]

    def test_to_unified_records(self):
        """Three sources merge into one record per date."""
        observations = [
            SolarObservation(date=date(2025, 1, 1), source="aavso", ra=80.0),
            SolarObservation(date=date(2025, 1, 1), source="silso", international_sunspot_number=82.0),
            SolarObservation(date=date(2025, 1, 1), source="noaa", f10_7=150.0, ap_index=10.0),
        ]
        records = Transformer().to_unified_records(observations)
        assert len(records) == 1
        assert records[0]["ra"] == 80.0
        assert records[0]["isn"] == 82.0
        assert records[0]["f10_7"] == 150.0

    def test_unified_records_sorted_by_date(self):
        """Output is always sorted chronologically."""
        observations = [
            SolarObservation(date=date(2025, 1, 3), source="aavso", ra=90.0),
            SolarObservation(date=date(2025, 1, 1), source="aavso", ra=80.0),
            SolarObservation(date=date(2025, 1, 2), source="aavso", ra=85.0),
        ]
        records = Transformer().to_unified_records(observations)
        dates = [r["date"] for r in records]
        assert dates == sorted(dates)

    def test_export_dat(self):
        """Exported .dat file has correct format and content."""
        records = [
            {"date": date(2025, 1, 1), "ra": 80.0, "isn": 82.0, "f10_7": 150.0},
            {"date": date(2025, 1, 2), "ra": None, "isn": None, "f10_7": None},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            import src.config
            original_dir = src.config.settings.dat_output_dir
            src.config.settings.dat_output_dir = tmpdir

            try:
                path = Transformer().export_dat(records, run_number=76, mdi_day_start=5401)
                assert os.path.exists(path)

                with open(path) as f:
                    lines = f.readlines()

                assert len(lines) == 2
                assert "5401" in lines[0]
                assert "80.0" in lines[0]
                assert "-1.0" in lines[1]  # None becomes -1.0
            finally:
                src.config.settings.dat_output_dir = original_dir


class TestAnomalyDetector:
    """Tests for statistical anomaly detection."""

    def _make_records(self, n: int, base_ra: float = 80.0) -> list[dict]:
        """Generate n days of stable data with small variation."""
        start = date(2025, 1, 1)
        return [
            {
                "date": start + timedelta(days=i),
                "ra": base_ra + (i % 5),
                "isn": None,
                "f10_7": None,
                "ap_index": None,
            }
            for i in range(n)
        ]

    def test_no_anomalies_in_stable_data(self):
        """Stable data should produce no flags."""
        records = self._make_records(60)
        flags = AnomalyDetector(zscore_threshold=3.0, window_size=30).detect(records)
        assert len(flags) == 0

    def test_detects_spike(self):
        """A sudden jump to 500 should be flagged."""
        records = self._make_records(50)
        records[45]["ra"] = 500.0
        flags = AnomalyDetector(zscore_threshold=3.0, window_size=30).detect(records)
        spike_flags = [f for f in flags if f.value == 500.0]
        assert len(spike_flags) == 1

    def test_critical_severity_for_extreme_outlier(self):
        """Extremely large value gets critical severity."""
        records = self._make_records(50)
        records[45]["ra"] = 9999.0
        flags = AnomalyDetector(zscore_threshold=3.0, window_size=30).detect(records)
        critical = [f for f in flags if f.severity == "critical"]
        assert len(critical) >= 1

    def test_insufficient_data_returns_empty(self):
        """Less data than window size returns empty list."""
        records = self._make_records(10)
        flags = AnomalyDetector(window_size=30).detect(records)
        assert len(flags) == 0

    def test_null_values_skipped(self):
        """None values don't crash the detector."""
        records = self._make_records(60)
        for r in records[20:40]:
            r["ra"] = None
        flags = AnomalyDetector(zscore_threshold=3.0, window_size=30).detect(records)
        assert isinstance(flags, list)