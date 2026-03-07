"""Tests for data source ingestion adapters.

Uses respx to mock HTTP responses so tests run without network access
and are deterministic.
"""

from datetime import date
import httpx
import pytest
import respx
from src.ingestion.base import IngestionError, SolarObservation
from src.ingestion.silso import SILSOSource
from src.ingestion.noaa import NOAASource
from src.ingestion.aavso import AAVSOSource


class TestSolarObservation:
    """Tests for the SolarObservation data model."""

    def test_minimal_observation(self):
        """Only date and source are required, everything else is None."""
        obs = SolarObservation(date=date(2025, 1, 1), source="test")
        assert obs.date == date(2025, 1, 1)
        assert obs.source == "test"
        assert obs.ra is None
        assert obs.f10_7 is None

    def test_full_observation(self):
        """All fields can be populated."""
        obs = SolarObservation(
            date=date(2025, 1, 1),
            source="aavso",
            ra=42.5,
            international_sunspot_number=45.0,
            f10_7=120.3,
            ap_index=8.0,
            raw_payload={"test": True},
        )
        assert obs.ra == 42.5
        assert obs.raw_payload == {"test": True}


class TestSILSOSource:
    """Tests for the SILSO International Sunspot Number adapter."""

    # 伪造的 SILSO CSV 数据，格式和真实数据一样
    SAMPLE_CSV = (
        "2025; 1; 1;2025.001; 198.0; 7.2; 22;1\n"
        "2025; 1; 2;2025.004; 187.0; 6.8; 20;1\n"
        "2025; 1; 3;2025.007; 199.0; 8.1; 23;0\n"
    )

    @respx.mock
    def test_fetch_parses_csv(self):
        """Normal case: 3 rows of valid data."""
        respx.get("https://www.sidc.be/SILSO/INFO/sndtotcsv.php").mock(
            return_value=httpx.Response(200, text=self.SAMPLE_CSV)
        )

        with SILSOSource() as source:
            result = source.fetch(date(2025, 1, 1), date(2025, 1, 3))

        assert len(result) == 3
        assert result[0].international_sunspot_number == 198.0
        assert result[0].date == date(2025, 1, 1)
        assert result[0].source == "silso"

    @respx.mock
    def test_fetch_filters_by_date_range(self):
        """Only returns data within the requested range."""
        csv_data = (
            "2025; 1; 1;2025.001; 198.0; 7.2; 22;1\n"
            "2025; 1; 2;2025.004; 187.0; 6.8; 20;1\n"
            "2025; 1; 3;2025.007; 199.0; 8.1; 23;0\n"
        )
        respx.get("https://www.sidc.be/SILSO/INFO/sndtotcsv.php").mock(
            return_value=httpx.Response(200, text=csv_data)
        )

        with SILSOSource() as source:
            # 只要 1月2日 这一天
            result = source.fetch(date(2025, 1, 2), date(2025, 1, 2))

        assert len(result) == 1
        assert result[0].date == date(2025, 1, 2)

    @respx.mock
    def test_fetch_handles_missing_values(self):
        """-1 in SILSO means missing data, should become None."""
        csv_data = "2025; 1; 1;2025.001;  -1; -1; -1;1\n"
        respx.get("https://www.sidc.be/SILSO/INFO/sndtotcsv.php").mock(
            return_value=httpx.Response(200, text=csv_data)
        )

        with SILSOSource() as source:
            result = source.fetch(date(2025, 1, 1), date(2025, 1, 1))

        assert len(result) == 1
        assert result[0].international_sunspot_number is None

    @respx.mock
    def test_fetch_handles_empty_response(self):
        """Empty response returns empty list, not an error."""
        respx.get("https://www.sidc.be/SILSO/INFO/sndtotcsv.php").mock(
            return_value=httpx.Response(200, text="")
        )

        with SILSOSource() as source:
            result = source.fetch(date(2025, 1, 1), date(2025, 1, 7))

        assert len(result) == 0

    @respx.mock
    def test_fetch_raises_on_server_error(self):
        """500 error should raise IngestionError after retries."""
        respx.get("https://www.sidc.be/SILSO/INFO/sndtotcsv.php").mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        with SILSOSource() as source:
            with pytest.raises(IngestionError):
                source.fetch(date(2025, 1, 1), date(2025, 1, 7))

    @respx.mock
    def test_fetch_skips_malformed_rows(self):
        """Bad rows are skipped, good rows still get parsed."""
        csv_data = (
            "2025; 1; 1;2025.001; 198.0; 7.2; 22;1\n"
            "garbage;not;a;valid;row\n"
            "2025; 1; 3;2025.007; 199.0; 8.1; 23;0\n"
        )
        respx.get("https://www.sidc.be/SILSO/INFO/sndtotcsv.php").mock(
            return_value=httpx.Response(200, text=csv_data)
        )

        with SILSOSource() as source:
            result = source.fetch(date(2025, 1, 1), date(2025, 1, 3))

        assert len(result) == 2

class TestNOAASource:
    """Tests for the NOAA SWPC adapter."""

    SAMPLE_JSON = [
        {"time-tag": "2025-01", "f10.7": 189.39, "ssn": 137.0, "ap": 12.0},
        {"time-tag": "2025-02", "f10.7": 184.31, "ssn": 155.7, "ap": 10.0},
    ]

    @respx.mock
    def test_fetch_parses_json(self):
        """Normal case: 2 months of valid data."""
        respx.get(
            "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json"
        ).mock(return_value=httpx.Response(200, json=self.SAMPLE_JSON))

        with NOAASource() as source:
            result = source.fetch(date(2025, 1, 1), date(2025, 2, 28))

        assert len(result) == 2
        assert result[0].f10_7 == 189.39
        assert result[0].international_sunspot_number == 137.0
        assert result[0].source == "noaa"

    @respx.mock
    def test_fetch_handles_missing_values(self):
        """Empty strings and None in JSON should become None."""
        records = [
            {"time-tag": "2025-01", "f10.7": "", "ssn": None, "ap": 12.0},
        ]
        respx.get(
            "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json"
        ).mock(return_value=httpx.Response(200, json=records))

        with NOAASource() as source:
            result = source.fetch(date(2025, 1, 1), date(2025, 1, 31))

        assert len(result) == 1
        assert result[0].f10_7 is None
        assert result[0].international_sunspot_number is None
        assert result[0].ap_index == 12.0

    @respx.mock
    def test_fetch_raises_on_server_error(self):
        """500 error should raise IngestionError."""
        respx.get(
            "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json"
        ).mock(return_value=httpx.Response(500, text="Server Error"))

        with NOAASource() as source:
            with pytest.raises(IngestionError):
                source.fetch(date(2025, 1, 1), date(2025, 1, 31))

class TestAAVSOSource:
    """Tests for the AAVSO Solar Bulletin PDF adapter."""

    def test_months_in_range_single_month(self):
        """Single month range."""
        from src.ingestion.aavso import AAVSOSource
        months = AAVSOSource._months_in_range(date(2025, 3, 1), date(2025, 3, 31))
        assert months == [(2025, 3)]

    def test_months_in_range_cross_year(self):
        """Range spanning December to January."""
        from src.ingestion.aavso import AAVSOSource
        months = AAVSOSource._months_in_range(date(2024, 11, 15), date(2025, 2, 10))
        assert months == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]

    @respx.mock
    def test_fetch_raises_on_server_error(self):
        """404 for missing bulletin should not crash pipeline."""
        respx.get(
            "https://www.aavso.org/sites/default/files/solar_bulletin/AAVSO_SB_2025_03.pdf"
        ).mock(return_value=httpx.Response(404, text="Not Found"))

        with AAVSOSource() as source:
            # Should return empty list, not raise, because of graceful degradation
            result = source.fetch(date(2025, 3, 1), date(2025, 3, 7))

        assert len(result) == 0