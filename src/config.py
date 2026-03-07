"""Centralized configuration for the solar activity pipeline.

Uses pydantic-settings to load from environment variables with sensible
defaults for local development.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database (先用 SQLite，后面再切 PostgreSQL)
    database_url: str = "sqlite:///data/solar_pipeline.db"

    # Logging
    log_level: str = "INFO"

    # HTTP requests
    request_timeout_seconds: int = 30
    max_retries: int = 3
    retry_wait_seconds: float = 2.0

    # SILSO data source
    silso_daily_url: str = "https://www.sidc.be/SILSO/INFO/sndtotcsv.php"

    # NOAA SWPC data source
    noaa_solar_indices_url: str = (
        "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json"
    )

    # AAVSO data source
    aavso_bulletin_url: str = "https://www.aavso.org/solar-bulletin"

    # Anomaly detection
    anomaly_zscore_threshold: float = 3.0

    # Cross-validation tolerance
    ra_isn_tolerance_percent: float = 20.0

    # Export
    dat_output_dir: str = "data/output"


settings = Settings()