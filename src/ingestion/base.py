"""Abstract base class for solar data source adapters.

All source adapters inherit from SolarDataSource and implement the
fetch() method. The base class provides retry logic, structured logging,
and a consistent interface for the pipeline orchestrator.
"""

from __future__ import annotations

import abc
from datetime import date
from typing import Any

import httpx
import structlog
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import settings

logger = structlog.get_logger()


class SolarObservation(BaseModel):
    """A single day's solar activity observation from one source.

    This is the canonical interchange format between ingestion and processing.
    Fields are optional because not every source provides every metric.
    """

    date: date
    source: str
    ra: float | None = Field(default=None, description="Relative sunspot number (AAVSO)")
    international_sunspot_number: float | None = Field(
        default=None, description="International Sunspot Number (SILSO)"
    )
    f10_7: float | None = Field(
        default=None, description="F10.7 cm radio flux (NOAA)"
    )
    ap_index: float | None = Field(
        default=None, description="Ap geomagnetic index"
    )
    raw_payload: dict[str, Any] = Field(
        default_factory=dict, description="Original unprocessed values for audit"
    )


class SolarDataSource(abc.ABC):
    """Abstract base for all solar data source adapters.

    Subclasses must implement fetch() to return a list of
    SolarObservation objects for the requested date range.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialized HTTP client with shared configuration."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                timeout=settings.request_timeout_seconds,
                follow_redirects=True,
                headers={"User-Agent": "SolarActivityPipeline/0.1 (research)"},
            )
        return self._client

    @abc.abstractmethod
    def fetch(self, start: date, end: date) -> list[SolarObservation]:
        """Fetch observations for the given date range (inclusive).

        Args:
            start: First date to retrieve.
            end: Last date to retrieve.

        Returns:
            List of observations, one per day where data is available.
        """

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=settings.retry_wait_seconds, min=1, max=30),
        reraise=True,
    )
    def _get(self, url: str, **kwargs: Any) -> httpx.Response:
        """HTTP GET with automatic retries on transient failures."""
        log = logger.bind(source=self.name, url=url)
        log.info("fetching_data")
        response = self.client.get(url, **kwargs)
        response.raise_for_status()
        log.info("fetch_complete", status=response.status_code, size=len(response.content))
        return response

    def close(self) -> None:
        """Release HTTP resources."""
        if self._client is not None and not self._client.is_closed:
            self._client.close()

    def __enter__(self) -> SolarDataSource:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class IngestionError(Exception):
    """Raised when a data source cannot be read after all retries."""