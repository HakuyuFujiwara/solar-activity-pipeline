"""Database connection management and data access operations.

Provides a Database class that manages the SQLAlchemy engine and session,
creates tables on initialization, and offers upsert methods for idempotent
data loading. Uses SQLite for local development, can switch to PostgreSQL
by changing DATABASE_URL.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import structlog
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from src.config import settings
from src.ingestion.base import SolarObservation
from src.storage.models import (
    AnomalyRecord,
    Base,
    PipelineRun,
    SolarObservationRecord,
)

logger = structlog.get_logger()


class Database:
    """Manages database connections and provides data access methods.

    Handles table creation, upsert operations for observations, and query
    methods for the dashboard.
    """

    def __init__(self, url: str | None = None) -> None:
        self.url = url or settings.database_url
        self.engine = create_engine(self.url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self) -> None:
        """Create all tables if they don't exist."""
        Base.metadata.create_all(self.engine)
        logger.info("database_tables_created")

    def get_session(self) -> Session:
        """Create a new database session."""
        return self.SessionLocal()

    def upsert_observations(self, observations: list[SolarObservation]) -> int:
        """Insert or update observations.

        For each observation, checks if a record with the same (date, source)
        already exists. If so, updates it. If not, inserts a new row.
        This makes pipeline re-runs safe and idempotent.

        Args:
            observations: List of observations to persist.

        Returns:
            Number of rows affected.
        """
        if not observations:
            return 0

        count = 0
        with self.get_session() as session:
            for obs in observations:
                existing = session.scalars(
                    select(SolarObservationRecord).where(
                        SolarObservationRecord.observation_date == obs.date,
                        SolarObservationRecord.source == obs.source,
                    )
                ).first()

                if existing:
                    existing.ra = obs.ra
                    existing.international_sunspot_number = obs.international_sunspot_number
                    existing.f10_7 = obs.f10_7
                    existing.ap_index = obs.ap_index
                    existing.raw_payload = json.dumps(obs.raw_payload) if obs.raw_payload else None
                    existing.updated_at = datetime.utcnow()
                else:
                    record = SolarObservationRecord(
                        observation_date=obs.date,
                        source=obs.source,
                        ra=obs.ra,
                        international_sunspot_number=obs.international_sunspot_number,
                        f10_7=obs.f10_7,
                        ap_index=obs.ap_index,
                        raw_payload=json.dumps(obs.raw_payload) if obs.raw_payload else None,
                    )
                    session.add(record)

                count += 1

            session.commit()
            logger.info("observations_upserted", count=count)
            return count

    def save_anomalies(self, flags: list) -> int:
        """Persist detected anomalies.

        Args:
            flags: AnomalyFlag objects from the detector.

        Returns:
            Number of anomalies saved.
        """
        if not flags:
            return 0

        with self.get_session() as session:
            records = [
                AnomalyRecord(
                    observation_date=f.date,
                    field=f.field,
                    value=f.value,
                    zscore=f.zscore,
                    mean=f.mean,
                    std=f.std,
                    severity=f.severity,
                )
                for f in flags
            ]
            session.add_all(records)
            session.commit()
            logger.info("anomalies_saved", count=len(records))
            return len(records)

    def create_pipeline_run(
        self, start: date, end: date, dry_run: bool = False
    ) -> int:
        """Create a pipeline run record."""
        with self.get_session() as session:
            run = PipelineRun(
                date_range_start=start,
                date_range_end=end,
                is_dry_run=dry_run,
                status="running",
            )
            session.add(run)
            session.commit()
            return run.id

    def complete_pipeline_run(
        self,
        run_id: int,
        status: str = "success",
        observations: int = 0,
        anomalies: int = 0,
        error: str | None = None,
    ) -> None:
        """Update a pipeline run record on completion."""
        with self.get_session() as session:
            run = session.get(PipelineRun, run_id)
            if run:
                run.status = status
                run.finished_at = datetime.utcnow()
                run.observations_ingested = observations
                run.anomalies_detected = anomalies
                run.error_message = error
                session.commit()

    def query_observations(
        self, start: date, end: date, source: str | None = None
    ) -> list[SolarObservationRecord]:
        """Query stored observations for the dashboard."""
        with self.get_session() as session:
            stmt = select(SolarObservationRecord).where(
                SolarObservationRecord.observation_date >= start,
                SolarObservationRecord.observation_date <= end,
            )
            if source:
                stmt = stmt.where(SolarObservationRecord.source == source)
            stmt = stmt.order_by(SolarObservationRecord.observation_date)
            return list(session.scalars(stmt).all())