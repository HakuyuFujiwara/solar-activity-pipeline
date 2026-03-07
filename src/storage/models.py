"""SQLAlchemy ORM models for solar activity data.

Defines the database schema for storing ingested observations, validation
results, and pipeline run metadata. Uses SQLAlchemy 2.0 mapped_column style.
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class SolarObservationRecord(Base):
    """A single solar activity observation from one source on one date.

    The (date, source) pair is unique: each source contributes at most one
    record per day. Upsert logic in the database layer ensures idempotency.
    """

    __tablename__ = "solar_observations"
    __table_args__ = (
        UniqueConstraint("observation_date", "source", name="uq_date_source"),
        Index("ix_obs_date", "observation_date"),
        Index("ix_obs_source", "source"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    observation_date: Mapped[date] = mapped_column(Date, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)

    # Solar activity values (nullable because not every source has every field)
    ra: Mapped[float | None] = mapped_column(Float, nullable=True)
    international_sunspot_number: Mapped[float | None] = mapped_column(Float, nullable=True)
    f10_7: Mapped[float | None] = mapped_column(Float, nullable=True)
    ap_index: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Store raw data for audit trail
    raw_payload: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )


class AnomalyRecord(Base):
    """A detected anomaly in the solar activity time series."""

    __tablename__ = "anomalies"
    __table_args__ = (
        Index("ix_anomaly_date", "observation_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    observation_date: Mapped[date] = mapped_column(Date, nullable=False)
    field: Mapped[str] = mapped_column(String(50), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    zscore: Mapped[float] = mapped_column(Float, nullable=False)
    mean: Mapped[float] = mapped_column(Float, nullable=False)
    std: Mapped[float] = mapped_column(Float, nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )


class PipelineRun(Base):
    """Record of a pipeline execution for observability."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    date_range_start: Mapped[date] = mapped_column(Date, nullable=False)
    date_range_end: Mapped[date] = mapped_column(Date, nullable=False)
    observations_ingested: Mapped[int] = mapped_column(Integer, default=0)
    anomalies_detected: Mapped[int] = mapped_column(Integer, default=0)
    is_dry_run: Mapped[bool] = mapped_column(Boolean, default=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)