"""SQLAlchemy model for the Events table.

This table stores webhook events (ProductAdd, ProductQuantity, ProductDelete, etc.)
received from BaseLinker with their processing status. Events are queued for
processing by Airflow DAGs.

Migrates from: data/exchange/events/evt_*.json
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from database.models.base import Base, Mapped, mapped_column


class Event(Base):
    """Webhook event record with processing status.

    Stores incoming webhook events from BaseLinker with their payloads
    and processing status. Events are processed by Airflow DAGs and
    their status is updated accordingly.

    Attributes:
        event_id: Unique identifier for the event (UUID).
        action: Event action type (e.g., 'ProductAdd', 'ProductQuantity').
        payload: Full webhook payload as JSONB.
        status: Processing status ('queued', 'processed', 'failed', 'OK').
        created_at: Timestamp when the event was received.
        processed_at: Timestamp when the event was processed (nullable).
        idempotency_token: SHA256 digest for duplicate detection (nullable).
        correlation_id: ID to track related events in same request (nullable).

    Indexes:
        - idx_events_status: For filtering by processing status.
        - idx_events_created_at: For time-based queries and ordering.
        - idx_events_idempotency_token: For fast duplicate detection.
        - idx_events_correlation_id: For tracking related events.
    """

    __tablename__ = "events"

    event_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="Unique identifier for the event",
    )

    action: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Event action type (e.g., 'ProductAdd', 'ProductQuantity')",
    )

    payload: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        doc="Full webhook payload as JSONB",
    )

    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="queued",
        doc="Processing status ('queued', 'processed', 'failed', 'OK')",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        doc="Timestamp when the event was received",
    )

    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
        doc="Timestamp when the event was processed",
    )

    idempotency_token: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
        doc="SHA256 digest for duplicate detection",
    )

    correlation_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        doc="ID to track related events in same request",
    )

    __table_args__ = (
        Index("idx_events_status", "status"),
        Index("idx_events_created_at", "created_at"),
        Index("idx_events_idempotency_token", "idempotency_token"),
        Index("idx_events_correlation_id", "correlation_id"),
    )

    def __repr__(self) -> str:
        return f"<Event(event_id={self.event_id}, action={self.action}, status={self.status})>"
