"""SQLAlchemy model for the Discogs CSV Records table.

This table stores Discogs CSV upload metadata and row data. Records
track CSV files generated for Discogs inventory uploads.

Migrates from: data/exchange/discogs_csv/*.csv and *.meta.json
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from database.models.base import Base, Mapped, mapped_column


class DiscogsCsvRecord(Base):
    """Discogs CSV upload record.

    Stores metadata and row data for CSV files generated for Discogs
    inventory uploads. Tracks upload status and API responses.

    Attributes:
        record_id: Unique identifier for the record (UUID).
        action: CSV action type ('add', 'change', 'delete').
        rows: CSV row data as JSONB array.
        idempotency_token: Token linking to originating event (nullable).
        created_at: Timestamp when the CSV was generated.
        uploaded_at: Timestamp when the CSV was uploaded (nullable).
        upload_response: Discogs API upload response as JSONB (nullable).

    Indexes:
        - idx_discogs_csv_action: For filtering by action type.
        - idx_discogs_csv_created_at: For time-based queries.
        - idx_discogs_csv_idempotency_token: For linking to events.
    """

    __tablename__ = "discogs_csv_records"

    record_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="Unique identifier for the record",
    )

    action: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="CSV action type ('add', 'change', 'delete')",
    )

    rows: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        doc="CSV row data as JSONB array",
    )

    idempotency_token: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True,
        doc="Token linking to originating event",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        doc="Timestamp when the CSV was generated",
    )

    uploaded_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
        doc="Timestamp when the CSV was uploaded",
    )

    upload_response: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
        doc="Discogs API upload response as JSONB",
    )

    __table_args__ = (
        Index("idx_discogs_csv_action", "action"),
        Index("idx_discogs_csv_created_at", "created_at"),
        Index("idx_discogs_csv_idempotency_token", "idempotency_token"),
    )

    def __repr__(self) -> str:
        rows_count = len(self.rows) if self.rows else 0
        return (
            f"<DiscogsCsvRecord(record_id={self.record_id}, "
            f"action={self.action}, rows={rows_count})>"
        )
