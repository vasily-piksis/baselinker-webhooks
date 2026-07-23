"""SQLAlchemy model for the Idempotency Records table.

This table stores idempotency digests and cached results to prevent
duplicate processing of events. Uses SHA256 digest as the primary key
for fast lookup.

Migrates from: data/exchange/idempotency/*.json
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB

from database.models.base import Base, Mapped, mapped_column


class IdempotencyRecord(Base):
    """Idempotency record for duplicate detection.

    Stores SHA256 digests of processed events along with their cached
    results. Used to prevent duplicate processing of webhook events.

    Attributes:
        digest: SHA256 hex digest (64 characters) as primary key.
        result: Cached processing result as JSONB (nullable).
        created_at: Timestamp when the record was created.
        updated_at: Timestamp when the record was last updated.

    Indexes:
        - Primary key on digest provides fast lookup.
        - idx_idempotency_created_at: For cleanup of old records.
    """

    __tablename__ = "idempotency_records"

    digest: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        doc="SHA256 hex digest (64 characters)",
    )

    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
        doc="Cached processing result as JSONB",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        doc="Timestamp when the record was created",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        onupdate=func.now(),
        doc="Timestamp when the record was last updated",
    )

    __table_args__ = (Index("idx_idempotency_created_at", "created_at"),)

    def __repr__(self) -> str:
        return f"<IdempotencyRecord(digest={self.digest[:16]}...)>"
