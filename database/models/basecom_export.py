"""SQLAlchemy model for the Base.com Export Records table.

This table stores Base.com export file metadata and data. Records
track export files generated for Base.com delivery via S3, SFTP,
or local filesystem.

Migrates from: data/exchange/basecom/*.csv|json and *.meta.json
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from database.models.base import Base, Mapped, mapped_column


class BasecomExportRecord(Base):
    """Base.com export record.

    Stores metadata and row data for export files generated for
    Base.com delivery. Tracks delivery status and responses.

    Attributes:
        export_id: Unique identifier for the export (UUID).
        action: Export action type ('add', 'change', 'delete').
        rows: Export row data as JSONB array.
        format: Export file format ('csv' or 'json').
        created_at: Timestamp when the export was generated.
        delivered_at: Timestamp when the export was delivered (nullable).
        delivery_status: Delivery status ('pending', 'delivered', 'failed').
        delivery_response: Delivery response/details as JSONB (nullable).

    Indexes:
        - idx_basecom_export_action: For filtering by action type.
        - idx_basecom_export_created_at: For time-based queries.
        - idx_basecom_export_delivery_status: For filtering by delivery status.
    """

    __tablename__ = "basecom_export_records"

    export_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        doc="Unique identifier for the export",
    )

    action: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Export action type ('add', 'change', 'delete')",
    )

    rows: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        doc="Export row data as JSONB array",
    )

    format: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="csv",
        doc="Export file format ('csv' or 'json')",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        doc="Timestamp when the export was generated",
    )

    delivered_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
        doc="Timestamp when the export was delivered",
    )

    delivery_status: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        default="pending",
        doc="Delivery status ('pending', 'delivered', 'failed')",
    )

    delivery_response: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
        doc="Delivery response/details as JSONB",
    )

    __table_args__ = (
        Index("idx_basecom_export_action", "action"),
        Index("idx_basecom_export_created_at", "created_at"),
        Index("idx_basecom_export_delivery_status", "delivery_status"),
    )

    def __repr__(self) -> str:
        return (
            f"<BasecomExportRecord(export_id={self.export_id}, "
            f"action={self.action}, format={self.format})>"
        )
