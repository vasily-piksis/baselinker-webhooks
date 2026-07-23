"""Repository for managing Base.com export records."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models.basecom_export import BasecomExportRecord

log = logging.getLogger(__name__)


class BasecomExportRepository:
    """Repository for BasecomExportRecord model operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def create_export_record(
        self,
        action: str,
        rows: List[Dict[str, Any]],
        file_format: str,
        *,
        created_at: Optional[datetime] = None,
    ) -> BasecomExportRecord:
        """Create a new Base.com export record.

        Args:
            action: Base.com export action name.
            rows: Export row payloads.
            file_format: Export file format (csv/json).
            created_at: Optional creation timestamp override.

        Returns:
            Newly created export record.
        """
        record = BasecomExportRecord(
            action=action,
            rows=rows,
            format=file_format,
        )
        if created_at:
            record.created_at = created_at
        self.session.add(record)
        self.session.flush()
        log.debug(
            "Created Base.com export %s (action=%s, rows=%s)",
            record.export_id,
            action,
            len(rows),
        )
        return record

    def get_export_by_id(self, export_id: UUID) -> Optional[BasecomExportRecord]:
        """Get export record by export_id.

        Args:
            export_id: Export record identifier.

        Returns:
            Export record if found, otherwise None.
        """
        return cast(
            Optional[BasecomExportRecord],
            self.session.get(BasecomExportRecord, export_id),
        )

    def get_exports_by_action(
        self,
        action: str,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[BasecomExportRecord]:
        """Get export records filtered by action.

        Args:
            action: Base.com export action name.
            limit: Optional max number of records to return.
            offset: Offset for pagination.

        Returns:
            List of export records.
        """
        query = (
            select(BasecomExportRecord)
            .where(BasecomExportRecord.action == action)
            .order_by(BasecomExportRecord.created_at)
            .offset(offset)
        )
        if limit is not None:
            query = query.limit(limit)
        return list(self.session.scalars(query).all())

    def update_delivery_status(
        self,
        export_id: UUID,
        *,
        delivered_at: Optional[datetime],
        delivery_status: str,
        delivery_response: Dict[str, Any],
    ) -> Optional[BasecomExportRecord]:
        """Update delivery status and response.

        Args:
            export_id: Export record identifier.
            delivered_at: Delivery timestamp.
            delivery_status: Status string (pending/delivered/failed).
            delivery_response: Delivery metadata payload.

        Returns:
            Updated export record, or None if not found.
        """
        record = self.get_export_by_id(export_id)
        if not record:
            return None
        record.delivered_at = delivered_at
        record.delivery_status = delivery_status
        record.delivery_response = delivery_response
        self.session.add(record)
        self.session.flush()
        log.debug("Updated Base.com export delivery status for %s", export_id)
        return record
