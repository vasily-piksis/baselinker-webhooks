"""Repository for managing Discogs CSV records."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models.discogs_csv import DiscogsCsvRecord

log = logging.getLogger(__name__)


class DiscogsCsvRepository:
    """Repository for DiscogsCsvRecord model operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def create_csv_record(
        self,
        action: str,
        rows: List[Dict[str, Any]],
        *,
        idempotency_token: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> DiscogsCsvRecord:
        """Create a new Discogs CSV record.

        Args:
            action: CSV action type ('add', 'change', 'delete').
            rows: CSV row data as a list of dicts.
            idempotency_token: Optional idempotency token.
            created_at: Optional creation timestamp.

        Returns:
            Created DiscogsCsvRecord instance.
        """
        record = DiscogsCsvRecord(
            action=action,
            rows=rows,
            idempotency_token=idempotency_token,
        )
        if created_at:
            record.created_at = created_at

        self.session.add(record)
        self.session.flush()

        log.debug(
            "Created Discogs CSV record %s (action=%s, rows=%s)",
            record.record_id,
            action,
            len(rows),
        )
        return record

    def get_by_idempotency_token(self, token: str) -> Optional[DiscogsCsvRecord]:
        """Get CSV record by idempotency token.

        Args:
            token: Idempotency token value.

        Returns:
            Matching CSV record if found, otherwise None.
        """
        query = select(DiscogsCsvRecord).where(DiscogsCsvRecord.idempotency_token == token)
        return cast(Optional[DiscogsCsvRecord], self.session.scalars(query).first())

    def get_by_record_id(self, record_id: UUID) -> Optional[DiscogsCsvRecord]:
        """Get CSV record by record_id.

        Args:
            record_id: CSV record identifier.

        Returns:
            Matching CSV record if found, otherwise None.
        """
        return cast(Optional[DiscogsCsvRecord], self.session.get(DiscogsCsvRecord, record_id))

    def get_csv_records_by_action(
        self,
        action: str,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[DiscogsCsvRecord]:
        """Get CSV records filtered by action.

        Args:
            action: Discogs CSV action type.
            limit: Optional max number of records to return.
            offset: Offset for pagination.

        Returns:
            List of CSV records.
        """
        query = (
            select(DiscogsCsvRecord)
            .where(DiscogsCsvRecord.action == action)
            .order_by(DiscogsCsvRecord.created_at)
            .offset(offset)
        )
        if limit is not None:
            query = query.limit(limit)
        return list(self.session.scalars(query).all())

    def update_upload_status(
        self,
        record_id: UUID,
        uploaded_at: datetime,
        upload_response: Dict[str, Any],
    ) -> Optional[DiscogsCsvRecord]:
        """Update upload status and response.

        Args:
            record_id: CSV record identifier.
            uploaded_at: Timestamp for upload completion.
            upload_response: Response payload from Discogs upload.

        Returns:
            Updated CSV record, or None if not found.
        """
        record = self.session.get(DiscogsCsvRecord, record_id)
        if not record:
            return None
        record.uploaded_at = uploaded_at
        record.upload_response = upload_response
        self.session.add(record)
        self.session.flush()
        log.debug("Updated Discogs CSV upload status for %s", record_id)
        return cast(Optional[DiscogsCsvRecord], record)
