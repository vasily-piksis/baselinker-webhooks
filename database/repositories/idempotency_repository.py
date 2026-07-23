"""Repository for managing Idempotency records."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional, cast

from sqlalchemy.orm import Session

from database.models.idempotency import IdempotencyRecord

log = logging.getLogger(__name__)


class IdempotencyRepository:
    """Repository for IdempotencyRecord model operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def get_by_digest(self, digest: str) -> Optional[IdempotencyRecord]:
        """Get idempotency record by digest (SHA256).

        Args:
            digest: SHA256 hex digest (64 characters).

        Returns:
            IdempotencyRecord instance if found, None otherwise.
        """
        return cast(Optional[IdempotencyRecord], self.session.get(IdempotencyRecord, digest))

    def create_or_update(
        self,
        digest: str,
        result: Dict[str, Any],
        *,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ) -> IdempotencyRecord:
        """Create or update idempotency record.

        Args:
            digest: SHA256 hex digest (64 characters).
            result: Cached processing result as dictionary.
            created_at: Optional creation timestamp (default: now).
            updated_at: Optional update timestamp (default: now).

        Returns:
            Created or updated IdempotencyRecord instance.
        """
        record = self.get_by_digest(digest)

        if record:
            record.result = result
            if updated_at:
                record.updated_at = updated_at
            log.debug("Updated idempotency record %s", digest)
        else:
            record = IdempotencyRecord(
                digest=digest,
                result=result,
            )
            if created_at:
                record.created_at = created_at
            if updated_at:
                record.updated_at = updated_at

            self.session.add(record)
            log.debug("Created idempotency record %s", digest)

        self.session.flush()
        return record

    def delete_by_digest(self, digest: str) -> bool:
        """Delete idempotency record (optional, for cleanup).

        Args:
            digest: SHA256 hex digest.

        Returns:
            True if deleted, False if not found.
        """
        record = self.get_by_digest(digest)
        if record:
            self.session.delete(record)
            self.session.flush()
            log.debug("Deleted idempotency record %s", digest)
            return True
        return False
