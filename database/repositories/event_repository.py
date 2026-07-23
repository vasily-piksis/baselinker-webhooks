"""Repository for managing Event records."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database.models.event import Event

log = logging.getLogger(__name__)


class EventRepository:
    """Repository for Event model operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def create_event(
        self,
        action: str,
        payload: Dict[str, Any],
        status: str = "queued",
        *,
        idempotency_token: Optional[str] = None,
        correlation_id: Optional[str] = None,
        created_at: Optional[datetime] = None,
        processed_at: Optional[datetime] = None,
    ) -> Event:
        """Create a new event record.

        Args:
            action: Event action type (e.g., 'ProductAdd').
            payload: Webhook payload as dictionary.
            status: Initial status (default: 'queued').
            idempotency_token: Optional SHA256 digest for duplicate detection.
            correlation_id: Optional ID to track related events.
            created_at: Optional creation timestamp (default: now).
            processed_at: Optional processed timestamp.

        Returns:
            Created Event instance.
        """
        event = Event(
            action=action,
            payload=payload,
            status=status,
            idempotency_token=idempotency_token,
            correlation_id=correlation_id,
        )

        if created_at:
            event.created_at = created_at

        if processed_at:
            event.processed_at = processed_at

        self.session.add(event)
        self.session.flush()  # Flush to generate event_id

        log.debug(
            "Created event %s (action=%s, status=%s)",
            event.event_id,
            action,
            status,
        )
        return event

    def get_event_by_id(self, event_id: UUID) -> Optional[Event]:
        """Get event by UUID.

        Args:
            event_id: Event UUID.

        Returns:
            Event instance if found, None otherwise.
        """
        return cast(Optional[Event], self.session.get(Event, event_id))

    def get_events_by_status(
        self,
        status: str,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Event]:
        """Get events filtered by status.

        Args:
            status: Status to filter by.
            limit: Maximum number of events to return.
            offset: Number of events to skip.

        Returns:
            List of Event instances.
        """
        query = (
            select(Event).where(Event.status == status).order_by(Event.created_at).offset(offset)
        )

        if limit is not None:
            query = query.limit(limit)

        return list(self.session.scalars(query).all())

    def get_events_by_statuses(
        self,
        statuses: Iterable[str],
        *,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Event]:
        """Get events filtered by a set of statuses.

        Args:
            statuses: Iterable of statuses to include.
            limit: Maximum number of events to return.
            offset: Number of events to skip.

        Returns:
            List of Event instances.
        """
        status_list = [str(status) for status in statuses]
        if not status_list:
            return []

        query = (
            select(Event)
            .where(Event.status.in_(status_list))
            .order_by(Event.created_at)
            .offset(offset)
        )

        if limit is not None:
            query = query.limit(limit)

        return list(self.session.scalars(query).all())

    def count_events_by_status(self, status: str) -> int:
        """Count events with a given status.

        Args:
            status: Status to filter by.

        Returns:
            Count of matching events.
        """
        count = self.session.scalar(
            select(func.count()).select_from(Event).where(Event.status == status)
        )
        return int(count or 0)

    def get_event_by_idempotency_token(self, token: str) -> Optional[Event]:
        """Get event by idempotency token.

        Args:
            token: SHA256 digest token.

        Returns:
            Event instance if found, None otherwise.
        """
        query = select(Event).where(Event.idempotency_token == token)
        return cast(Optional[Event], self.session.scalars(query).first())

    def update_event_status(
        self,
        event_id: UUID,
        status: str,
        *,
        processed_at: Optional[datetime] = None,
    ) -> Optional[Event]:
        """Update event status and processed_at timestamp.

        Args:
            event_id: Event UUID.
            status: New status value.
            processed_at: Optional timestamp when processing completed.

        Returns:
            Updated Event instance if found, None otherwise.
        """
        event = self.get_event_by_id(event_id)
        if not event:
            return None

        event.status = status
        if processed_at:
            event.processed_at = processed_at

        self.session.add(event)
        self.session.flush()

        log.debug(
            "Updated event %s status to %s",
            event_id,
            status,
        )
        return event
