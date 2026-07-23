"""Repository for managing catalog state records."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models.catalog_state import CatalogState

log = logging.getLogger(__name__)


class CatalogStateRepository:
    """Repository for CatalogState model operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def get_product(self, product_id: str) -> Optional[CatalogState]:
        """Get product by product_id.

        Args:
            product_id: Product identifier.

        Returns:
            Catalog state record if found, otherwise None.
        """
        return cast(Optional[CatalogState], self.session.get(CatalogState, product_id))

    def upsert_product(
        self,
        product_id: str,
        data: Dict[str, Any],
        *,
        updated_at: Optional[datetime] = None,
    ) -> CatalogState:
        """Create or update product data.

        Args:
            product_id: Product identifier.
            data: Catalog data payload.
            updated_at: Optional timestamp override.

        Returns:
            Catalog state record.
        """
        record = self.get_product(product_id)
        if record:
            record.data = data
        else:
            record = CatalogState(product_id=product_id, data=data)
        if updated_at:
            record.updated_at = updated_at
        self.session.add(record)
        self.session.flush()
        log.debug("Upserted catalog product %s", product_id)
        return record

    def delete_product(self, product_id: str) -> bool:
        """Delete product by product_id.

        Args:
            product_id: Product identifier.

        Returns:
            True if deleted, False if the record did not exist.
        """
        record = self.get_product(product_id)
        if not record:
            return False
        self.session.delete(record)
        self.session.flush()
        log.debug("Deleted catalog product %s", product_id)
        return True

    def list_products(
        self,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[CatalogState]:
        """List products with pagination.

        Args:
            limit: Optional max number of records to return.
            offset: Offset for pagination.

        Returns:
            List of catalog state records.
        """
        query = select(CatalogState).order_by(CatalogState.updated_at.desc()).offset(offset)
        if limit is not None:
            query = query.limit(limit)
        return list(self.session.scalars(query).all())

    def get_all_products(self) -> List[CatalogState]:
        """Get all catalog products.

        Returns:
            List of catalog state records.
        """
        query = select(CatalogState).order_by(CatalogState.updated_at.desc())
        return list(self.session.scalars(query).all())
