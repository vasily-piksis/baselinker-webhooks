"""Repository for managing master catalog records."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Dict, List, Optional, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models.master_catalog import MasterCatalog

log = logging.getLogger(__name__)


class MasterCatalogRepository:
    """Repository for MasterCatalog model operations."""

    def __init__(self, session: Session):
        """Initialize repository with database session.

        Args:
            session: SQLAlchemy database session.
        """
        self.session = session

    def get_by_sku(self, external_sku: str) -> Optional[MasterCatalog]:
        """Get catalog entry by external_sku.

        Args:
            external_sku: External SKU identifier.

        Returns:
            Master catalog record if found, otherwise None.
        """
        return cast(Optional[MasterCatalog], self.session.get(MasterCatalog, external_sku))

    def get_by_listing_id(self, listing_id: str) -> Optional[MasterCatalog]:
        """Get catalog entry by Discogs listing_id.

        Args:
            listing_id: Discogs listing identifier.

        Returns:
            Matching catalog record if found, otherwise None.
        """
        query = (
            select(MasterCatalog)
            .where(MasterCatalog.listing_id == listing_id)
            .limit(1)
        )
        return cast(Optional[MasterCatalog], self.session.scalars(query).first())

    def upsert_entry(
        self,
        external_sku: str,
        *,
        release_id: Optional[str] = None,
        listing_id: Optional[str] = None,
        title: Optional[str] = None,
        artist: Optional[str] = None,
        format: Optional[str] = None,
        condition: Optional[str] = None,
        price: Optional[Decimal] = None,
        currency: Optional[str] = None,
        quantity: Optional[int] = None,
        location: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> MasterCatalog:
        """Create or update a catalog entry.

        Args:
            external_sku: External SKU identifier.
            release_id: Optional Discogs release id.
            listing_id: Optional Discogs listing id.
            title: Release title.
            artist: Artist name.
            format: Format description.
            condition: Condition string.
            price: Listing price.
            currency: Currency code.
            quantity: Available quantity.
            location: Location string.
            notes: Additional notes.

        Returns:
            Master catalog record.
        """
        record = self.get_by_sku(external_sku)
        if not record:
            record = MasterCatalog(external_sku=external_sku)
        record.release_id = release_id
        record.listing_id = listing_id
        record.title = title
        record.artist = artist
        record.format = format
        record.condition = condition
        record.price = price
        record.currency = currency
        record.quantity = quantity
        record.location = location
        record.notes = notes
        self.session.add(record)
        self.session.flush()
        log.debug("Upserted master catalog entry %s", external_sku)
        return record

    def delete_entry(self, external_sku: str) -> bool:
        """Delete catalog entry by external_sku.

        Args:
            external_sku: External SKU identifier.

        Returns:
            True if deleted, False if the record did not exist.
        """
        record = self.get_by_sku(external_sku)
        if not record:
            return False
        self.session.delete(record)
        self.session.flush()
        log.debug("Deleted master catalog entry %s", external_sku)
        return True

    def list_entries(
        self,
        *,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[MasterCatalog]:
        """List catalog entries with pagination.

        Args:
            limit: Optional max number of records to return.
            offset: Offset for pagination.

        Returns:
            List of catalog records.
        """
        query = select(MasterCatalog).order_by(MasterCatalog.updated_at.desc()).offset(offset)
        if limit is not None:
            query = query.limit(limit)
        return list(self.session.scalars(query).all())

    def search_by_release_id(self, release_id: str) -> List[MasterCatalog]:
        """Search entries by release_id.

        Args:
            release_id: Discogs release id.

        Returns:
            List of matching catalog records.
        """
        query = select(MasterCatalog).where(MasterCatalog.release_id == release_id)
        return list(self.session.scalars(query).all())

    def get_listing_id_map(self, skus: List[str]) -> Dict[str, str]:
        """Batch-query listing_ids for the given SKUs.

        Args:
            skus: List of external SKU identifiers.

        Returns:
            Mapping of {sku: listing_id} for SKUs that have a listing_id.
        """
        if not skus:
            return {}
        result: Dict[str, str] = {}
        batch_size = 500
        for i in range(0, len(skus), batch_size):
            batch = skus[i : i + batch_size]
            query = select(
                MasterCatalog.external_sku, MasterCatalog.listing_id
            ).where(
                MasterCatalog.external_sku.in_(batch),
                MasterCatalog.listing_id.isnot(None),
            )
            for row in self.session.execute(query):
                result[row[0]] = row[1]
        return result

    def bulk_update_listing_ids(self, mapping: Dict[str, str]) -> int:
        """Batch-update listing_ids for existing catalog entries.

        Args:
            mapping: {sku: listing_id} pairs to persist.

        Returns:
            Number of records updated.
        """
        if not mapping:
            return 0
        updated = 0
        for sku, listing_id in mapping.items():
            record = self.get_by_sku(sku)
            if record:
                record.listing_id = listing_id
                self.session.add(record)
                updated += 1
            else:
                record = MasterCatalog(external_sku=sku, listing_id=listing_id)
                self.session.add(record)
                updated += 1
        self.session.flush()
        log.debug("Bulk-updated listing_ids for %s entries", updated)
        return updated

    def set_listing_id(self, external_sku: str, listing_id: Optional[str]) -> MasterCatalog:
        """Update only the listing_id for a catalog entry.

        Args:
            external_sku: External SKU identifier.
            listing_id: New Discogs listing id, or None to clear it.

        Returns:
            The updated catalog record.
        """
        record = self.get_by_sku(external_sku)
        if not record:
            record = MasterCatalog(external_sku=external_sku)
        record.listing_id = listing_id
        self.session.add(record)
        self.session.flush()
        log.debug("Set listing_id for %s to %s", external_sku, listing_id)
        return record
