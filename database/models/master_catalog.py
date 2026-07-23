"""SQLAlchemy model for the Master Catalog table.

This table stores master catalog entries with Discogs release mappings.
Used for hydrating exchange rows with catalog data.

Migrates from: data/exchange/master.csv (single CSV file)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import DateTime, Index, Integer, Numeric, String, Text, func

from database.models.base import Base, Mapped, mapped_column


class MasterCatalog(Base):
    """Master catalog entry with Discogs release mapping.

    Stores master catalog entries used for hydrating exchange rows
    with catalog data. Maps external SKUs to Discogs release IDs
    and stores product metadata.

    Attributes:
        external_sku: External SKU identifier (primary key).
        release_id: Discogs release ID (nullable, indexed).
        listing_id: Discogs listing ID (nullable, indexed).
        title: Album/product title (nullable).
        artist: Artist name (nullable).
        format: Format (LP, CD, etc.) (nullable).
        condition: Condition (NM, M, VG+, etc.) (nullable).
        price: Price as decimal (nullable).
        currency: Currency code (USD, EUR, etc.) (nullable).
        quantity: Available quantity (nullable).
        location: Storage location (nullable).
        notes: Additional notes (nullable, TEXT).
        created_at: Timestamp when the entry was created.
        updated_at: Timestamp when the entry was last updated.

    Indexes:
        - Primary key on external_sku provides fast lookup.
        - idx_master_catalog_release_id: For lookup by Discogs release ID.
        - idx_master_catalog_listing_id: For lookup by Discogs listing ID.
    """

    __tablename__ = "master_catalog"

    external_sku: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        doc="External SKU identifier",
    )

    release_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        doc="Discogs release ID",
    )

    listing_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        doc="Discogs listing ID",
    )

    title: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        doc="Album/product title",
    )

    artist: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        doc="Artist name",
    )

    format: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        doc="Format (LP, CD, etc.)",
    )

    condition: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        doc="Condition (NM, M, VG+, etc.)",
    )

    price: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
        doc="Price as decimal",
    )

    currency: Mapped[Optional[str]] = mapped_column(
        String(10),
        nullable=True,
        doc="Currency code (USD, EUR, etc.)",
    )

    quantity: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="Available quantity",
    )

    location: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Storage location",
    )

    notes: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Additional notes",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        doc="Timestamp when the entry was created",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        onupdate=func.now(),
        doc="Timestamp when the entry was last updated",
    )

    __table_args__ = (
        Index("idx_master_catalog_release_id", "release_id"),
        Index("idx_master_catalog_listing_id", "listing_id"),
    )

    def __repr__(self) -> str:
        return f"<MasterCatalog(external_sku={self.external_sku}, release_id={self.release_id})>"
