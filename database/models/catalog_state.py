"""SQLAlchemy model for the Catalog State table.

This table stores product catalog state for the BaseLinker Shop
Integration Protocol. Each row represents a product's current state.

Migrates from: data/exchange/catalog.json (single JSON file)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB

from database.models.base import Base, Mapped, mapped_column


class CatalogState(Base):
    """Catalog state record for product data.

    Stores the current state of products in the catalog. Used by
    the BaseLinker Shop Integration Protocol for product lookups
    and state management.

    Attributes:
        product_id: Unique product identifier (primary key).
        data: Full product data as JSONB.
        updated_at: Timestamp when the product was last updated.

    Indexes:
        - Primary key on product_id provides fast lookup.
    """

    __tablename__ = "catalog_state"

    product_id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        doc="Unique product identifier",
    )

    data: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        doc="Full product data as JSONB",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        server_default=func.now(),
        onupdate=func.now(),
        doc="Timestamp when the product was last updated",
    )

    def __repr__(self) -> str:
        return f"<CatalogState(product_id={self.product_id})>"
