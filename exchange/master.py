"""Master catalog sync helpers."""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from database.models.master_catalog import MasterCatalog
from database.repositories.master_catalog_repository import MasterCatalogRepository
from database.session import get_session

_MASTER_LOCK = threading.Lock()


def _canonical_key(row: Dict[str, Any]) -> Optional[str]:
    for key in ("external_sku", "sku", "product_id", "external_id", "id"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _record_to_row(record: MasterCatalog) -> Dict[str, Any]:
    price = record.price
    if price is not None:
        price = str(price)
    return {
        "external_sku": record.external_sku,
        "release_id": record.release_id,
        "title": record.title,
        "artist": record.artist,
        "format": record.format,
        "condition": record.condition,
        "price": price,
        "currency": record.currency,
        "quantity": record.quantity,
        "location": record.location,
        "notes": record.notes,
    }


def get_master_row(identifier: str) -> Optional[Dict[str, Any]]:
    """Fetch a master catalog row by identifier.

    Args:
        identifier: External SKU or product identifier.

    Returns:
        Master catalog row dictionary, or None if not found.
    """
    ident = str(identifier or "").strip()
    if not ident:
        return None
    with _MASTER_LOCK:
        try:
            with get_session() as session:
                repo = MasterCatalogRepository(session)
                record = repo.get_by_sku(ident)
                if not record:
                    return None
                return _record_to_row(record)
        except Exception:
            # Do not crash the request path when the DB has issues.
            return None


def hydrate_exchange_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Merge a master catalog row into an exchange row.

    Args:
        row: Exchange row dictionary.

    Returns:
        Exchange row enriched with master catalog data when available.
    """
    if not isinstance(row, dict):
        return row
    key = _canonical_key(row)
    if not key:
        return row
    master = get_master_row(key)
    if not master:
        return row
    merged = dict(master)
    merged.update({k: v for k, v in row.items() if v not in (None, "", [])})
    return merged


__all__ = ["get_master_row", "hydrate_exchange_row"]
