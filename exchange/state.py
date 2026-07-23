"""Small process-local state used only for protocol compatibility."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

_categories: Dict[str, str] = {"discogs": "Discogs Marketplace"}


def upsert_from_payload(action: str, payload: Dict[str, Any], rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return accepted rows without retaining catalog state."""
    del action, payload
    return [dict(row) for row in rows if isinstance(row, dict)]


def delete_from_payload(payload: Dict[str, Any]) -> List[str]:
    """Acknowledge deletion without retaining catalog state."""
    value = payload.get("product_id") or payload.get("sku")
    return [str(value)] if value not in (None, "") else []


def list_categories() -> Dict[str, str]:
    return dict(_categories)


def add_category(name: str, parent_id: str | None = None) -> str:
    del parent_id
    normalized = "-".join(name.lower().split())
    category_id = normalized or "discogs"
    _categories[category_id] = name
    return category_id
