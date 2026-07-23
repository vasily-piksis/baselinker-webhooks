"""Stateless catalog hooks retained for webhook compatibility."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def upsert_from_payload(action: str, payload: Dict[str, Any], rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return accepted rows without retaining catalog state."""
    del action, payload
    return [dict(row) for row in rows if isinstance(row, dict)]


def delete_from_payload(payload: Dict[str, Any]) -> List[str]:
    """Acknowledge deletion without retaining catalog state."""
    value = payload.get("product_id") or payload.get("sku")
    return [str(value)] if value not in (None, "") else []
