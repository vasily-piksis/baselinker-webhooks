"""No-op master catalog compatibility helpers for the stateless service."""

from __future__ import annotations

from typing import Any, Dict, Optional


def get_master_row(identifier: str) -> Optional[Dict[str, Any]]:
    del identifier
    return None


def hydrate_exchange_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return row


__all__ = ["get_master_row", "hydrate_exchange_row"]
