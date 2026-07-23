from __future__ import annotations

import os
from typing import Any, Dict, Optional

from exchange.utils.recent_result_cache import load_recent_result, store_recent_result

RECENT_DISCOGS_LISTING_NAMESPACE = "discogs-recent-listing-by-sku"
RECENT_DISCOGS_LISTING_TTL = max(0, int(os.getenv("DISCOGS_RECENT_LISTING_TTL", "604800")))


def _sku_key(sku: Any) -> str:
    return str(sku or "").strip()


def load_recent_discogs_listing(sku: Any) -> Optional[Dict[str, Any]]:
    cache_key = _sku_key(sku)
    if not cache_key:
        return None
    payload = load_recent_result(RECENT_DISCOGS_LISTING_NAMESPACE, cache_key)
    return payload if isinstance(payload, dict) else None


def store_recent_discogs_listing(
    sku: Any,
    *,
    listing_id: Any,
    source: str = "",
) -> None:
    cache_key = _sku_key(sku)
    listing_key = str(listing_id or "").strip()
    if not cache_key or not listing_key or RECENT_DISCOGS_LISTING_TTL <= 0:
        return
    store_recent_result(
        RECENT_DISCOGS_LISTING_NAMESPACE,
        cache_key,
        {
            "sku": cache_key,
            "listing_id": listing_key,
            "source": str(source or "").strip(),
        },
        ttl_seconds=RECENT_DISCOGS_LISTING_TTL,
    )


__all__ = [
    "load_recent_discogs_listing",
    "store_recent_discogs_listing",
]
