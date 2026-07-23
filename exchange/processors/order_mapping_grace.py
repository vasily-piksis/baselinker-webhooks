from __future__ import annotations

import os
from typing import Any, Dict, Optional

from exchange.utils.recent_result_cache import load_recent_result, store_recent_result

ORDER_MAPPING_GRACE_NAMESPACE = "order-mapping-grace"
ORDER_MAPPING_GRACE_TTL = max(0, int(os.getenv("ORDER_MAPPING_GRACE_TTL", "1800")))


def _listing_key(listing_id: Any) -> str:
    return str(listing_id or "").strip()


def mark_order_mapping_grace(
    listing_id: Any,
    *,
    sku: str = "",
    source: str = "",
) -> None:
    cache_key = _listing_key(listing_id)
    if not cache_key or ORDER_MAPPING_GRACE_TTL <= 0:
        return
    store_recent_result(
        ORDER_MAPPING_GRACE_NAMESPACE,
        cache_key,
        {
            "listing_id": cache_key,
            "sku": str(sku or "").strip(),
            "source": str(source or "").strip(),
            "protected": True,
        },
        ttl_seconds=ORDER_MAPPING_GRACE_TTL,
    )


def load_order_mapping_grace(listing_id: Any) -> Optional[Dict[str, Any]]:
    cache_key = _listing_key(listing_id)
    if not cache_key:
        return None
    payload = load_recent_result(ORDER_MAPPING_GRACE_NAMESPACE, cache_key)
    return payload if isinstance(payload, dict) else None


__all__ = [
    "load_order_mapping_grace",
    "mark_order_mapping_grace",
]
