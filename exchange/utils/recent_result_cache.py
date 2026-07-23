"""In-memory TTL cache for duplicate webhook responses."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, Optional

from exchange.utils.ttl_cache import TTLCache


_CACHE: TTLCache[Dict[str, Any]] = TTLCache()


def _cache_key(namespace: str, digest: str) -> str:
    return f"recent-result:{namespace}:{digest}"


def _normalize_payload(value: Any, ignored_keys: set[str]) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_payload(val, ignored_keys) for key, val in value.items() if key not in ignored_keys}
    if isinstance(value, list):
        return [_normalize_payload(item, ignored_keys) for item in value]
    return value


def stable_digest(action: str, payload: Dict[str, Any], *, ignored_keys: Iterable[str] = ()) -> str:
    normalized = {"action": action, "payload": _normalize_payload(payload, set(ignored_keys))}
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_recent_result(namespace: str, digest: str) -> Optional[Dict[str, Any]]:
    value = _CACHE.get(_cache_key(namespace, digest))
    return dict(value) if value is not None else None


def store_recent_result(namespace: str, digest: str, result: Dict[str, Any], *, ttl_seconds: int) -> None:
    _CACHE.set(_cache_key(namespace, digest), dict(result), ttl_seconds=ttl_seconds)


def delete_recent_result(namespace: str, digest: str) -> None:
    _CACHE.delete(_cache_key(namespace, digest))


def clear_local_recent_result_cache() -> None:
    _CACHE.clear()
