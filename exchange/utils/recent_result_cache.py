"""Short-lived recent-result cache backed by Redis when available."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Iterable, Optional

try:
    import redis
except Exception:  # pragma: no cover - optional dependency
    redis = None

log = logging.getLogger("exchange.recent_result_cache")
_REDIS_CLIENTS: dict[str, object] = {}
_LOCK = threading.Lock()
_MEMORY_CACHE: dict[str, tuple[float, Dict[str, Any]]] = {}
_REDIS_IMPORT_WARNED = False
_REDIS_FAILURE_WARNED = False


def _redis_client() -> object | None:
    global _REDIS_IMPORT_WARNED
    redis_url = os.getenv("RATE_LIMITER_REDIS_URL") or ""
    if not redis_url:
        return None
    if redis is None:
        if not _REDIS_IMPORT_WARNED:
            log.warning(
                "RATE_LIMITER_REDIS_URL is set but the redis package is not installed; "
                "falling back to local recent-result cache"
            )
            _REDIS_IMPORT_WARNED = True
        return None
    with _LOCK:
        client = _REDIS_CLIENTS.get(redis_url)
        if client is None:
            client = redis.from_url(redis_url, decode_responses=True)
            _REDIS_CLIENTS[redis_url] = client
        return client


def _warn_redis_failure(exc: Exception) -> None:
    global _REDIS_FAILURE_WARNED
    if not _REDIS_FAILURE_WARNED:
        log.warning("Recent-result Redis cache unavailable, falling back to local cache: %s", exc)
        _REDIS_FAILURE_WARNED = True


def _cache_key(namespace: str, digest: str) -> str:
    return f"recent-result:{namespace}:{digest}"


def _normalize_payload(value: Any, ignored_keys: set[str]) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_payload(val, ignored_keys)
            for key, val in value.items()
            if key not in ignored_keys
        }
    if isinstance(value, list):
        return [_normalize_payload(item, ignored_keys) for item in value]
    return value


def stable_digest(action: str, payload: Dict[str, Any], *, ignored_keys: Iterable[str] = ()) -> str:
    normalized = {
        "action": action,
        "payload": _normalize_payload(payload, set(ignored_keys)),
    }
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_recent_result(namespace: str, digest: str) -> Optional[Dict[str, Any]]:
    cache_key = _cache_key(namespace, digest)
    client = _redis_client()
    if client is not None:
        try:
            raw = client.get(cache_key)
            if raw:
                payload = json.loads(raw)
                return payload if isinstance(payload, dict) else None
        except Exception as exc:  # pragma: no cover - depends on external Redis
            _warn_redis_failure(exc)

    with _LOCK:
        entry = _MEMORY_CACHE.get(cache_key)
        if not entry:
            return None
        expires_at, payload = entry
        if expires_at <= time.time():
            _MEMORY_CACHE.pop(cache_key, None)
            return None
        return dict(payload)


def store_recent_result(
    namespace: str,
    digest: str,
    result: Dict[str, Any],
    *,
    ttl_seconds: int,
) -> None:
    if ttl_seconds <= 0:
        return
    cache_key = _cache_key(namespace, digest)
    payload = json.dumps(result, sort_keys=True, default=str)
    client = _redis_client()
    if client is not None:
        try:
            client.setex(cache_key, ttl_seconds, payload)
            return
        except Exception as exc:  # pragma: no cover - depends on external Redis
            _warn_redis_failure(exc)

    with _LOCK:
        _MEMORY_CACHE[cache_key] = (
            time.time() + float(ttl_seconds),
            json.loads(payload),
        )


def delete_recent_result(namespace: str, digest: str) -> None:
    cache_key = _cache_key(namespace, digest)
    client = _redis_client()
    if client is not None:
        try:
            client.delete(cache_key)
        except Exception as exc:  # pragma: no cover - depends on external Redis
            _warn_redis_failure(exc)

    with _LOCK:
        _MEMORY_CACHE.pop(cache_key, None)


def clear_local_recent_result_cache() -> None:
    with _LOCK:
        _MEMORY_CACHE.clear()
