"""Rate limiter for API clients.

Provides a local token bucket limiter and an optional Redis-backed
cross-process limiter when ``RATE_LIMITER_REDIS_URL`` is configured.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time

try:
    import redis
except Exception:  # pragma: no cover - optional dependency
    redis = None

log = logging.getLogger("exchange.rate_limiter")
_REDIS_CLIENTS: dict[str, object] = {}
_REDIS_LOCK = threading.Lock()
_REDIS_IMPORT_WARNED = False


def _redis_client(redis_url: str) -> object | None:
    global _REDIS_IMPORT_WARNED
    if not redis_url:
        return None
    if redis is None:
        if not _REDIS_IMPORT_WARNED:
            log.warning(
                "RATE_LIMITER_REDIS_URL is set but the redis package is not installed; "
                "falling back to local rate limiter"
            )
            _REDIS_IMPORT_WARNED = True
        return None
    with _REDIS_LOCK:
        client = _REDIS_CLIENTS.get(redis_url)
        if client is None:
            client = redis.from_url(redis_url, decode_responses=True)
            _REDIS_CLIENTS[redis_url] = client
        return client


class RateLimiter:
    """Token bucket rate limiter for API request throttling.

    Thread-safe implementation that limits requests per minute using
    a token bucket algorithm with automatic refill.

    Args:
        per_minute: Maximum allowed requests per minute.

    Examples:
        >>> limiter = RateLimiter(per_minute=60)
        >>> limiter.wait()  # blocks if rate limit exceeded
    """

    def __init__(self, per_minute: int, *, key: str = "default", redis_url: str | None = None):
        """Initialize the rate limiter.

        Args:
            per_minute: Allowed requests per minute.
            key: Logical limiter key used to isolate budgets.
            redis_url: Optional Redis URL for distributed throttling.
        """
        self.per_minute = max(1, per_minute)
        self.key = key
        self.tokens = float(self.per_minute)
        self.lock = threading.Lock()
        self.last = time.time()
        self._redis_url = redis_url or os.getenv("RATE_LIMITER_REDIS_URL") or ""
        self._redis = _redis_client(self._redis_url)
        self._redis_disabled = False
        self._redis_warned = False
        self._cooldown_key = f"rate-limit-cooldown:{self.key}"
        self._local_cooldown_until = 0.0

    def _warn_redis_fallback(self, exc: Exception) -> None:
        self._redis_disabled = True
        if not self._redis_warned:
            log.warning("Redis rate limiter unavailable, falling back to local limiter: %s", exc)
            self._redis_warned = True

    def impose_cooldown(self, seconds: float) -> None:
        """Broadcast a cooldown period so all callers back off together."""
        cooldown = max(0.0, float(seconds))
        if cooldown <= 0:
            return
        unlock_at = time.time() + cooldown
        if self._redis is not None and not self._redis_disabled:
            try:
                self._redis.setex(
                    self._cooldown_key,
                    max(1, int(math.ceil(cooldown))),
                    f"{unlock_at:.6f}",
                )
                return
            except Exception as exc:  # pragma: no cover - depends on external Redis
                self._warn_redis_fallback(exc)
        with self.lock:
            self._local_cooldown_until = max(self._local_cooldown_until, unlock_at)

    def _wait_local_cooldown(self) -> None:
        sleep_s = 0.0
        with self.lock:
            now = time.time()
            if self._local_cooldown_until > now:
                sleep_s = self._local_cooldown_until - now
            else:
                self._local_cooldown_until = 0.0
        if sleep_s > 0:
            time.sleep(sleep_s)

    def _wait_local(self) -> None:
        """Local in-process token bucket."""
        self._wait_local_cooldown()
        sleep_s = 0.0
        with self.lock:
            now = time.time()
            elapsed = now - self.last
            self.last = now
            # Refill bucket based on elapsed time
            self.tokens = min(
                self.per_minute,
                self.tokens + elapsed * (self.per_minute / 60.0),
            )
            if self.tokens < 1.0:
                sleep_s = (1.0 - self.tokens) * (60.0 / self.per_minute)

        if sleep_s > 0:
            time.sleep(sleep_s)

        with self.lock:
            self.tokens = max(0.0, self.tokens - 1.0)

    def _wait_redis(self) -> None:
        """Fixed-window limiter shared across processes via Redis."""
        assert self._redis is not None
        while True:
            cooldown_until_raw = self._redis.get(self._cooldown_key)
            if cooldown_until_raw is not None:
                try:
                    cooldown_until = float(cooldown_until_raw)
                except (TypeError, ValueError):
                    cooldown_until = 0.0
                if cooldown_until > time.time():
                    time.sleep(cooldown_until - time.time())
                    continue
            now = int(time.time())
            window = now // 60
            redis_key = f"rate-limit:{self.key}:{self.per_minute}:{window}"
            count = self._redis.incr(redis_key)
            if count == 1:
                self._redis.expire(redis_key, 61)
            if count <= self.per_minute:
                return
            ttl = self._redis.ttl(redis_key)
            sleep_s = max(1, int(ttl) if isinstance(ttl, int) and ttl > 0 else 1)
            time.sleep(float(sleep_s))

    def wait(self) -> None:
        """Wait until a request token is available.

        Blocks the calling thread if the rate limit has been exceeded,
        waiting until enough time has passed to refill the bucket.
        """
        if self._redis is not None and not self._redis_disabled:
            try:
                self._wait_redis()
                return
            except Exception as exc:  # pragma: no cover - depends on external Redis
                self._warn_redis_fallback(exc)
        self._wait_local()


__all__ = ["RateLimiter"]
