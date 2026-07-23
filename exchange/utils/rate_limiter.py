"""Thread-safe in-memory token-bucket rate limiter."""

from __future__ import annotations

import threading
import time


class RateLimiter:
    def __init__(self, per_minute: int, *, key: str = "default") -> None:
        self.per_minute = max(1, per_minute)
        self.key = key
        self.tokens = float(self.per_minute)
        self.last = time.monotonic()
        self._cooldown_until = 0.0
        self.lock = threading.Lock()

    def impose_cooldown(self, seconds: float) -> None:
        with self.lock:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + max(0.0, seconds))

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                wait_for = max(0.0, self._cooldown_until - now)
                if wait_for == 0:
                    elapsed = now - self.last
                    self.last = now
                    self.tokens = min(self.per_minute, self.tokens + elapsed * self.per_minute / 60.0)
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                    wait_for = (1 - self.tokens) * 60.0 / self.per_minute
            time.sleep(wait_for)


__all__ = ["RateLimiter"]
