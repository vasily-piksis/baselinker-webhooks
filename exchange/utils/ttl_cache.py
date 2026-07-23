"""Small bounded, thread-safe TTL cache for one webhook process."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Callable, Generic, TypeVar


T = TypeVar("T")


class TTLCache(Generic[T]):
    def __init__(self, *, max_entries: int = 10_000, clock: Callable[[], float] = time.monotonic):
        self._max_entries = max(1, max_entries)
        self._clock = clock
        self._entries: OrderedDict[str, tuple[float, T]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> T | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at <= self._clock():
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return value

    def set(self, key: str, value: T, *, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            return
        with self._lock:
            self._entries[key] = (self._clock() + ttl_seconds, value)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def delete(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
