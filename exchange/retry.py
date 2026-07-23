"""Standard retry configurations for external API calls."""

from __future__ import annotations

import os
from typing import Any, Callable, TypeVar

import httpx

from exchange.errors import BaseLinkerAPIError, DiscogsAPIError, RateLimitError

F = TypeVar("F", bound=Callable[..., Any])

try:
    from tenacity import (  # type: ignore[import-untyped]
        retry,
        retry_if_exception,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
    )
except Exception:  # pragma: no cover - optional dependency fallback

    def _identity(func: F) -> F:
        return func

    standard_retry: Callable[[F], F] = _identity
    rate_limit_retry: Callable[[F], F] = _identity

    def is_retryable_exception(_exc: BaseException) -> bool:
        return False

else:
    _rate_limit_backoff = wait_exponential(multiplier=2, min=5, max=60)
    _max_rate_limit_wait = float(os.getenv("RATE_LIMIT_RETRY_MAX_WAIT", "15"))

    def _http_status_from_exc(exc: BaseException) -> int | None:
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
            return exc.response.status_code
        if isinstance(exc, httpx.HTTPError):
            response = getattr(exc, "response", None)
            if response is not None:
                return getattr(response, "status_code", None)
        return None

    def is_retryable_exception(exc: BaseException) -> bool:
        if isinstance(exc, RateLimitError):
            return False
        if isinstance(exc, httpx.HTTPError):
            status = _http_status_from_exc(exc)
            if status is None:
                return True
            return status >= 500
        if isinstance(exc, (DiscogsAPIError, BaseLinkerAPIError)):
            status = exc.context.get("status")
            if isinstance(status, int) and status >= 500:
                return True
            return exc.error_code in {
                "discogs_connection_error",
                "discogs_inventory_upload_conflict",
                "baselinker_connection_error",
            }
        return False

    def _rate_limit_wait(retry_state: Any) -> float:
        outcome = getattr(retry_state, "outcome", None)
        if outcome is not None:
            exc = outcome.exception()
            if isinstance(exc, RateLimitError):
                retry_after = exc.context.get("retry_after")
                if isinstance(retry_after, (int, float)) and retry_after > 0:
                    return min(float(retry_after), _max_rate_limit_wait)
        return float(_rate_limit_backoff(retry_state))

    def _should_retry_rate_limit(exc: BaseException) -> bool:
        if not isinstance(exc, RateLimitError):
            return False
        if exc.context.get("error_code") == "ERROR_BLOCKED_TOKEN":
            return False
        retry_after = exc.context.get("retry_after")
        if isinstance(retry_after, (int, float)) and retry_after > _max_rate_limit_wait:
            return False
        return True

    standard_retry = retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception(is_retryable_exception),
        reraise=True,
    )

    rate_limit_retry = retry(
        stop=stop_after_attempt(3),
        wait=_rate_limit_wait,
        retry=retry_if_exception(_should_retry_rate_limit),
        reraise=True,
    )


__all__ = ["standard_retry", "rate_limit_retry", "is_retryable_exception"]
