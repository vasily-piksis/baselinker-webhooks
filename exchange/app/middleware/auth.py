"""Authentication middleware for the Exchange API.

This module provides authentication and authorization functionality:
- Secret validation for API requests
- Sensitive data scrubbing for logging
- Request body size limiting
"""

from __future__ import annotations

import os
from typing import Any, Awaitable, Callable, Dict, List, Optional

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from exchange.errors import raise_error
from exchange.app.middleware.exception_handlers import error_payload

# Load allowed passwords from environment
BL_PASS = os.getenv("BL_PASS", "")
_ALLOWED_PASSES: List[str] = []
for env_name in ("BL_ALLOWED_PASSES", "BASELINKER_ALLOWED_PASSES"):
    raw = os.getenv(env_name)
    if not raw:
        continue
    for part in raw.split(","):
        cleaned = part.strip()
        if cleaned:
            _ALLOWED_PASSES.append(cleaned)
if BL_PASS:
    _ALLOWED_PASSES.append(BL_PASS)
_ALLOWED_PASSES = list(dict.fromkeys(_ALLOWED_PASSES))
_ALLOWED_PASS_SET = set(_ALLOWED_PASSES)

# Keys that contain secrets
_SECRET_KEYS = {"bl_pass", "baselinker_pass", "password", "secret"}

# Maximum request body size
MAX_BODY_BYTES = 2 * 1024 * 1024


def allowed_secret_values() -> set[str]:
    """Get the set of allowed secret values.

    Returns:
        Set of allowed password/secret values
    """
    allowed = set(_ALLOWED_PASS_SET)
    if BL_PASS:
        allowed.add(BL_PASS)
    return allowed


def require_secret(val: str | None) -> None:
    """Validate that the provided secret is allowed.

    Args:
        val: The secret value to validate

    Raises:
        BaseDiscogsError: If the secret is invalid or missing
    """
    allowed = allowed_secret_values()
    if not allowed:
        return
    if val in (None, ""):
        raise_error("unauthorized", "Invalid bl_pass", http_status=401)
    incoming = str(val)
    if incoming in allowed:
        return
    raise_error("unauthorized", "Invalid bl_pass", http_status=401)


def resolve_secret(req: Request, body: Dict[str, Any]) -> Optional[str]:
    """Extract the secret from request headers or body.

    Args:
        req: The FastAPI request object
        body: The parsed request body

    Returns:
        The secret value if found, None otherwise
    """
    header = (
        req.headers.get("X-BL-PASS")
        or req.headers.get("X-BL-PASSWORD")
        or req.headers.get("X-BASELINKER-PASS")
        or req.headers.get("baselinker-pass")
    )
    if header:
        return str(header)
    for key in ("bl_pass", "baselinker_pass", "password", "secret"):
        val = body.get(key)
        if val not in (None, ""):
            return str(val)
    return None


def scrub_sensitive_data(data: Any) -> Any:
    """Remove sensitive data from a dictionary for logging.

    Args:
        data: The data to scrub (typically a request body dict)

    Returns:
        The data with sensitive values replaced with "[REDACTED]"
    """
    if isinstance(data, dict):
        for key in list(data.keys()):
            if key.lower() in _SECRET_KEYS:
                data[key] = "[REDACTED]"
            elif isinstance(data[key], dict):
                scrub_sensitive_data(data[key])
            elif isinstance(data[key], list):
                for item in data[key]:
                    scrub_sensitive_data(item)
    return data


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Middleware to limit request body size.

    Rejects requests with body larger than max_body bytes.
    """

    def __init__(self, app: Any, *, max_body: int = MAX_BODY_BYTES) -> None:
        """Initialize the middleware.

        Args:
            app: Downstream ASGI application.
            max_body: Maximum request body size in bytes.
        """
        super().__init__(app)
        self.max_body = max_body

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > self.max_body:
                    return JSONResponse(
                        status_code=413,
                        content=error_payload(
                            "payload_too_large", "Request body exceeds allowed size"
                        ),
                    )
            except ValueError:
                pass

        body = await request.body()
        if len(body) > self.max_body:
            return JSONResponse(
                status_code=413,
                content=error_payload("payload_too_large", "Request body exceeds allowed size"),
            )

        request._body = body  # type: ignore[attr-defined]

        async def receive() -> Dict[str, Any]:
            return {"type": "http.request", "body": body, "more_body": False}

        request._receive = receive  # type: ignore[attr-defined]
        return await call_next(request)
