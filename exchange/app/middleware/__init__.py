"""Middleware package.

This package contains middleware components:
- auth.py: Authentication middleware (secret validation)
- exception_handlers.py: Exception handlers for FastAPI
"""

from exchange.app.middleware.auth import (
    require_secret,
    resolve_secret,
    scrub_sensitive_data,
    allowed_secret_values,
    BodySizeLimitMiddleware,
)
from exchange.app.middleware.exception_handlers import (
    connector_error_handler,
    discogs_error_handler,
    validation_exception_handler,
    error_payload,
)

__all__ = [
    # Auth
    "require_secret",
    "resolve_secret",
    "scrub_sensitive_data",
    "allowed_secret_values",
    "BodySizeLimitMiddleware",
    # Exception handlers
    "connector_error_handler",
    "discogs_error_handler",
    "validation_exception_handler",
    "error_payload",
]
