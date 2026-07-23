"""Main FastAPI application for the Exchange API.

This module provides the FastAPI application setup:
- Application initialization
- Middleware registration
- Exception handler registration
- Route registration
"""

from __future__ import annotations

import os
from typing import Any, List, cast

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from exchange.errors import BaseDiscogsError
from exchange.logging_utils import (
    CorrelationIdMiddleware,
    register_sensitive_values,
    setup_logging,
)

from exchange.app.middleware.auth import (
    BodySizeLimitMiddleware,
    MAX_BODY_BYTES,
    _ALLOWED_PASSES,
)
from exchange.app.middleware.exception_handlers import (
    discogs_error_handler,
    validation_exception_handler,
)
from exchange.app.routes import (
    health_router,
    webhooks_router,
)
from exchange.app.routes.health import APP_VERSION

# Load environment variables
load_dotenv(override=False)

# Collect sensitive values for logging redaction
SENSITIVE_VALUES: List[str] = list(
    dict.fromkeys(
        value
        for value in (
            _ALLOWED_PASSES
            + [
                os.getenv("DISCOGS_TOKEN"),
                os.getenv("DISCOGS_UA") or os.getenv("DISCOGS_USER_AGENT"),
            ]
        )
        if value
    )
)

# Register sensitive values and setup logging
register_sensitive_values(SENSITIVE_VALUES)
setup_logging()

# Create FastAPI application
app = FastAPI(title="BaseLinker Webhooks", version=APP_VERSION)

# Add middleware
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(BodySizeLimitMiddleware, max_body=MAX_BODY_BYTES)

# Register exception handlers
app.add_exception_handler(BaseDiscogsError, cast(Any, discogs_error_handler))
app.add_exception_handler(RequestValidationError, cast(Any, validation_exception_handler))

# Include routers
app.include_router(health_router)
app.include_router(webhooks_router)
