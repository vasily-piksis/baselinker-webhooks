"""Exception handlers for the Exchange API.

This module provides exception handlers for FastAPI:
- BaseDiscogsError handler
- RequestValidationError handler
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from exchange.errors import BaseDiscogsError, ValidationError
from exchange.logging_utils import get_correlation_id

log = logging.getLogger("exchange.app.errors")


def error_payload(
    code: str,
    message: str,
    *,
    correlation_id: str | None = None,
    request_id: str | None = None,
    details: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Create a standard error response payload.

    Args:
        code: Error code
        message: Error message

    Returns:
        Error response dictionary
    """
    payload: Dict[str, Any] = {
        "status": "ERROR",
        "code": code,
        "message": message,
    }
    payload["correlation_id"] = correlation_id or get_correlation_id()
    if request_id:
        payload["request_id"] = request_id
    if details:
        payload["details"] = details
    return payload


async def discogs_error_handler(request: Request, exc: BaseDiscogsError) -> JSONResponse:
    """Handle domain exceptions.

    Args:
        request: The FastAPI request
        exc: The domain exception

    Returns:
        JSONResponse with error details
    """
    log.error(
        "API error",
        extra={
            "error_code": exc.error_code,
            "http_status": exc.http_status,
            "correlation_id": exc.correlation_id,
            "request_id": exc.request_id,
            "exception_type": type(exc).__name__,
            "request_path": request.url.path,
            "request_method": request.method,
            "details": exc.context,
        },
    )
    return JSONResponse(
        status_code=exc.http_status,
        content=error_payload(
            exc.error_code,
            exc.message,
            correlation_id=exc.correlation_id,
            request_id=exc.request_id,
            details=exc.context,
        ),
    )


# Backward-compatible alias for legacy imports.
connector_error_handler = discogs_error_handler


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle RequestValidationError exceptions.

    Args:
        request: The FastAPI request
        exc: The RequestValidationError exception

    Returns:
        JSONResponse with validation error details
    """
    message = "; ".join(err.get("msg", "invalid payload") for err in exc.errors())
    error = ValidationError(
        message, error_code="validation_error", context={"errors": exc.errors()}
    )
    log.error(
        "Validation error",
        extra={
            "error_code": error.error_code,
            "http_status": error.http_status,
            "correlation_id": error.correlation_id,
            "request_id": error.request_id,
            "exception_type": type(error).__name__,
            "request_path": request.url.path,
            "request_method": request.method,
            "details": error.context,
        },
    )
    return JSONResponse(
        status_code=error.http_status,
        content=error_payload(
            error.error_code,
            error.message,
            correlation_id=error.correlation_id,
            request_id=error.request_id,
            details=error.context,
        ),
    )
