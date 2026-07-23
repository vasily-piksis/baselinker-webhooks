"""Domain exceptions for consistent error handling."""

from __future__ import annotations

from typing import Any, Dict, NoReturn, Optional, Type

from exchange.logging_utils import get_correlation_id

ErrorContext = Dict[str, Any]


class BaseDiscogsError(Exception):
    """Base exception for all domain errors."""

    default_http_status: int = 500

    def __init__(
        self,
        message: str,
        *,
        error_code: Optional[str] = None,
        http_status: Optional[int] = None,
        correlation_id: Optional[str] = None,
        request_id: Optional[str] = None,
        context: Optional[ErrorContext] = None,
    ) -> None:
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.http_status = http_status if http_status is not None else self.default_http_status
        self.correlation_id = correlation_id
        self.request_id = request_id
        self.context = context or {}
        super().__init__(self.message)


class APIError(BaseDiscogsError):
    """Base exception for API errors."""

    default_http_status = 400


class ValidationError(APIError):
    """Request validation error."""

    default_http_status = 400


class AuthenticationError(APIError):
    """Authentication failure."""

    default_http_status = 401


class NotFoundError(APIError):
    """Resource not found."""

    default_http_status = 404


class ConflictError(APIError):
    """Resource conflict."""

    default_http_status = 409


class DatabaseError(BaseDiscogsError):
    """Base exception for database errors."""

    default_http_status = 500


class ConnectionError(DatabaseError):
    """Database connection error."""


class IntegrityError(DatabaseError):
    """Database integrity constraint violation."""

    default_http_status = 409


class TransactionError(DatabaseError):
    """Database transaction failure."""


class ClientError(BaseDiscogsError):
    """Base exception for client errors."""

    default_http_status = 502


class DiscogsAPIError(ClientError):
    """Discogs API error."""


class BaseLinkerAPIError(ClientError):
    """BaseLinker API error."""


class RateLimitError(ClientError):
    """Rate limiting error."""

    default_http_status = 429


class ProcessorError(BaseDiscogsError):
    """Base exception for processor errors."""

    default_http_status = 500


class TransformationError(ProcessorError):
    """Data transformation error."""

    default_http_status = 422


class ConnectorError(APIError):
    """Backward-compatible connector error (deprecated)."""

    def __init__(
        self,
        code: str,
        message: str,
        http_status: int = 400,
        *,
        correlation_id: Optional[str] = None,
        request_id: Optional[str] = None,
        context: Optional[ErrorContext] = None,
    ) -> None:
        super().__init__(
            message,
            error_code=code,
            http_status=http_status,
            correlation_id=correlation_id,
            request_id=request_id,
            context=context,
        )
        self.code = code


_CODE_CLASS_MAP: Dict[str, Type[BaseDiscogsError]] = {
    "invalid_request": ValidationError,
    "validation_error": ValidationError,
    "invalid_body": ValidationError,
    "missing_action": ValidationError,
    "unauthorized": AuthenticationError,
    "not_found": NotFoundError,
    "conflict": ConflictError,
    "discogs_not_configured": DiscogsAPIError,
    "discogs_unavailable": DiscogsAPIError,
    "discogs_http_error": DiscogsAPIError,
    "discogs_connection_error": DiscogsAPIError,
    "rate_limit": RateLimitError,
    "connector_error": ClientError,
    "internal_error": APIError,
}


def raise_error(
    code: str,
    message: str,
    http_status: int = 400,
    *,
    context: Optional[ErrorContext] = None,
    correlation_id: Optional[str] = None,
    request_id: Optional[str] = None,
    exc_type: Optional[Type[BaseDiscogsError]] = None,
) -> NoReturn:
    """Raise a structured API error with standard fields."""

    error_cls = exc_type or _CODE_CLASS_MAP.get(code, APIError)
    raise error_cls(
        message,
        error_code=code,
        http_status=http_status,
        correlation_id=correlation_id or get_correlation_id(),
        request_id=request_id,
        context=context,
    )


__all__ = [
    "BaseDiscogsError",
    "APIError",
    "ValidationError",
    "AuthenticationError",
    "NotFoundError",
    "ConflictError",
    "DatabaseError",
    "ConnectionError",
    "IntegrityError",
    "TransactionError",
    "ClientError",
    "DiscogsAPIError",
    "BaseLinkerAPIError",
    "RateLimitError",
    "ProcessorError",
    "TransformationError",
    "ConnectorError",
    "raise_error",
]
