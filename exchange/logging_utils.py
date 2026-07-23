"""Logging helpers for correlation IDs and JSON log formatting."""

from __future__ import annotations

import json
import logging
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

_correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="-")
_sensitive_values: set[str] = set()


def get_correlation_id() -> str:
    """Return the current request correlation id.

    Returns:
        Correlation id string for the active request.
    """
    return _correlation_id_ctx.get("-")


def register_sensitive_value(value: str | None) -> None:
    """Register a value that should be redacted in logs.

    Args:
        value: The sensitive value to redact in log output.
    """
    if value:
        _sensitive_values.add(value)


def register_sensitive_values(values: Iterable[str | None]) -> None:
    """Register multiple sensitive values for log redaction.

    Args:
        values: Iterable of sensitive values to redact.
    """
    for value in values:
        register_sensitive_value(value)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Assign and propagate correlation ids for requests.

    Attributes:
        header_name: Header name used to read/write correlation ids.
    """

    header_name: str

    def __init__(self, app: ASGIApp, header_name: str = "X-Correlation-ID") -> None:
        """Initialize the middleware.

        Args:
            app: Downstream ASGI application.
            header_name: Header name to read/write correlation ids.
        """
        super().__init__(app)
        self.header_name = header_name

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Process an incoming request and attach a correlation id.

        Args:
            request: Incoming request.
            call_next: ASGI handler for the next middleware/app.

        Returns:
            The response from the downstream application.
        """
        supplied = request.headers.get(self.header_name)
        correlation_id = supplied or uuid.uuid4().hex
        token = _correlation_id_ctx.set(correlation_id)
        request.state.correlation_id = correlation_id
        try:
            response = await call_next(request)
        except Exception:
            raise
        finally:
            _correlation_id_ctx.reset(token)
        response.headers.setdefault(self.header_name, correlation_id)
        return response


class JsonLogFormatter(logging.Formatter):
    """Format log records as JSON with correlation ids and redaction."""

    def __init__(self) -> None:
        """Initialize the formatter."""
        super().__init__()

    def _redact_text(self, text: str) -> str:
        redacted = text
        for token in _sensitive_values:
            if token:
                redacted = redacted.replace(token, "***")
        return redacted

    def _redact_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._redact_text(value)
        if isinstance(value, dict):
            return {key: self._redact_value(val) for key, val in value.items()}
        if isinstance(value, list):
            return [self._redact_value(item) for item in value]
        return value

    def format(self, record: logging.LogRecord) -> str:
        """Render a log record as a JSON string.

        Args:
            record: Log record to format.

        Returns:
            JSON-encoded string for the log record.
        """
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        payload: Dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "message": self._redact_text(record.getMessage()),
            "correlation_id": getattr(record, "correlation_id", get_correlation_id()),
        }
        standard_keys = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
        }
        for key, value in record.__dict__.items():
            if key in standard_keys or key.startswith("_"):
                continue
            if key in payload:
                continue
            payload[key] = self._redact_value(value)
        if record.exc_info:
            payload["exc_info"] = self._redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)


class _CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id()
        return True


def setup_logging(level: int = logging.INFO) -> None:
    """Configure JSON logging and correlation id propagation.

    Args:
        level: Logging level to apply to the root logger.
    """
    root = logging.getLogger()
    already_configured = any(
        isinstance(handler.formatter, JsonLogFormatter)
        for handler in root.handlers
        if handler.formatter
    )
    formatter = JsonLogFormatter()

    if root.handlers:
        for handler in root.handlers:
            handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        root.addHandler(handler)

    if not already_configured:
        root.setLevel(level)
        root.addFilter(_CorrelationIdFilter())


__all__ = [
    "CorrelationIdMiddleware",
    "setup_logging",
    "get_correlation_id",
    "register_sensitive_value",
    "register_sensitive_values",
]
