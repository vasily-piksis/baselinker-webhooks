"""Connector handler for the Exchange API.

This module provides BaseLinker connector invocation:
- invoke_connector: Invoke a connector handler with error handling
- validate_payload: Validate request payload against Pydantic models
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, TypeVar, cast

import httpx
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ValidationError

from exchange.errors import BaseDiscogsError, raise_error

from exchange.app.handlers.utils import with_cid

log = logging.getLogger("exchange.app.handlers.connector")
TModel = TypeVar("TModel", bound=BaseModel)


async def invoke_connector(
    handler: Callable[[Dict[str, Any]], Dict[str, Any]], payload: Any
) -> Dict[str, Any]:
    """Invoke a connector handler with error handling.

    Args:
        handler: The connector handler function
        payload: The request payload (Pydantic model or dict)

    Returns:
        Response dictionary from the connector

    Raises:
        BaseDiscogsError: If the connector operation fails
    """
    if hasattr(payload, "model_dump"):
        body = payload.model_dump()
    else:
        body = payload
    try:
        result = await run_in_threadpool(handler, body)
        if not isinstance(result, dict):
            result = {"result": result}
        return with_cid(result)
    except BaseDiscogsError:
        raise
    except ValueError as exc:
        raise_error("invalid_request", str(exc))
    except RuntimeError as exc:
        message = str(exc)
        lowered = message.lower()
        if "discogs_token" in lowered:
            raise_error(
                "discogs_not_configured",
                "Discogs credentials missing (set DISCOGS_TOKEN)",
                http_status=503,
            )
        if "circuit breaker" in lowered:
            raise_error("discogs_unavailable", message, http_status=503)
        raise_error("connector_error", message, http_status=502)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        detail: str | None = None
        if exc.response is not None:
            try:
                data = exc.response.json()
                if isinstance(data, dict):
                    detail = json.dumps(data, ensure_ascii=False)
                else:
                    detail = str(data)
            except ValueError:
                detail = exc.response.text
        base_message = f"Discogs API returned HTTP {status}"
        message = f"{base_message}: {detail}" if detail else base_message
        raise_error(
            "discogs_http_error",
            message,
            http_status=502,
            context={"status": status, "detail": detail},
        )
    except httpx.HTTPError as exc:
        raise_error(
            "discogs_connection_error",
            f"Unable to reach Discogs: {exc}",
            http_status=503,
        )
    except Exception as exc:  # pragma: no cover
        log.exception("Unhandled connector exception: %s", exc)
        raise_error("internal_error", "Unexpected connector failure", http_status=500)


def validate_payload(model_cls: type[TModel], data: Dict[str, Any]) -> TModel:
    """Validate request payload against a Pydantic model.

    Args:
        model_cls: The Pydantic model class
        data: The data to validate

    Returns:
        Validated Pydantic model instance

    Raises:
        BaseDiscogsError: If validation fails
    """
    try:
        return cast(TModel, model_cls.model_validate(data))
    except ValidationError as exc:
        message = "; ".join(err.get("msg", "invalid payload") for err in exc.errors())
        raise_error("validation_error", message)
