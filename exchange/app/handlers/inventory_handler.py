"""Inventory handler for the Exchange API.

This module provides inventory action handling:
- handle_inventory_action: Main inventory action dispatcher
- inventory_response: Response formatter for successful inventory actions
- queue_response: Response formatter for queued inventory actions
"""

from __future__ import annotations

import logging
import html
import json
from typing import Any, Dict

from exchange.processor import process_inventory_event
from exchange.state import delete_from_payload, upsert_from_payload

log = logging.getLogger(__name__)

from exchange.app.handlers.utils import (
    coerce_bool,
    idempotency_hint,
    with_cid,
)


def inventory_response(result: Dict[str, Any]) -> Dict[str, Any]:
    """Format a successful inventory action response.

    Args:
        result: The result from process_inventory_event

    Returns:
        Formatted response dictionary
    """
    detail = result.get("detail", {})
    rows = detail.get("rows", [])
    response = {
        "status": result.get("status"),
        "discogs_action": detail.get("discogs_action"),
        "rows_sent": len(rows),
        "discogs_response": detail.get("response"),
        "event_file": result.get("event_path"),
    }
    if result.get("idempotency_token"):
        response["idempotency_token"] = result["idempotency_token"]
    basecom_record_id = result.get("basecom_export_record_id") or detail.get(
        "basecom_export_record_id"
    )
    if basecom_record_id:
        response["basecom_export_record_id"] = basecom_record_id
    csv_record_id = result.get("discogs_csv_record_id") or detail.get("discogs_csv_record_id")
    if csv_record_id:
        response["discogs_csv_record_id"] = csv_record_id
    basecom_rows = detail.get("basecom_rows")
    if isinstance(basecom_rows, list):
        response["basecom_rows"] = len(basecom_rows)
    elif isinstance(basecom_rows, int):
        response["basecom_rows"] = basecom_rows
    return with_cid(response)


def queue_response(result: Dict[str, Any]) -> Dict[str, Any]:
    """Format a queued inventory action response.

    Args:
        result: The result from process_inventory_event

    Returns:
        Formatted response dictionary
    """
    response = {
        "status": result.get("status", "QUEUED"),
        "reason": result.get("reason") or result.get("detail", {}).get("reason"),
        "detail": result.get("detail"),
        "queued_file": result.get("event_path"),
    }
    if result.get("idempotency_token"):
        response["idempotency_token"] = result["idempotency_token"]
    detail = result.get("detail") or {}
    csv_record_id = detail.get("discogs_csv_record_id")
    if csv_record_id:
        response["discogs_csv_record_id"] = csv_record_id
    basecom_record_id = detail.get("basecom_export_record_id")
    if basecom_record_id:
        response["basecom_export_record_id"] = basecom_record_id
    return with_cid(response)


def _incoming_webhook_count(body: Dict[str, Any]) -> int:
    products = body.get("products") or body.get("rows") or body.get("items")
    if isinstance(products, str) and products:
        try:
            products = json.loads(html.unescape(products))
        except Exception:
            products = None
    if isinstance(products, list):
        return len(products)
    if isinstance(products, dict):
        if products and all(str(key).isdigit() for key in products.keys()):
            return len(products)
        return 1
    if body.get("product_id") not in (None, ""):
        return 1
    indexed = [
        key
        for key, value in body.items()
        if key.startswith("product_id") and key != "product_id" and value not in (None, "")
    ]
    return len(indexed)


def _webhook_counter(result: Dict[str, Any], body: Dict[str, Any]) -> int:
    """Return the number of webhook rows accepted from BaseLinker."""
    incoming_count = _incoming_webhook_count(body)
    detail = result.get("detail") or {}
    response = detail.get("response") or {}
    processed = response.get("processed", 0)
    failed = response.get("failed", 0)
    try:
        counter = int(processed or 0) + int(failed or 0)
    except (TypeError, ValueError):
        counter = 0
    if counter > 0:
        return counter
    rows_count = len(detail.get("rows") or [])
    if rows_count > 0:
        return rows_count
    if result.get("status") != "OK" and result.get("reason") == "discogs_error":
        return incoming_count
    return incoming_count if result.get("status") == "OK" else 0


def _result_error_summary(result: Dict[str, Any]) -> str:
    detail = result.get("detail") or {}
    direct_error = detail.get("error")
    if direct_error:
        return str(direct_error)

    response = detail.get("response") or {}
    errors = response.get("errors")
    if isinstance(errors, list) and errors:
        first_error = errors[0]
        if isinstance(first_error, dict):
            return str(first_error.get("error") or first_error)
        return str(first_error)
    if errors:
        return str(errors)
    return ""


def handle_inventory_action(action: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """Handle an inventory action.

    Args:
        action: The action name (e.g., ProductAdd, ProductDelete)
        body: The request body

    Returns:
        Response dictionary
    """
    force_flag = coerce_bool(body.get("force"))
    if "force" in body:
        body = dict(body)
        body.pop("force", None)
    params = body.get("parameters")
    if isinstance(params, dict) and "force" in params:
        params = dict(params)
        params.pop("force", None)
        body["parameters"] = params
    idem_hint = idempotency_hint(body)
    result = process_inventory_event(
        action,
        body,
        persist=True,
        idempotency_key=idem_hint,
        force=force_flag,
    )
    log.info(
        "Inventory action=%s result: status=%s reason=%s rows=%s error=%s",
        action,
        result.get("status"),
        result.get("reason"),
        len(result.get("detail", {}).get("rows") or []),
        _result_error_summary(result) or "-",
    )
    normalized = action.replace(".", "").lower()
    
    # BaseLinker expects specific response formats per action type
    # ProductsQuantityUpdate: {"counter": N}
    # ProductsPriceUpdate: {"counter": N}
    if normalized in {"productquantityupdate", "productsquantityupdate"}:
        return {"counter": _webhook_counter(result, body)}
    
    if normalized in {"productpriceupdate", "productspriceupdate"}:
        return {"counter": _webhook_counter(result, body)}
    
    # ProductAdd: {"product_id": "..."}
    if normalized in {"productadd", "productsadd"}:
        if result.get("status") == "OK":
            detail = result.get("detail", {})
            rows = detail.get("rows") or []
            upsert_from_payload(action, body, rows)
            response = detail.get("response", {})
            results_list = response.get("results", [])
            # Return the first created listing_id as product_id
            if results_list and results_list[0].get("listing_id"):
                return {"product_id": str(results_list[0]["listing_id"])}
            # Fallback to product_id from body if available
            product_id = body.get("product_id") or body.get("sku") or ""
            return {"product_id": str(product_id)}
        # On error, return empty product_id
        return {"product_id": ""}
    
    # ProductDelete: {} (empty response on success)
    if normalized in {"productdelete", "productsdelete"}:
        if result.get("status") == "OK":
            delete_from_payload(body)
        return {}
    
    if result.get("status") == "OK":
        detail = result.get("detail", {})
        rows = detail.get("rows") or []
        upsert_from_payload(action, body, rows)
        return inventory_response(result)
    return queue_response(result)
