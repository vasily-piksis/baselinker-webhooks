"""Stateless implementation of the BaseLinker Exchange protocol."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from exchange.app.handlers.inventory_handler import handle_inventory_action
from exchange.app.handlers.order_handler import sync_order_update_to_discogs
from exchange.app.handlers.utils import (
    build_delivery_entries,
    build_payment_entries,
    build_status_entries,
    collect_prices,
    collect_quantities,
    paginate,
    parse_ids,
    read_request_body,
    to_int,
    with_cid,
)
from exchange.app.middleware.auth import require_secret, resolve_secret, scrub_sensitive_data
from exchange.app.middleware.exception_handlers import error_payload
from exchange.app.routes.health import APP_BUILD, APP_VERSION
from exchange.bl_connector import BaseLinkerConnector
from exchange.errors import raise_error
from exchange.state import add_category, list_categories
from exchange.translation import PAYMENT_RECEIVED_STATUS, is_payment_received_order

router = APIRouter(tags=["exchange"])
log = logging.getLogger("exchange.app.routes.exchange")

SUPPORTED_METHODS = [
    "SupportedMethods", "FileVersion", "ProductsCategories", "ProductsList",
    "ProductsData", "ProductsPrices", "ProductsQuantity", "ProductsPriceUpdate",
    "ProductsQuantityUpdate", "ProductAdd", "CategoryAdd", "ProductDelete",
    "OrderAdd", "OrderUpdate", "OrdersGet", "StatusesList",
    "DeliveryMethodsList", "PaymentMethodsList",
]
ORDERS_PAGE_SIZE = 100


def _supported_methods() -> List[str]:
    return list(SUPPORTED_METHODS)


def _catalog_entries(result: Dict[str, Any], ids: List[str]) -> List[Dict[str, Any]]:
    selected = set(ids)
    entries: List[Dict[str, Any]] = []
    for product_id, product in result.items():
        if product_id == "pages" or not isinstance(product, dict):
            continue
        if selected and str(product_id) not in selected:
            continue
        entries.append(dict(product, product_id=str(product_id)))
    return entries


def _catalog_metric(connector: BaseLinkerConnector, body: Dict[str, Any], field: str) -> Dict[str, Any]:
    """Return prices or quantities directly from the current Discogs export."""
    page = max(1, to_int(body.get("page"), 1) or 1)
    per_page = max(1, min(to_int(body.get("per_page"), 1000) or 1000, 1000))
    requested_ids = parse_ids(body.get("products") or body.get("products_id") or body.get("ids"))
    result = connector.products_list(
        {
            "page": page,
            "per_page": per_page,
            "status": body.get("status"),
            "action_token": body.get("action_token"),
        }
    )
    entries = _catalog_entries(result, requested_ids)
    # When BaseLinker names explicit product IDs, its expected pagination is for
    # that selected set; otherwise the Discogs export pagination is preserved.
    if requested_ids:
        entries, _, pages = paginate(entries, page, per_page)
    else:
        pages = max(1, to_int(result.get("pages"), 1))
    values = collect_prices(entries) if field == "price" else collect_quantities(entries)
    values["pages"] = pages
    return values


def _orders_get(connector: BaseLinkerConnector, body: Dict[str, Any]) -> Dict[str, Any]:
    only_paid_raw = body.get("only_paid") if body.get("only_paid") not in (None, "") else body.get("onlyPaid")
    only_paid = str(only_paid_raw).strip().lower() in {"1", "true", "yes", "y", "on"}
    order_id = body.get("order_id")
    if order_id not in (None, ""):
        result = connector.orders_get({"order_id": order_id})
        order = result.get("order") or {}
        response: Dict[str, Any] = {"pages": 1}
        if order and (not only_paid or is_payment_received_order(order)):
            response[str(order_id)] = order
        return response

    page = max(1, to_int(body.get("page"), 1) or 1)
    result = connector.orders_list(
        {
            "page": page,
            "per_page": to_int(body.get("per_page"), ORDERS_PAGE_SIZE) or ORDERS_PAGE_SIZE,
            "status": PAYMENT_RECEIVED_STATUS if only_paid else body.get("status"),
            "only_paid": only_paid,
            "date_from": body.get("time_from") or body.get("date_from"),
        }
    )
    response = {str(order["order_id"]): order for order in result.get("orders", []) if order.get("order_id") not in (None, "")}
    response["pages"] = result.get("pages", 1)
    return response


@router.get("/exchange")
async def exchange_get_no_params() -> Any:
    return JSONResponse(status_code=401, content=error_payload("no_password", "Missing credentials"))


@router.post("/exchange")
async def exchange(req: Request) -> Any:
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    action = str(body.get("action") or body.get("method") or "").strip()
    if not action:
        raise_error("missing_action", "Missing action")
    normalized = action.replace(".", "")
    lower = normalized.lower()

    if lower == "supportedmethods":
        return _supported_methods()
    if lower in {"getmoduleinformation", "moduleinformation"}:
        return with_cid({
            "status": "OK", "version": "1.0", "module": "DiscogsExchange",
            "platform": "Discogs", "platform_version": APP_VERSION, "protocol": "2.0",
            "supported_methods": _supported_methods(),
            "file_version": {"platform": "Discogs Bridge", "version": APP_VERSION, "standard": 4, "build": APP_BUILD},
        })
    if lower == "fileversion":
        return {"platform": "Discogs Bridge", "version": APP_VERSION, "standard": 4}
    if lower == "productscategories":
        return list_categories()
    if lower == "categoryadd":
        name = str(body.get("name") or "").strip()
        if not name:
            raise_error("validation_error", "Category name is required")
        return with_cid({"status": "OK", "category_id": add_category(name, str(body.get("parent_id") or "") or None)})

    connector = BaseLinkerConnector()
    try:
        if lower in {"productslist", "productlist"}:
            return await run_in_threadpool(connector.products_list, body)
        if lower in {"productsdata", "productdata"}:
            ids = parse_ids(body.get("products_id") or body.get("products") or body.get("ids"))
            return await run_in_threadpool(connector.products_data, {"ids": ids}) if ids else {}
        if lower in {"productsprices", "productprices"}:
            return await run_in_threadpool(_catalog_metric, connector, body, "price")
        if lower in {"productsquantity", "productquantity"}:
            return await run_in_threadpool(_catalog_metric, connector, body, "quantity")
        if lower == "ordersget":
            return await run_in_threadpool(_orders_get, connector, body)
    except Exception as exc:  # pragma: no cover - defensive network boundary
        log.exception("Discogs request failed for %s", action)
        if lower.startswith("products"):
            return {"pages": 0}
        if lower == "ordersget":
            return {"pages": 1}
        raise_error("discogs_error", str(exc), http_status=502)

    if lower == "statuseslist":
        return build_status_entries()
    if lower == "deliverymethodslist":
        return build_delivery_entries()
    if lower == "paymentmethodslist":
        return build_payment_entries()
    if lower == "orderadd":
        # There is no local order inbox in this standalone service.  BaseLinker
        # only needs an acknowledgement; subsequent reads come from Discogs.
        order_id = body.get("order_id") or body.get("id") or ""
        return {"order_id": order_id}
    if lower == "orderupdate":
        result = await run_in_threadpool(sync_order_update_to_discogs, body)
        if result.get("status") == "ERROR":
            raise_error("discogs_update_failed", "; ".join(result.get("failed") or []) or "Discogs update failed", http_status=502)
        return {"counter": int(result.get("processed") or 0)}
    if lower in {"productadd", "productquantity", "productquantityupdate", "productpriceupdate", "productdelete", "productsquantityupdate", "productspriceupdate"}:
        return await run_in_threadpool(handle_inventory_action, normalized, body)
    raise_error("unsupported_action", f"Unsupported action: {action}. Try one of: {', '.join(_supported_methods())}")
