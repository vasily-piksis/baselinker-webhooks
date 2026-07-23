"""BaseLinker Shop Integration routes for the Exchange API.

This module provides BaseLinker Shop Integration endpoints:
- /bl/products/list: ProductsList
- /bl/products/data: ProductData
- /bl/products/add: ProductAdd
- /bl/products/quantity: ProductQuantity
- /bl/products/quantity/update: ProductQuantityUpdate
- /bl/products/delete: ProductDelete
- /bl/orders/list: OrdersList
- /bl/orders/get: OrdersGet
- /bl/orders/status: OrdersStatus
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool

from exchange.bl_connector import BaseLinkerConnector
from exchange.schemas import (
    ErrorResponse,
    OrdersGetRequest,
    OrdersGetResponse,
    OrdersListRequest,
    OrdersListResponse,
    OrdersStatusRequest,
    OrdersStatusResponse,
    ProductDataRequest,
    ProductsListRequest,
)

from exchange.app.handlers.connector_handler import invoke_connector, validate_payload
from exchange.app.handlers.inventory_handler import handle_inventory_action
from exchange.app.handlers.utils import read_request_body
from exchange.app.middleware.auth import (
    require_secret,
    resolve_secret,
    scrub_sensitive_data,
)

router = APIRouter(prefix="/bl", tags=["baselinker"])

# Connector instance
connector = BaseLinkerConnector()

# Example payloads for OpenAPI documentation
ERROR_EXAMPLE: Dict[str, Any] = {
    "status": "ERROR",
    "code": "validation_error",
    "message": "Invalid request payload",
    "correlation_id": "demo-correlation",
}

PRODUCTS_LIST_REQUEST_EXAMPLE = {"page": 1, "per_page": 1000}
PRODUCTS_LIST_RESPONSE_EXAMPLE = {
    "4247722671": {
        "name": "Example Artist - Example Title (Vinyl, LP)",
        "quantity": 1,
        "price": 19.99,
        "sku": "SKU-1",
        "ean": "5901234123457",
        "location": "A-12",
        "currency": "GBP",
    },
    "pages": 1,
}
PRODUCTS_DATA_REQUEST_EXAMPLE = {"products_id": "4247722671"}
ORDERS_LIST_REQUEST_EXAMPLE = {"page": 1, "per_page": 25, "status": "New Order"}
ORDERS_STATUS_REQUEST_EXAMPLE = {
    "order_id": "13435027-2",
    "status": "Shipped",
    "tracking_number": "1Z999",
    "message": "Shipped via UPS",
}
PRODUCTS_DATA_RESPONSE_EXAMPLE = {
    "4247722671": {
        "name": "Example Artist - Example Title (Vinyl, LP)",
        "sku": "SKU-1",
        "quantity": 1,
        "price": 19.99,
        "currency": "GBP",
        "tax": 0,
        "category_name": "Discogs Marketplace",
        "extra_field_32141": "12172034",
    },
}
ORDERS_LIST_RESPONSE_EXAMPLE = {
    "status": "OK",
    "page": 1,
    "per_page": 25,
    "counter": 1,
    "orders": [{"order_id": "13435027-2", "status": "New Order", "products": []}],
}
ORDERS_GET_RESPONSE_EXAMPLE = {
    "status": "OK",
    "order": {"order_id": "13435027-2", "status": "New Order", "products": []},
}
ORDERS_STATUS_RESPONSE_EXAMPLE = {
    "status": "OK",
    "order_id": "13435027-2",
    "updated_fields": ["status"],
    "message_sent": True,
}

STANDARD_ERROR_RESPONSES: Dict[int | str, Dict[str, Any]] = {
    400: {
        "model": ErrorResponse,
        "description": "Invalid request",
        "content": {"application/json": {"example": ERROR_EXAMPLE}},
    },
    401: {
        "model": ErrorResponse,
        "description": "Unauthorized",
        "content": {
            "application/json": {
                "example": {
                    "status": "ERROR",
                    "code": "unauthorized",
                    "message": "Invalid bl_pass",
                    "correlation_id": "demo-correlation",
                }
            }
        },
    },
}


async def _inventory_endpoint(req: Request, action: str) -> Dict[str, Any]:
    """Common handler for inventory endpoints.

    Args:
        req: The FastAPI request
        action: The inventory action name

    Returns:
        Response dictionary
    """
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    body.setdefault("action", action)
    return await run_in_threadpool(handle_inventory_action, action, body)


@router.post(
    "/products/list",
    response_model=None,
    responses={
        200: {
            "description": "Discogs products translated to BaseLinker format",
            "content": {"application/json": {"example": PRODUCTS_LIST_RESPONSE_EXAMPLE}},
        },
        **STANDARD_ERROR_RESPONSES,
    },
    summary="ProductsList",
    openapi_extra={
        "requestBody": {
            "content": {"application/json": {"example": PRODUCTS_LIST_REQUEST_EXAMPLE}}
        },
    },
)
async def bl_products_list(req: Request) -> Dict[str, Any]:
    """ProductsList endpoint - list products in BaseLinker format.

    Args:
        req: The FastAPI request

    Request:
        - Body: ProductsListRequest

    Returns:
        Products list response

    Response:
        - 200: Shops API ProductsList response
    """
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    payload = validate_payload(ProductsListRequest, body)
    return await invoke_connector(connector.products_list, payload)


@router.post(
    "/products/data",
    response_model=None,
    responses={
        200: {
            "description": "Discogs listing data in BaseLinker format",
            "content": {"application/json": {"example": PRODUCTS_DATA_RESPONSE_EXAMPLE}},
        },
        **STANDARD_ERROR_RESPONSES,
    },
    summary="ProductData",
    openapi_extra={
        "requestBody": {
            "content": {"application/json": {"example": PRODUCTS_DATA_REQUEST_EXAMPLE}}
        },
    },
)
async def bl_products_data(req: Request) -> Dict[str, Any]:
    """ProductData endpoint - get product details in BaseLinker format.

    Args:
        req: The FastAPI request

    Request:
        - Body: ProductDataRequest

    Returns:
        Product data response

    Response:
        - 200: Shops API ProductsData response
    """
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    payload = validate_payload(ProductDataRequest, body)
    return await invoke_connector(connector.products_data, payload)


@router.post("/products/add")
async def bl_products_add(req: Request) -> Dict[str, Any]:
    """ProductAdd endpoint - add products.

    Args:
        req: The FastAPI request

    Request:
        - Body: BaseLinker ProductAdd payload

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductAdd")


@router.post("/products/quantity")
async def bl_products_quantity(req: Request) -> Dict[str, Any]:
    """ProductQuantity endpoint - get product quantities.

    Args:
        req: The FastAPI request

    Request:
        - Body: BaseLinker ProductQuantity payload

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductQuantity")


@router.post("/products/quantity/update")
async def bl_products_quantity_update(req: Request) -> Dict[str, Any]:
    """ProductQuantityUpdate endpoint - update product quantities.

    Args:
        req: The FastAPI request

    Request:
        - Body: BaseLinker ProductQuantityUpdate payload

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductQuantityUpdate")


@router.post("/products/delete")
async def bl_products_delete(req: Request) -> Dict[str, Any]:
    """ProductDelete endpoint - delete products.

    Args:
        req: The FastAPI request

    Request:
        - Body: BaseLinker ProductDelete payload

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductDelete")


@router.post(
    "/orders/list",
    response_model=OrdersListResponse,
    responses={
        200: {
            "description": "Discogs orders translated to BaseLinker format",
            "content": {"application/json": {"example": ORDERS_LIST_RESPONSE_EXAMPLE}},
        },
        **STANDARD_ERROR_RESPONSES,
    },
    summary="OrdersList",
    openapi_extra={
        "requestBody": {"content": {"application/json": {"example": ORDERS_LIST_REQUEST_EXAMPLE}}},
    },
)
async def bl_orders_list(req: Request) -> Dict[str, Any]:
    """OrdersList endpoint - list orders in BaseLinker format.

    Args:
        req: The FastAPI request

    Request:
        - Body: OrdersListRequest

    Returns:
        Orders list response

    Response:
        - 200: OrdersListResponse
    """
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    payload = validate_payload(OrdersListRequest, body)
    return await invoke_connector(connector.orders_list, payload)


@router.post(
    "/orders/get",
    response_model=OrdersGetResponse,
    responses={
        200: {
            "description": "Single Discogs order mapped to BaseLinker schema",
            "content": {"application/json": {"example": ORDERS_GET_RESPONSE_EXAMPLE}},
        },
        **STANDARD_ERROR_RESPONSES,
    },
    summary="OrdersGet",
)
async def bl_orders_get(req: Request) -> Dict[str, Any]:
    """OrdersGet endpoint - get a single order.

    Args:
        req: The FastAPI request

    Request:
        - Body: OrdersGetRequest

    Returns:
        Order details response

    Response:
        - 200: OrdersGetResponse
    """
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    payload = validate_payload(OrdersGetRequest, body)
    return await invoke_connector(connector.orders_get, payload)


@router.post(
    "/orders/status",
    response_model=OrdersStatusResponse,
    responses={
        200: {
            "description": "Discogs order status/message update",
            "content": {"application/json": {"example": ORDERS_STATUS_RESPONSE_EXAMPLE}},
        },
        **STANDARD_ERROR_RESPONSES,
    },
    summary="OrdersStatusUpdate",
    openapi_extra={
        "requestBody": {
            "content": {"application/json": {"example": ORDERS_STATUS_REQUEST_EXAMPLE}}
        },
    },
)
async def bl_orders_status(req: Request) -> Dict[str, Any]:
    """OrdersStatus endpoint - update order status.

    Args:
        req: The FastAPI request

    Request:
        - Body: OrdersStatusRequest

    Returns:
        Order status update response

    Response:
        - 200: OrdersStatusResponse
    """
    body = await read_request_body(req)
    require_secret(resolve_secret(req, body))
    scrub_sensitive_data(body)
    payload = validate_payload(OrdersStatusRequest, body)
    return await invoke_connector(connector.orders_status, payload)
