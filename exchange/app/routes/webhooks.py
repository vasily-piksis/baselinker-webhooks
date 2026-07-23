"""Webhook routes for the Exchange API.

This module provides webhook endpoints for inventory actions:
- /product/add: ProductAdd webhook
- /product/quantity: ProductQuantity webhook
- /product/quantity/update: ProductQuantityUpdate webhook
- /product/price/update: ProductPriceUpdate webhook
- /product/delete: ProductDelete webhook
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool

from exchange.app.handlers.inventory_handler import handle_inventory_action
from exchange.app.handlers.utils import read_request_body
from exchange.app.middleware.auth import (
    require_secret,
    resolve_secret,
    scrub_sensitive_data,
)

router = APIRouter(tags=["webhooks"])


async def _inventory_endpoint(req: Request, action: str) -> Dict[str, Any]:
    """Common handler for inventory webhook endpoints.

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


@router.post("/product/add")
async def product_add(req: Request) -> Dict[str, Any]:
    """ProductAdd webhook endpoint.

    Args:
        req: The FastAPI request

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductAdd")


@router.post("/product/quantity")
async def product_quantity(req: Request) -> Dict[str, Any]:
    """ProductQuantity webhook endpoint.

    Args:
        req: The FastAPI request

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductQuantity")


@router.post("/product/quantity/update")
async def product_quantity_update(req: Request) -> Dict[str, Any]:
    """ProductQuantityUpdate webhook endpoint.

    Args:
        req: The FastAPI request

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductQuantityUpdate")


@router.post("/product/price/update")
async def product_price_update(req: Request) -> Dict[str, Any]:
    """ProductPriceUpdate webhook endpoint.

    Args:
        req: The FastAPI request

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductPriceUpdate")


@router.post("/product/delete")
async def product_delete(req: Request) -> Dict[str, Any]:
    """ProductDelete webhook endpoint.

    Args:
        req: The FastAPI request

    Returns:
        Inventory action response
    """
    return await _inventory_endpoint(req, "ProductDelete")
