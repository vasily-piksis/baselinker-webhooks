# exchange/processors/inventory_processor.py
"""Inventory processor for handling inventory events.

This module provides functions for:
- Processing inventory events (add, update, delete)
- Managing idempotency for event processing
- Persisting events to database
- Reprocessing events
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from uuid import UUID

from exchange.clients.discogs_client import (
    create_listing,
    delete_listing,
    edit_listing,
    get_inventory_listing,
    upload_inventory_csv,
)
from exchange.clients.baselinker_client import BaseLinkerClient, add_inventory_product
from exchange.processors.basecom_processor import (
    build_basecom_rows,
    write_basecom_file,
)
from exchange.processors.order_mapping_grace import (
    load_order_mapping_grace,
    mark_order_mapping_grace,
)
from exchange.processors.recent_discogs_listing import (
    load_recent_discogs_listing,
    store_recent_discogs_listing,
)
from exchange.processors.discogs_csv_processor import (
    exchange_rows_from_baselinker_event,
    inventory_rows_from_baselinker_event,
    write_discogs_csv_file,
)
from exchange.processors.discogs_relist import execute_discogs_relist_flow
from exchange.settings import (
    BL_INVENTORY_ID,
    BL_SHOP_ID,
    DISCOGS_DEFAULT_LISTING_COMMENTS,
)
from exchange.utils.recent_result_cache import (
    delete_recent_result,
    load_recent_result,
    stable_digest,
    store_recent_result,
)
from exchange.utils import normalize_action
from database.repositories.discogs_csv_repository import DiscogsCsvRepository
from database.repositories.event_repository import EventRepository
from database.repositories.idempotency_repository import IdempotencyRepository
from database.session import get_session

log = logging.getLogger("exchange.processors.inventory")

# Threshold for using direct API vs CSV bulk upload
# Items <= this threshold use direct API (faster for small batches)
# Items > this threshold use CSV upload (more efficient for bulk)
DIRECT_API_THRESHOLD = 5
DIRECT_API_MUTABLE_STATUSES = {"For Sale", "Draft", "Expired"}
RECENT_SUCCESS_CACHE_NAMESPACE = "inventory-success"
RECENT_SUCCESS_CACHE_TTL = max(0, int(os.getenv("INVENTORY_RECENT_SUCCESS_TTL", "600")))
LISTING_FETCH_CACHE_NAMESPACE = "discogs-listing"
LISTING_FETCH_CACHE_TTL = max(0, int(os.getenv("DISCOGS_LISTING_FETCH_CACHE_TTL", "120")))
RETIRED_LISTING_CACHE_NAMESPACE = "discogs-retired-listing"
RETIRED_LISTING_CACHE_TTL = max(0, int(os.getenv("DISCOGS_RETIRED_LISTING_TTL", "1800")))
RECENT_SUCCESS_IGNORED_KEYS = frozenset(
    {
        "action_token",
        "bl_pass",
        "request_id",
        "correlation_id",
    }
)
SEMANTIC_IDEMPOTENCY_IGNORED_KEYS = RECENT_SUCCESS_IGNORED_KEYS | frozenset(
    {
        "request_uid",
        "action",
    }
)
QUANTITY_UPDATE_ACTIONS = frozenset(
    {
        "productquantity",
        "productsquantity",
        "productquantityupdate",
        "productsquantityupdate",
    }
)
PRICE_UPDATE_ACTIONS = frozenset(
    {
        "productpriceupdate",
        "productspriceupdate",
    }
)
PRODUCT_ADD_ACTIONS = frozenset({"productadd", "productsadd"})

_normalize_action = normalize_action


def _persist_event(
    action: str,
    payload: Dict[str, Any],
    status: str,
    detail: Dict[str, Any],
    *,
    idempotency_token: Optional[str] = None,
) -> UUID:
    """Persist an inventory event to the database.

    Args:
        action: Normalized inventory action name.
        payload: Event payload data.
        status: Processing status to persist.
        detail: Additional detail payload.
        idempotency_token: Optional idempotency token for deduplication.

    Returns:
        UUID for the persisted event.
    """
    # Merge detail into payload since model doesn't have detail field yet
    # This matches the migration script strategy
    payload_with_detail = payload.copy()
    if detail:
        payload_with_detail["_migration_detail"] = detail

    with get_session() as session:
        repo = EventRepository(session)
        event = repo.create_event(
            action=action,
            payload=payload_with_detail,
            status=status,
            idempotency_token=idempotency_token,
        )
        session.commit()
        return cast(UUID, event.event_id)


def _discogs_action_for_exchange(act: str) -> str:
    # Handle both singular and plural forms (Product* and Products*)
    if act in {"productadd", "productsadd"}:
        return "add"
    if act in {
        "productquantity", "productsquantity",
        "productquantityupdate", "productsquantityupdate",
        "productpriceupdate", "productspriceupdate",
    }:
        return "change"
    if act in {"productdelete", "productsdelete"}:
        return "delete"
    return ""


def _build_discogs_rows(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    return inventory_rows_from_baselinker_event(body)


def _shop_key() -> str:
    if not BL_SHOP_ID:
        return ""
    return BL_SHOP_ID if BL_SHOP_ID.startswith("shop_") else f"shop_{BL_SHOP_ID}"


def _is_quantity_update_action(act: str) -> bool:
    return act in QUANTITY_UPDATE_ACTIONS


def _is_price_update_action(act: str) -> bool:
    return act in PRICE_UPDATE_ACTIONS


def _is_product_add_action(act: str) -> bool:
    return act in PRODUCT_ADD_ACTIONS


def _normalize_sequence(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        rows: List[Dict[str, Any]] = []
        for item in value:
            if isinstance(item, str):
                try:
                    item = json.loads(item)
                except Exception:
                    continue
            if isinstance(item, dict):
                rows.append(item)
        return rows
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(html.unescape(value))
        except Exception:
            return []
        return _normalize_sequence(parsed)
    if isinstance(value, dict):
        if value and all(str(key).isdigit() for key in value.keys()):
            return [item for _, item in sorted(value.items(), key=lambda pair: int(pair[0])) if isinstance(item, dict)]
        return [value]
    return []


def _extract_indexed_webhook_products(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    products: Dict[int, Dict[str, Any]] = {}
    for key, val in body.items():
        if not key.startswith("product_id") or key == "product_id":
            continue
        suffix = key[len("product_id"):]
        if not suffix.isdigit():
            continue
        pid = str(val).strip()
        if not pid:
            continue
        idx = int(suffix)
        entry: Dict[str, Any] = {"product_id": pid}
        raw_value = body.get(f"value{suffix}")
        if raw_value not in (None, ""):
            entry["quantity"] = raw_value
        variant_id = body.get(f"variant_id{suffix}")
        if variant_id not in (None, ""):
            entry["variant_id"] = variant_id
        products[idx] = entry
    return [products[idx] for idx in sorted(products)]


def _extract_quantity_update_items(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_products = _normalize_sequence(body.get("rows") or body.get("products") or body.get("items"))
    indexed_products = _extract_indexed_webhook_products(body)
    if indexed_products:
        raw_by_pid = {str(item.get("product_id") or ""): item for item in raw_products}
        merged: List[Dict[str, Any]] = []
        for indexed in indexed_products:
            base = raw_by_pid.get(str(indexed.get("product_id") or ""), {}).copy()
            base.update(indexed)
            merged.append(base)
        raw_products = merged
    if not raw_products and body.get("product_id") not in (None, ""):
        raw_products = [
            {
                "product_id": body.get("product_id"),
                "quantity": body.get("quantity") or body.get("value"),
                "sku": body.get("sku"),
            }
        ]
    return raw_products


def _extract_price_update_items(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_products = _normalize_sequence(body.get("rows") or body.get("products") or body.get("items"))
    indexed_products = _extract_indexed_webhook_products(body)
    if indexed_products:
        raw_by_pid = {str(item.get("product_id") or ""): item for item in raw_products}
        merged: List[Dict[str, Any]] = []
        for indexed in indexed_products:
            base = raw_by_pid.get(str(indexed.get("product_id") or ""), {}).copy()
            base.update(indexed)
            merged.append(base)
        raw_products = merged
    if not raw_products and body.get("product_id") not in (None, ""):
        raw_products = [
            {
                "product_id": body.get("product_id"),
                "price": body.get("price") or body.get("value"),
                "sku": body.get("sku"),
            }
        ]
    return raw_products


def _looks_like_discogs_listing_id(value: Any) -> bool:
    raw = str(value or "").strip()
    return raw.isdigit() and len(raw) >= 9


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _iter_feature_pairs(features: Any) -> List[tuple[str, Any]]:
    pairs: List[tuple[str, Any]] = []
    if isinstance(features, dict):
        for key, value in features.items():
            pairs.append((str(key), value))
    elif isinstance(features, list):
        for item in features:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                pairs.append((str(item[0]), item[1]))
            elif isinstance(item, dict):
                name = item.get("name")
                if name not in (None, ""):
                    pairs.append((str(name), item.get("value")))
    return pairs


def _extract_feature_value(product: Dict[str, Any], *feature_names: str) -> str:
    accepted = {name.strip().lower() for name in feature_names if name}
    if not accepted:
        return ""
    sources = [
        product.get("features"),
        (product.get("text_fields") or {}).get("features"),
    ]
    for source in sources:
        for feature_name, feature_value in _iter_feature_pairs(source):
            if feature_name.strip().lower() in accepted and feature_value not in (None, ""):
                return str(feature_value)
    return ""


def _lookup_discogs_listing_sku(listing_id: str) -> str:
    listing = _load_linked_discogs_listing(listing_id)
    if not isinstance(listing, dict):
        raise ValueError(f"Discogs listing {listing_id} could not be loaded")
    return str(listing.get("external_id") or "").strip()


def _extract_text_field_value(product: Dict[str, Any], *field_names: str) -> str:
    text_fields = product.get("text_fields") or {}
    if not isinstance(text_fields, dict):
        return ""
    accepted = {name.strip().lower() for name in field_names if name}
    for key, value in text_fields.items():
        if str(key).strip().lower() in accepted and value not in (None, ""):
            return str(value)
    return ""


def _extract_text_field_by_pattern(product: Dict[str, Any], *patterns: str) -> str:
    text_fields = product.get("text_fields") or {}
    if not isinstance(text_fields, dict):
        return ""
    accepted = [pattern.strip().lower() for pattern in patterns if pattern]
    for key, value in text_fields.items():
        key_text = str(key).strip().lower()
        if value in (None, ""):
            continue
        for pattern in accepted:
            if pattern in key_text:
                return str(value)
    return ""


def _first_non_empty(*values: Any) -> str:
    for value in values:
        if value not in (None, ""):
            return str(value)
    return ""


def _extract_release_id(product: Dict[str, Any]) -> str:
    direct = _first_non_empty(
        product.get("release_id"),
        product.get("discogs_release_id"),
        _extract_text_field_value(product, "release_id", "discogs_release_id"),
        _extract_text_field_by_pattern(
            product,
            "release_id",
            "release id",
            "discogs_release",
            "discogs release",
        ),
        _extract_feature_value(
            product,
            "release id",
            "release_id",
            "discogs release id",
            "discogs_release_id",
            "discogs release id",
        ),
    )
    return direct.strip()


def _extract_discogs_media_condition(product: Dict[str, Any]) -> str:
    return _first_non_empty(
        product.get("media_condition"),
        product.get("extra_field_96935"),
        _extract_feature_value(product, "media condition", "media_condition"),
        product.get("condition"),
        _extract_feature_value(product, "condition"),
        "Mint (M)",
    )


def _extract_discogs_sleeve_condition(product: Dict[str, Any]) -> str:
    return _first_non_empty(
        product.get("sleeve_condition"),
        product.get("extra_field_96934"),
        _extract_feature_value(product, "sleeve condition", "sleeve_condition", "sleeve"),
        "Mint (M)",
    )


def _extract_product_price(product: Dict[str, Any]) -> float:
    for raw in (
        product.get("price"),
        product.get("price_brutto"),
        product.get("price_netto"),
    ):
        if raw not in (None, ""):
            price = _to_float(raw, default=0.0)
            if price > 0:
                return price
    prices = product.get("prices")
    if isinstance(prices, dict):
        for _, raw in sorted(prices.items(), key=lambda pair: str(pair[0])):
            price = _to_float(raw, default=0.0)
            if price > 0:
                return price
    return 0.0


def _extract_product_location(product: Dict[str, Any]) -> str:
    location = _first_non_empty(
        product.get("location"),
        _extract_text_field_value(product, "location"),
    )
    if location:
        return location
    locations = product.get("locations")
    if isinstance(locations, dict):
        for _, raw in sorted(locations.items(), key=lambda pair: str(pair[0])):
            if raw not in (None, ""):
                return str(raw)
    return ""


def _extract_product_comments(product: Dict[str, Any]) -> str:
    return _first_non_empty(
        product.get("comments"),
        _extract_feature_value(product, "discogs comments", "comments"),
        product.get("description"),
        _extract_text_field_value(product, "comments", "description"),
        _extract_text_field_by_pattern(
            product,
            "comments",
            "comment",
            "notes",
        ),
    )


def _extract_product_add_comments(product: Dict[str, Any]) -> str:
    return _first_non_empty(
        product.get("comments"),
        product.get("discogs_comments"),
        _extract_feature_value(product, "discogs comments", "comments"),
        _extract_text_field_value(product, "comments", "discogs_comments"),
        _extract_text_field_by_pattern(product, "discogs comments", "comments", "comment"),
        DISCOGS_DEFAULT_LISTING_COMMENTS,
    )


def _extract_baselinker_product_quantity(product: Dict[str, Any]) -> int:
    for key in ("quantity", "available", "amount"):
        if product.get(key) not in (None, ""):
            return _to_int(product.get(key), default=0)

    stock = product.get("stock")
    if isinstance(stock, dict):
        total = 0
        saw_value = False
        for _, raw in stock.items():
            if isinstance(raw, dict):
                nested = (
                    raw.get("quantity")
                    or raw.get("qty")
                    or raw.get("stock")
                    or raw.get("available")
                )
                if nested not in (None, ""):
                    total += _to_int(nested, default=0)
                    saw_value = True
            elif raw not in (None, ""):
                total += _to_int(raw, default=0)
                saw_value = True
        if saw_value:
            return total

    if stock not in (None, ""):
        return _to_int(stock, default=0)
    return 0


def _extract_allow_offers(product: Dict[str, Any]) -> bool:
    raw = _first_non_empty(
        product.get("allow_offers"),
        _extract_feature_value(
            product,
            "allow offers",
            "allow_offers",
            "accept offers",
            "discogs allow offers",
        ),
        _extract_text_field_by_pattern(
            product,
            "allow_offer",
            "allow offers",
            "accept_offer",
            "accept offers",
            "offers",
        ),
    )
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _extract_product_format_quantity(product: Dict[str, Any]) -> str:
    return _first_non_empty(
        product.get("format_quantity"),
        product.get("extra_field_32139"),
        _extract_text_field_by_pattern(
            product,
            "format_quantity",
            "format quantity",
        ),
        _extract_feature_value(
            product,
            "format quantity",
            "format_quantity",
            "discogs format quantity",
        ),
    )


def _product_add_payload_items(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    products = _normalize_sequence(body.get("rows") or body.get("products") or body.get("items"))
    if not products:
        return [body] if body else []

    merged_products: List[Dict[str, Any]] = []
    for product in products:
        merged = dict(body)
        merged.update(product)
        merged_products.append(merged)
    return merged_products


def _build_product_add_rows_from_webhook(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product in _product_add_payload_items(body):
        format_value = str(product.get("format") or "").strip()
        release_id = _first_non_empty(
            _extract_release_id(product),
            product.get("extra_field_32141"),
            format_value if format_value.isdigit() else "",
        )
        if not release_id:
            continue

        price = _extract_product_price(product)
        quantity = _extract_baselinker_product_quantity(product)
        sku = _first_non_empty(
            product.get("sku"),
            product.get("external_id"),
            product.get("external_sku"),
        )
        row: Dict[str, Any] = {
            "release_id": _to_int(release_id),
            "external_id": sku,
            "price": price,
            "media_condition": _extract_discogs_media_condition(product),
            "sleeve_condition": _extract_discogs_sleeve_condition(product),
            "comments": _extract_product_add_comments(product),
            "allow_offers": _extract_allow_offers(product),
            "status": "For Sale" if price > 0 and quantity > 0 else "Draft",
            "quantity": quantity,
        }

        bl_product_id = _first_non_empty(
            product.get("bl_product_id"),
            product.get("product_id"),
            product.get("id"),
        )
        if bl_product_id:
            row["bl_product_id"] = bl_product_id

        variant_id = _first_non_empty(product.get("variant_id"), body.get("variant_id"))
        if variant_id:
            row["variant_id"] = variant_id

        location = _extract_product_location(product)
        if location:
            row["location"] = location

        weight = product.get("weight")
        if weight not in (None, ""):
            row["weight"] = weight

        format_quantity = _extract_product_format_quantity(product)
        if format_quantity:
            row["format_quantity"] = format_quantity

        rows.append(row)
    return rows


def _extract_current_linked_listing_id(product: Dict[str, Any]) -> str:
    links = product.get("links")
    if not isinstance(links, dict):
        return ""
    shop_key = _shop_key()
    if shop_key:
        link = links.get(shop_key)
        if isinstance(link, dict):
            return str(link.get("product_id") or "").strip()
    for _, link in links.items():
        if isinstance(link, dict):
            linked_id = str(link.get("product_id") or "").strip()
            if linked_id:
                return linked_id
    return ""


def _extract_link_variant_id(product: Dict[str, Any]) -> int:
    links = product.get("links")
    if not isinstance(links, dict):
        return 0
    shop_key = _shop_key()
    if shop_key:
        link = links.get(shop_key)
        if isinstance(link, dict):
            return _to_int(link.get("variant_id"), default=0)
    return 0


def _has_inspected_tag(product: Dict[str, Any]) -> bool:
    tags = product.get("tags")
    if not isinstance(tags, list):
        return False
    return any(str(tag).strip().lower() == "inspected" for tag in tags)


def _load_linked_discogs_listing(
    listing_id: str,
    *,
    use_cache: bool = True,
) -> Optional[Dict[str, Any]]:
    if not listing_id or listing_id == "0":
        return None
    if use_cache:
        cached = _load_cached_listing(listing_id)
        if cached is not None:
            return cached
    try:
        listing = get_inventory_listing(listing_id)
    except Exception as exc:
        log.debug("Linked Discogs listing %s is not reusable: %s", listing_id, exc)
        return None
    _store_cached_listing(listing_id, listing)
    return listing


def _candidate_discogs_listing_alive(listing: Optional[Dict[str, Any]], sku: str) -> bool:
    if not isinstance(listing, dict):
        return False
    status = str(listing.get("status") or "").strip().lower()
    if status == "sold":
        return False
    linked_sku = str(listing.get("external_id") or "").strip()
    if sku and linked_sku and linked_sku != sku:
        return False
    return True


def _load_recent_active_discogs_listing_for_sku(sku: str) -> Optional[Dict[str, Any]]:
    payload = load_recent_discogs_listing(sku)
    if not isinstance(payload, dict):
        return None
    listing_id = str(payload.get("listing_id") or "").strip()
    if not listing_id:
        return None
    listing = _load_linked_discogs_listing(listing_id, use_cache=False)
    if not _candidate_discogs_listing_alive(listing, sku):
        return None
    return {
        "listing_id": listing_id,
        "listing": listing,
        "source": str(payload.get("source") or "").strip(),
    }


def _delete_sold_discogs_listing_if_needed(
    listing_id: str,
    listing: Optional[Dict[str, Any]],
    *,
    ignore_order_mapping_grace: bool = False,
) -> Optional[Dict[str, Any]]:
    if not listing_id or listing_id == "0" or not isinstance(listing, dict):
        return None
    status = str(listing.get("status") or "").strip().lower()
    if status != "sold":
        return None
    grace_payload = None if ignore_order_mapping_grace else load_order_mapping_grace(listing_id)
    if grace_payload:
        return {
            "status": "skipped",
            "listing_id": listing_id,
            "reason": "order_mapping_grace",
            "grace": grace_payload,
        }
    try:
        result = delete_listing(listing_id=listing_id)
        _invalidate_cached_listing(listing_id)
        _mark_retired_listing(
            listing_id,
            reason="deleted_after_sold_cleanup",
            sku=str(listing.get("external_id") or "").strip(),
        )
        return {
            "status": "deleted",
            "listing_id": listing_id,
            "response": result,
        }
    except Exception as exc:
        if _is_discogs_not_found_error(exc):
            _invalidate_cached_listing(listing_id)
            _mark_retired_listing(
                listing_id,
                reason="already_missing_after_sold_cleanup",
                sku=str(listing.get("external_id") or "").strip(),
            )
            return {
                "status": "deleted",
                "listing_id": listing_id,
                "note": "Listing already missing in Discogs",
            }
        log.warning(
            "Failed to delete sold Discogs listing %s during quantity webhook cleanup: %s",
            listing_id,
            exc,
        )
        return {
            "status": "error",
            "listing_id": listing_id,
            "error": str(exc),
        }


def _fetch_inventory_products_data(
    bl_client: BaseLinkerClient,
    inventory_id: int,
    product_ids: List[int | str],
) -> Dict[str, Dict[str, Any]]:
    response = bl_client.get_inventory_products_data(inventory_id, product_ids)
    products = response.get("products") or {}
    if not isinstance(products, dict):
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, value in products.items():
        if isinstance(value, dict):
            normalized[str(key)] = value
    return normalized


def _find_bl_product_id_in_listing_scan(
    bl_client: BaseLinkerClient,
    inventory_id: int,
    *,
    webhook_product_id: str,
    webhook_sku: str = "",
) -> str:
    if webhook_sku:
        response = bl_client.get_inventory_products_list(
            inventory_id,
            page=1,
            filter_sku=webhook_sku,
        )
        products = response.get("products") or {}
        if isinstance(products, dict):
            for product_id, product in products.items():
                if not isinstance(product, dict):
                    continue
                sku = str(product.get("sku") or "").strip()
                if sku == webhook_sku:
                    return str(product_id)
        return ""

    if webhook_product_id and webhook_product_id.isdigit():
        response = bl_client.get_inventory_products_list(
            inventory_id,
            page=1,
            filter_id=webhook_product_id,
        )
        products = response.get("products") or {}
        if isinstance(products, dict):
            product = products.get(webhook_product_id)
            if isinstance(product, dict):
                return webhook_product_id
            for product_id in products.keys():
                if str(product_id) == webhook_product_id:
                    return str(product_id)
        return ""

    page = 1
    while True:
        response = bl_client.get_inventory_products_list(inventory_id, page=page)
        products = response.get("products") or {}
        if not isinstance(products, dict) or not products:
            break
        for product_id, product in products.items():
            if not isinstance(product, dict):
                continue
            product_id_str = str(product_id)
            sku = str(product.get("sku") or "").strip()
            if webhook_sku and sku == webhook_sku:
                return product_id_str
            if webhook_product_id and product_id_str == webhook_product_id:
                return product_id_str
        page += 1
    return ""


def _resolve_live_bl_product(
    bl_client: BaseLinkerClient,
    inventory_id: int,
    *,
    webhook_product_id: str,
    webhook_sku: str = "",
) -> tuple[str, Dict[str, Any]]:
    attempted_scans: set[tuple[str, str]] = set()

    def _resolve_by_scan(*, product_id: str, sku: str) -> tuple[str, Dict[str, Any]] | None:
        scan_key = (str(product_id or ""), str(sku or ""))
        if scan_key in attempted_scans:
            return None
        attempted_scans.add(scan_key)
        resolved_id = _find_bl_product_id_in_listing_scan(
            bl_client,
            inventory_id,
            webhook_product_id=product_id,
            webhook_sku=sku,
        )
        if not resolved_id:
            return None
        products = _fetch_inventory_products_data(bl_client, inventory_id, [resolved_id])
        product = products.get(resolved_id)
        if product is None:
            return None
        return resolved_id, product

    resolved_sku = webhook_sku
    if webhook_product_id and not resolved_sku and _looks_like_discogs_listing_id(webhook_product_id):
        try:
            resolved_sku = _lookup_discogs_listing_sku(webhook_product_id)
        except Exception as exc:
            log.debug(
                "Unable to derive SKU from Discogs listing %s during webhook resolution: %s",
                webhook_product_id,
                exc,
            )

    if resolved_sku:
        resolved = _resolve_by_scan(product_id="", sku=resolved_sku)
        if resolved:
            return resolved

    if webhook_product_id:
        resolved = _resolve_by_scan(product_id=webhook_product_id, sku="")
        if resolved:
            return resolved

    resolved = _resolve_by_scan(product_id=webhook_product_id, sku=resolved_sku)
    if resolved:
        return resolved

    raise ValueError(
        f"Unable to resolve BaseLinker product for webhook_product_id={webhook_product_id!r} "
        f"webhook_sku={webhook_sku!r}"
    )


def _update_baselinker_listing_link(
    *,
    bl_product_id: str,
    target_listing_id: str,
    current_product: Dict[str, Any],
) -> Dict[str, Any]:
    if not BL_INVENTORY_ID or not BL_SHOP_ID:
        raise ValueError("BL_INVENTORY_ID and BL_SHOP_ID must be configured for link updates")
    shop_key = _shop_key()
    current_listing_id = _extract_current_linked_listing_id(current_product)
    if current_listing_id == target_listing_id:
        return {"status": "noop", "product_id": bl_product_id, "listing_id": target_listing_id}

    payload: Dict[str, Any] = {
        "inventory_id": BL_INVENTORY_ID,
        "product_id": bl_product_id,
        "links": {
            shop_key: {
                "product_id": target_listing_id,
                "variant_id": _extract_link_variant_id(current_product),
            }
        },
    }
    result = add_inventory_product(payload)
    return {
        "status": "updated",
        "product_id": bl_product_id,
        "listing_id": target_listing_id,
        "response": result,
    }


def _build_discogs_listing_create_payload(product: Dict[str, Any], quantity: int) -> Dict[str, Any]:
    release_id = _extract_release_id(product)
    if not release_id:
        raise ValueError("release_id is required to create a Discogs listing")
    price = _extract_product_price(product)
    weight_kg = _to_float(product.get("weight"), default=0.0)
    payload: Dict[str, Any] = {
        "release_id": _to_int(release_id),
        "condition": _extract_discogs_media_condition(product),
        "price": price,
        "sleeve_condition": _extract_discogs_sleeve_condition(product),
        "comments": _extract_product_comments(product),
        "allow_offers": _extract_allow_offers(product),
        "status": "For Sale" if price > 0 and quantity > 0 else "Draft",
        "external_id": str(product.get("sku") or "").strip(),
        "location": _extract_product_location(product),
        "weight": int(weight_kg * 1000) if weight_kg > 0 else None,
        "format_quantity": _to_int(
            _first_non_empty(
                product.get("format_quantity"),
                _extract_text_field_by_pattern(
                    product,
                    "format_quantity",
                    "format quantity",
                ),
                _extract_feature_value(
                    product,
                    "format quantity",
                    "format_quantity",
                    "discogs format quantity",
                ),
            ),
            default=0,
        )
        or None,
    }
    return payload


def _reconcile_resolved_bl_product(
    *,
    bl_product_id: str,
    product: Dict[str, Any],
    quantity: int,
    webhook_product_id: str = "",
    webhook_sku: str = "",
    cleanup_listing_id_override: str = "",
    cleanup_listing_override: Optional[Dict[str, Any]] = None,
    delete_cleanup_listing: bool = True,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    sku = str(product.get("sku") or webhook_sku or "").strip()
    current_listing_id = _extract_current_linked_listing_id(product)
    current_listing = _load_linked_discogs_listing(current_listing_id, use_cache=False)
    cleanup_listing_id = (
        cleanup_listing_id_override
        or (current_listing_id if current_listing_id != "0" else "")
        or webhook_product_id
    )
    cleanup_listing = current_listing
    if cleanup_listing_id_override and cleanup_listing_override is not None:
        cleanup_listing = cleanup_listing_override
    elif cleanup_listing_id and cleanup_listing_id != current_listing_id:
        cleanup_listing = _load_linked_discogs_listing(cleanup_listing_id, use_cache=False)
    inspected = _has_inspected_tag(product)
    should_create_listing = inspected and quantity > 0
    reusable_listing = _candidate_discogs_listing_alive(current_listing, sku)

    row = {
        "external_id": sku,
        "quantity": quantity,
        "release_id": _extract_release_id(product),
        "listing_id": current_listing_id,
        "bl_product_id": bl_product_id,
        "tags": product.get("tags") or [],
    }

    if reusable_listing:
        current_status = str(current_listing.get("status") or "").strip()
        update_payload: Dict[str, Any] = {}
        action_name = "acknowledged"
        note = "Existing linked Discogs listing is already in the expected state"

        if quantity <= 0 and current_status != "Draft":
            update_payload["status"] = "Draft"
            action_name = "updated"
            note = "Existing linked Discogs listing moved to Draft because quantity is 0"
        elif quantity > 0 and current_status == "Draft":
            update_payload["status"] = "For Sale"
            action_name = "updated"
            note = "Existing linked Discogs Draft listing reactivated because quantity is positive"

        if update_payload:
            edit_listing(listing_id=current_listing_id, **update_payload)
            _invalidate_cached_listing(current_listing_id)

        cleanup_result = None
        if delete_cleanup_listing and cleanup_listing_id and cleanup_listing_id != current_listing_id:
            cleanup_result = _delete_sold_discogs_listing_if_needed(
                cleanup_listing_id,
                cleanup_listing,
                ignore_order_mapping_grace=True,
            )

        row["listing_id"] = current_listing_id
        result_payload = {
            "action": action_name,
            "bl_product_id": bl_product_id,
            "sku": sku,
            "quantity": quantity,
            "inspected": inspected,
            "listing_id": current_listing_id,
            "status": update_payload.get("status", current_status),
            "note": note,
        }
        if cleanup_result:
            result_payload["discogs_delete"] = cleanup_result
        return row, result_payload

    if not should_create_listing:
        link_update = _update_baselinker_listing_link(
            bl_product_id=bl_product_id,
            target_listing_id="0",
            current_product=product,
        )
        delete_result = None
        if delete_cleanup_listing:
            delete_result = _delete_sold_discogs_listing_if_needed(
                cleanup_listing_id,
                cleanup_listing,
                ignore_order_mapping_grace=True,
            )
        row["listing_id"] = "0"
        result_payload = {
            "action": "acknowledged" if link_update.get("status") == "noop" else "linked_zero",
            "bl_product_id": bl_product_id,
            "sku": sku,
            "quantity": quantity,
            "inspected": inspected,
            "listing_id": "0",
            "bl_update": link_update,
        }
        if delete_result:
            result_payload["discogs_delete"] = delete_result
        return row, result_payload

    recent_listing = _load_recent_active_discogs_listing_for_sku(sku)
    if recent_listing:
        recent_listing_id = str(recent_listing["listing_id"])
        link_update = _update_baselinker_listing_link(
            bl_product_id=bl_product_id,
            target_listing_id=recent_listing_id,
            current_product=product,
        )
        cleanup_result = None
        if delete_cleanup_listing and cleanup_listing_id and cleanup_listing_id != recent_listing_id:
            cleanup_result = _delete_sold_discogs_listing_if_needed(
                cleanup_listing_id,
                cleanup_listing,
                ignore_order_mapping_grace=True,
            )
        row["listing_id"] = recent_listing_id
        result_payload = {
            "action": "reused_recent",
            "bl_product_id": bl_product_id,
            "sku": sku,
            "quantity": quantity,
            "inspected": True,
            "listing_id": recent_listing_id,
            "bl_update": link_update,
            "note": "Reused recent Discogs listing for SKU to avoid duplicate relist",
        }
        if cleanup_result:
            result_payload["discogs_delete"] = cleanup_result
        return row, result_payload

    relist_result = execute_discogs_relist_flow(
        create_payload=_build_discogs_listing_create_payload(product, quantity),
        create_listing_fn=create_listing,
        link_back_fn=lambda new_listing_id: _update_baselinker_listing_link(
            bl_product_id=bl_product_id,
            target_listing_id=new_listing_id,
            current_product=product,
        ),
        old_listing_id=cleanup_listing_id,
        old_listing_status=str((cleanup_listing or {}).get("status") or ""),
        delete_old_listing_fn=(
            lambda retired_listing_id: _delete_sold_discogs_listing_if_needed(
                retired_listing_id,
                cleanup_listing,
                ignore_order_mapping_grace=True,
            )
        )
        if delete_cleanup_listing
        else None,
        respect_order_mapping_grace=False,
    )
    store_recent_discogs_listing(
        sku,
        listing_id=relist_result["listing_id"],
        source="inventory_reconcile",
    )
    row["listing_id"] = relist_result["listing_id"]
    result_payload = {
        "action": "created",
        "bl_product_id": bl_product_id,
        "sku": sku,
        "quantity": quantity,
        "inspected": True,
        "listing_id": relist_result["listing_id"],
        "discogs_create": relist_result["discogs_create"],
        "bl_update": relist_result["bl_update"],
    }
    if relist_result.get("discogs_delete"):
        result_payload["discogs_delete"] = relist_result["discogs_delete"]
    return row, result_payload


def _process_quantity_update_via_live_bl(
    body: Dict[str, Any],
) -> Dict[str, Any]:
    if not BL_INVENTORY_ID:
        raise ValueError("BL_INVENTORY_ID must be configured for quantity update processing")
    bl_client = BaseLinkerClient()
    try:
        inventory_id = int(BL_INVENTORY_ID)
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        rows: List[Dict[str, Any]] = []

        for item in _extract_quantity_update_items(body):
            webhook_product_id = str(item.get("product_id") or "").strip()
            webhook_sku = str(item.get("sku") or item.get("external_id") or "").strip()
            quantity = _to_int(
                item.get("quantity", item.get("stock", body.get("quantity", body.get("value")))),
                default=0,
            )
            if not webhook_product_id and not webhook_sku:
                errors.append({"row": item, "error": "webhook product identifier is required"})
                continue

            try:
                bl_product_id, product = _resolve_live_bl_product(
                    bl_client,
                    inventory_id,
                    webhook_product_id=webhook_product_id,
                    webhook_sku=webhook_sku,
                )
                row, result_payload = _reconcile_resolved_bl_product(
                    bl_product_id=bl_product_id,
                    product=product,
                    quantity=quantity,
                    webhook_product_id=webhook_product_id,
                    webhook_sku=webhook_sku,
                )
                rows.append(row)
                results.append(result_payload)
            except Exception as exc:
                log.warning("Quantity update processing failed for %s: %s", item, exc)
                errors.append({"row": item, "error": str(exc)})

        return {
            "method": "live_baselinker_lookup",
            "processed": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
            "rows": rows,
        }
    finally:
        bl_client.close()


def _process_price_update_via_live_bl(
    body: Dict[str, Any],
) -> Dict[str, Any]:
    if not BL_INVENTORY_ID:
        raise ValueError("BL_INVENTORY_ID must be configured for price update processing")
    bl_client = BaseLinkerClient()
    try:
        inventory_id = int(BL_INVENTORY_ID)
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        rows: List[Dict[str, Any]] = []

        for item in _extract_price_update_items(body):
            webhook_product_id = str(item.get("product_id") or "").strip()
            webhook_sku = str(item.get("sku") or item.get("external_id") or "").strip()
            price = _first_non_empty(
                item.get("price"),
                item.get("value"),
                body.get("price"),
                body.get("value"),
            )
            if not webhook_product_id and not webhook_sku:
                errors.append({"row": item, "error": "webhook product identifier is required"})
                continue

            try:
                bl_product_id, product = _resolve_live_bl_product(
                    bl_client,
                    inventory_id,
                    webhook_product_id=webhook_product_id,
                    webhook_sku=webhook_sku,
                )
                sku = str(product.get("sku") or webhook_sku or "").strip()
                current_listing_id = _extract_current_linked_listing_id(product)
                inspected = _has_inspected_tag(product)
                row = {
                    "external_id": sku,
                    "price": price,
                    "listing_id": current_listing_id,
                    "bl_product_id": bl_product_id,
                    "tags": product.get("tags") or [],
                }

                target_listing_id = (
                    current_listing_id
                    if current_listing_id and current_listing_id != "0"
                    else webhook_product_id
                )
                if not target_listing_id or target_listing_id == "0":
                    row["listing_id"] = "0"
                    rows.append(row)
                    results.append(
                        {
                            "action": "acknowledged",
                            "bl_product_id": bl_product_id,
                            "sku": sku,
                            "price": price,
                            "inspected": inspected,
                            "listing_id": "0",
                            "note": "Skipped Discogs price update because product has no linked listing",
                        }
                    )
                    continue

                update_row: Dict[str, Any] = {
                    "listing_id": target_listing_id,
                    "external_id": sku,
                    "price": price,
                }
                live_quantity = _extract_baselinker_product_quantity(product)
                if inspected and live_quantity > 0:
                    update_row["quantity"] = live_quantity

                update_response = _process_via_direct_api(
                    "change",
                    [update_row],
                )
                update_results = update_response.get("results") or []
                update_action = "updated"
                if update_results and isinstance(update_results[0], dict):
                    update_action = str(update_results[0].get("action") or update_action)
                row["listing_id"] = target_listing_id
                rows.append(row)
                results.append(
                    {
                        "action": update_action,
                        "bl_product_id": bl_product_id,
                        "sku": sku,
                        "price": price,
                        "inspected": inspected,
                        "listing_id": target_listing_id,
                        "discogs_update": update_response,
                    }
                )
            except Exception as exc:
                log.warning("Price update processing failed for %s: %s", item, exc)
                errors.append({"row": item, "error": str(exc)})

        return {
            "method": "live_baselinker_price_lookup",
            "processed": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
            "rows": rows,
        }
    finally:
        bl_client.close()


def reconcile_sold_order_listing(
    listing_id: str,
    *,
    delete_old_listing: bool = True,
) -> Dict[str, Any]:
    listing_id = str(listing_id or "").strip()
    if not listing_id:
        raise ValueError("listing_id is required for sold order reconciliation")
    if not BL_INVENTORY_ID:
        raise ValueError("BL_INVENTORY_ID must be configured for sold order reconciliation")

    bl_client = BaseLinkerClient()
    try:
        inventory_id = int(BL_INVENTORY_ID)

        try:
            bl_product_id, product = _resolve_live_bl_product(
                bl_client,
                inventory_id,
                webhook_product_id=listing_id,
                webhook_sku="",
            )
            if not delete_old_listing:
                mark_order_mapping_grace(
                    listing_id,
                    sku=str(product.get("sku") or "").strip(),
                    source="sold_order_reconcile",
                )
        except ValueError as exc:
            cleanup_listing = _load_cached_listing(listing_id)
            if not delete_old_listing:
                mark_order_mapping_grace(
                    listing_id,
                    sku=str((cleanup_listing or {}).get("external_id") or "").strip(),
                    source="sold_order_reconcile",
                )
            row = {
                "external_id": str((cleanup_listing or {}).get("external_id") or "").strip(),
                "quantity": 0,
                "listing_id": listing_id,
                "bl_product_id": "",
                "tags": [],
            }
            delete_result = None
            action_name = "acknowledged"
            note = f"No BaseLinker product resolved for sold order listing: {exc}"
            if delete_old_listing:
                delete_result = _delete_sold_discogs_listing_if_needed(listing_id, cleanup_listing)
                if delete_result:
                    action_name = "deleted_orphan"
            else:
                note = (
                    "No BaseLinker product resolved for sold order listing; "
                    f"preserving Discogs listing for downstream order mapping: {exc}"
                )
            result_payload = {
                "action": action_name,
                "bl_product_id": "",
                "sku": row["external_id"],
                "quantity": 0,
                "inspected": None,
                "listing_id": listing_id,
                "note": note,
            }
            if delete_result:
                result_payload["discogs_delete"] = delete_result
            return {
                "method": "sold_order_reconcile",
                "processed": 1,
                "failed": 0,
                "results": [result_payload],
                "errors": [],
                "rows": [row],
                "resolved_bl_product_id": "",
                "resolved_quantity": 0,
            }

        quantity = _extract_baselinker_product_quantity(product)
        row, result_payload = _reconcile_resolved_bl_product(
            bl_product_id=bl_product_id,
            product=product,
            quantity=quantity,
            webhook_product_id=listing_id,
            cleanup_listing_id_override=listing_id,
            delete_cleanup_listing=delete_old_listing,
        )
        return {
            "method": "sold_order_reconcile",
            "processed": 1,
            "failed": 0,
            "results": [result_payload],
            "errors": [],
            "rows": [row],
            "resolved_bl_product_id": bl_product_id,
            "resolved_quantity": quantity,
        }
    finally:
        bl_client.close()


def _process_via_direct_api(
    discogs_action: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Process inventory items via direct Discogs API (single-item operations).

    More efficient than CSV upload for small batches (1-5 items).

    Args:
        discogs_action: Action type ("add", "change", "delete").
        rows: List of inventory row dictionaries.

    Returns:
        Aggregated response with results for each item.
    """
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for row in rows:
        listing_id = row.get("listing_id")
        release_id = row.get("release_id")

        try:
            if discogs_action == "add":
                # Create new listing
                if not release_id:
                    errors.append({"row": row, "error": "release_id required for add"})
                    continue
                
                # SKU (external_id) is required from BaseLinker - log error but continue
                external_id = row.get("external_id")
                if not external_id:
                    log.error(
                        "SKU (external_id) missing for ProductAdd - release_id=%s, row=%s. "
                        "Skipping this product. Please provide SKU from BaseLinker.",
                        release_id, row
                    )
                    errors.append({
                        "row": row,
                        "error": "SKU (external_id) required for add - must be provided from BaseLinker"
                    })
                    continue
                
                # Parse allow_offers - can be boolean, "Y"/"N", "true"/"false", etc.
                allow_offers_raw = row.get("allow_offers")
                allow_offers = False
                if allow_offers_raw is True:
                    allow_offers = True
                elif isinstance(allow_offers_raw, str):
                    allow_offers = allow_offers_raw.lower().strip() in ("y", "yes", "true", "1", "on")
                
                price_val = float(row.get("price", 0))
                # If price is 0 or missing, list as Draft to avoid free sales
                add_status = str(row.get("status") or "").strip()
                if not add_status:
                    add_status = "Draft" if price_val <= 0 else "For Sale"
                recent_listing = _load_recent_active_discogs_listing_for_sku(str(external_id))
                if recent_listing:
                    result = {
                        "listing_id": recent_listing["listing_id"],
                        "status": "reused_recent",
                    }
                else:
                    result = create_listing(
                        release_id=int(release_id),
                        condition=row.get("condition") or row.get("media_condition") or "Mint (M)",
                        price=price_val,
                        sleeve_condition=row.get("sleeve_condition") or "Mint (M)",
                        comments=row.get("comments") or DISCOGS_DEFAULT_LISTING_COMMENTS,
                        allow_offers=allow_offers,
                        status=add_status,
                        external_id=external_id,
                        location=row.get("location"),
                        # BaseLinker stores weight in kg, Discogs expects grams
                        weight=int(float(row["weight"]) * 1000) if row.get("weight") else None,
                        format_quantity=int(row["format_quantity"]) if row.get("format_quantity") else None,
                    )
                
                discogs_listing_id = result.get("listing_id")
                bl_update_result = None
                if discogs_listing_id:
                    store_recent_discogs_listing(
                        external_id,
                        listing_id=discogs_listing_id,
                        source="direct_add",
                    )
                
                # After creating Discogs listing, update BaseLinker with the listing_id
                # This links the BaseLinker product to the Discogs listing
                bl_product_id = str(row.get("bl_product_id") or external_id or "").strip()
                if discogs_listing_id and bl_product_id and BL_INVENTORY_ID and BL_SHOP_ID:
                    try:
                        # Format shop_id properly (should be "shop_123" format)
                        shop_key = BL_SHOP_ID if BL_SHOP_ID.startswith("shop_") else f"shop_{BL_SHOP_ID}"
                        
                        bl_product_update = {
                            "inventory_id": BL_INVENTORY_ID,
                            "product_id": bl_product_id,
                            "links": {
                                shop_key: {
                                    "product_id": str(discogs_listing_id),
                                    "variant_id": _to_int(row.get("variant_id"), default=0),
                                }
                            }
                        }
                        bl_update_result = add_inventory_product(bl_product_update)
                        log.info(
                            "Updated BaseLinker product %s with Discogs listing_id %s: %s",
                            bl_product_id, discogs_listing_id, bl_update_result
                        )
                    except Exception as bl_exc:
                        # Log error but don't fail the flow - Discogs listing was created successfully
                        log.error(
                            "Failed to update BaseLinker product %s with Discogs listing_id %s: %s. "
                            "The Discogs listing was created but BaseLinker link was not updated.",
                            bl_product_id, discogs_listing_id, bl_exc
                        )
                elif discogs_listing_id and bl_product_id:
                    log.warning(
                        "Cannot update BaseLinker - BL_INVENTORY_ID or BL_SHOP_ID not configured. "
                        "Discogs listing %s created with SKU %s but BaseLinker link not updated.",
                        discogs_listing_id, external_id
                    )
                
                results.append({
                    "listing_id": discogs_listing_id,
                    "action": "reused_recent" if result.get("status") == "reused_recent" else "created",
                    "bl_product_id": bl_product_id,
                    "row": row,
                    "bl_update": bl_update_result,
                })

            elif discogs_action == "change":
                # Update existing listing
                if not listing_id:
                    errors.append({"row": row, "error": "listing_id required for change"})
                    continue
                retired_marker = _load_retired_listing_marker(listing_id)
                if retired_marker:
                    results.append({
                        "listing_id": listing_id,
                        "action": "acknowledged",
                        "status": "Retired",
                        "note": "Discogs listing already retired; skipping stale update",
                        "row": row,
                    })
                    continue
                
                # Fetch current listing data from Discogs to preserve existing values
                # This is critical - we can't send 0 price or we'd sell items for free!
                try:
                    current_listing = get_inventory_listing(listing_id)
                    _store_cached_listing(listing_id, current_listing)
                except Exception as fetch_exc:
                    if _is_discogs_not_found_error(fetch_exc):
                        _invalidate_cached_listing(listing_id)
                        _mark_retired_listing(
                            listing_id,
                            reason="discogs_listing_missing_during_change",
                            sku=str(row.get("external_id") or "").strip(),
                        )
                        results.append({
                            "listing_id": listing_id,
                            "action": "acknowledged",
                            "status": "Retired",
                            "note": "Discogs listing not found; treating stale update as no-op",
                            "row": row,
                        })
                        continue
                    log.warning("Failed to fetch listing %s: %s", listing_id, fetch_exc)
                    errors.append({"row": row, "error": f"Failed to fetch listing: {fetch_exc}"})
                    continue
                
                # Extract current values from the listing
                current_price = None
                if current_listing.get("price"):
                    price_data = current_listing["price"]
                    if isinstance(price_data, dict):
                        current_price = price_data.get("value")
                    else:
                        current_price = price_data
                
                current_condition = current_listing.get("condition")
                current_sleeve = current_listing.get("sleeve_condition")
                current_comments = current_listing.get("comments")
                current_location = current_listing.get("location")
                current_external_id = current_listing.get("external_id")
                current_status = current_listing.get("status")
                
                # Determine new values - use incoming if provided and valid, else keep current
                price_raw = row.get("price")
                price_value = None
                if price_raw not in (None, "", "0", "0.00", 0, 0.0):
                    try:
                        price_value = float(price_raw)
                        if price_value <= 0:
                            price_value = None
                    except (TypeError, ValueError):
                        price_value = None
                
                # Use current price if no new price provided
                if price_value is None:
                    price_value = float(current_price) if current_price else None
                
                condition = row.get("condition") or row.get("media_condition")
                if condition in (None, "", "Mint", "Mint (M)"):
                    condition = current_condition  # Keep existing
                
                sleeve_condition = row.get("sleeve_condition") or current_sleeve
                comments = row.get("comments") if row.get("comments") else current_comments
                location = row.get("location") if row.get("location") else current_location
                # Always sync external_id (SKU) from the incoming row to keep
                # Discogs in line with BaseLinker.  Fall back to the current
                # value when the incoming row has no SKU.
                external_id = row.get("external_id") or current_external_id
                
                # Parse quantity for status logic only when the source event provided it.
                quantity_raw = row.get("quantity")
                quantity_int: Optional[int] = None
                if quantity_raw is not None:
                    try:
                        quantity_int = int(quantity_raw)
                    except (TypeError, ValueError):
                        quantity_int = None

                # Determine status based on quantity:
                # - quantity <= 0: set status to "Draft" (hide from sale)
                # - quantity > 0 and was Draft: set status to "For Sale"
                new_status = None
                if quantity_int is not None:
                    if quantity_int <= 0 and current_status != "Draft":
                        new_status = "Draft"
                        log.info(
                            "Listing %s quantity=0, changing status from '%s' to 'Draft'",
                            listing_id, current_status
                        )
                    elif quantity_int > 0 and current_status == "Draft":
                        new_status = "For Sale"
                        log.info(
                            "Listing %s quantity=%s, changing status from 'Draft' to 'For Sale'",
                            listing_id, quantity_int
                        )
                
                log.info(
                    "Processing update for listing %s: qty=%s, price=%s (current=%s), status=%s->%s",
                    listing_id, quantity_int, price_value, current_price, current_status, new_status or current_status
                )
                
                # Build update - only send fields that differ from current
                has_changes = False
                update_kwargs: Dict[str, Any] = {"listing_id": listing_id}
                
                if price_value and price_value != current_price:
                    update_kwargs["price"] = price_value
                    has_changes = True
                if condition and condition != current_condition:
                    update_kwargs["condition"] = condition
                    has_changes = True
                if sleeve_condition and sleeve_condition != current_sleeve:
                    update_kwargs["sleeve_condition"] = sleeve_condition
                    has_changes = True
                if comments and comments != current_comments:
                    update_kwargs["comments"] = comments
                    has_changes = True
                if location and location != current_location:
                    update_kwargs["location"] = location
                    has_changes = True
                if external_id and external_id != current_external_id:
                    update_kwargs["external_id"] = external_id
                    has_changes = True
                
                # Add status change if needed (quantity-based)
                if new_status:
                    update_kwargs["status"] = new_status
                    has_changes = True
                
                if not has_changes:
                    # No changes needed - acknowledge
                    log.info(
                        "No changes for listing %s (qty=%s, status=%s)",
                        listing_id, quantity_int, current_status
                    )
                    results.append({
                        "listing_id": listing_id,
                        "action": "acknowledged",
                        "quantity": quantity_int,
                        "status": current_status,
                        "note": "No Discogs listing fields changed",
                        "row": row
                    })
                    continue

                if current_status not in DIRECT_API_MUTABLE_STATUSES:
                    note = (
                        f"Discogs listing status '{current_status}' cannot be modified via direct API"
                    )
                    log.warning(
                        "Skipping update for listing %s with immutable Discogs status '%s'",
                        listing_id,
                        current_status,
                    )
                    results.append({
                        "listing_id": listing_id,
                        "action": "skipped",
                        "quantity": quantity_int,
                        "status": current_status,
                        "note": note,
                        "row": row,
                    })
                    continue
                
                result = edit_listing(**update_kwargs)
                _invalidate_cached_listing(listing_id)
                results.append({
                    "listing_id": listing_id,
                    "action": "updated",
                    "quantity": quantity_int,
                    "status": new_status or current_status,
                    "row": row
                })

            elif discogs_action == "delete":
                # Delete listing
                if not listing_id:
                    errors.append({"row": row, "error": "listing_id required for delete"})
                    continue
                result = delete_listing(listing_id=listing_id)
                _invalidate_cached_listing(listing_id)
                results.append({"listing_id": listing_id, "action": "deleted", "row": row})

        except Exception as exc:
            log.warning("Direct API error for %s: %s", row, exc)
            errors.append({"row": row, "error": str(exc)})

    return {
        "method": "direct_api",
        "processed": len(results),
        "failed": len(errors),
        "results": results,
        "errors": errors,
    }


def _idempotency_digest(action: str, payload: Dict[str, Any], provided: Optional[str]) -> str:
    """Build a deterministic digest covering the action and payload.

    Falls back to hashing the sorted JSON payload when an explicit key is not provided.

    Args:
        action: Inventory action name.
        payload: Event payload data.
        provided: Explicit idempotency key, if available.

    Returns:
        SHA256 hex digest representing the idempotency key.
    """
    token_source = provided or payload.get("idempotency_key") or payload.get("request_uid")
    if token_source:
        raw = str(token_source)
    else:
        normalized_action = _normalize_action(action)
        if _is_quantity_update_action(normalized_action) or _is_price_update_action(normalized_action):
            return stable_digest(
                normalized_action,
                payload,
                ignored_keys=SEMANTIC_IDEMPOTENCY_IGNORED_KEYS,
            )
        try:
            raw = json.dumps({"action": action, "payload": payload}, sort_keys=True, default=str)
        except TypeError:
            raw = f"{action}:{repr(payload)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _recent_success_digest(action: str, payload: Dict[str, Any]) -> str:
    return stable_digest(
        _normalize_action(action),
        payload,
        ignored_keys=RECENT_SUCCESS_IGNORED_KEYS,
    )


def _load_recent_success_result(digest: str) -> Optional[Dict[str, Any]]:
    cached = load_recent_result(RECENT_SUCCESS_CACHE_NAMESPACE, digest)
    if cached is None:
        return None
    cached.setdefault("recent_cache_hit", True)
    return cached


def _store_recent_success_result(digest: str, result: Dict[str, Any]) -> None:
    if result.get("status") != "OK":
        return
    store_recent_result(
        RECENT_SUCCESS_CACHE_NAMESPACE,
        digest,
        result,
        ttl_seconds=RECENT_SUCCESS_CACHE_TTL,
    )


def _listing_cache_key(listing_id: Any) -> str:
    return str(listing_id).strip()


def _load_retired_listing_marker(listing_id: Any) -> Optional[Dict[str, Any]]:
    cache_key = _listing_cache_key(listing_id)
    if not cache_key:
        return None
    return load_recent_result(RETIRED_LISTING_CACHE_NAMESPACE, cache_key)


def _mark_retired_listing(
    listing_id: Any,
    *,
    reason: str,
    sku: str = "",
) -> None:
    cache_key = _listing_cache_key(listing_id)
    if not cache_key:
        return
    store_recent_result(
        RETIRED_LISTING_CACHE_NAMESPACE,
        cache_key,
        {
            "listing_id": cache_key,
            "retired": True,
            "reason": reason,
            "sku": sku,
        },
        ttl_seconds=RETIRED_LISTING_CACHE_TTL,
    )


def _is_discogs_not_found_error(exc: Exception) -> bool:
    text = str(exc).strip().lower()
    return "404" in text and "not found" in text


def _load_cached_listing(listing_id: Any) -> Optional[Dict[str, Any]]:
    cache_key = _listing_cache_key(listing_id)
    if not cache_key:
        return None
    return load_recent_result(LISTING_FETCH_CACHE_NAMESPACE, cache_key)


def _store_cached_listing(listing_id: Any, listing: Dict[str, Any]) -> None:
    cache_key = _listing_cache_key(listing_id)
    if not cache_key or not isinstance(listing, dict):
        return
    store_recent_result(
        LISTING_FETCH_CACHE_NAMESPACE,
        cache_key,
        listing,
        ttl_seconds=LISTING_FETCH_CACHE_TTL,
    )


def _invalidate_cached_listing(listing_id: Any) -> None:
    cache_key = _listing_cache_key(listing_id)
    if not cache_key:
        return
    delete_recent_result(LISTING_FETCH_CACHE_NAMESPACE, cache_key)


def _load_idempotent_result(digest: str) -> Optional[Dict[str, Any]]:
    """Load an idempotent result from the database.

    Args:
        digest: Idempotency digest to look up.

    Returns:
        Cached result payload if found, otherwise None.
    """
    try:
        with get_session() as session:
            repo = IdempotencyRepository(session)
            record = repo.get_by_digest(digest)
            if record:
                result = dict(record.result) if record.result else {}
                result.setdefault("idempotency_token", digest)
                return result
    except Exception as e:
        log.warning("Failed to read idempotency from DB: %s", e)
    return None


def _store_idempotent_result(digest: str, result: Dict[str, Any]) -> None:
    """Store an idempotent result in the database.

    Args:
        digest: Idempotency digest key.
        result: Result payload to store.
    """
    try:
        with get_session() as session:
            repo = IdempotencyRepository(session)
            repo.create_or_update(digest, result)
            session.commit()
    except Exception as e:
        log.error("Failed to store idempotency to DB: %s", e)


def process_inventory_event(
    action: str,
    body: Dict[str, Any],
    *,
    persist: bool = True,
    target: Path | None = None,
    idempotency_key: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """Process an inventory event.

    Args:
        action: The action name (e.g., ProductAdd, ProductDelete)
        body: The event payload
        persist: Whether to persist the event (to database)
        target: Deprecated: Optional target path for the event file (ignored for DB)
        idempotency_key: Optional idempotency key
        force: Whether to force reprocessing

    Returns:
        Result dictionary with status and details

    Example:
        >>> result = process_inventory_event(
        ...     "ProductAdd",
        ...     {"products": [{"product_id": "SKU-1", "quantity": 2}]},
        ... )
        >>> result.get("status")
        'OK'
    """
    act = _normalize_action(action)
    discogs_action = _discogs_action_for_exchange(act)
    uses_live_external_state = _is_quantity_update_action(act) or _is_price_update_action(act)
    recent_token: Optional[str] = None
    if not uses_live_external_state and not force and not idempotency_key:
        recent_token = _recent_success_digest(action, body)
        recent_cached = _load_recent_success_result(recent_token)
        if recent_cached is not None:
            log.info(
                "Inventory recent-success cache hit for action=%s token=%s",
                action,
                recent_token[:12],
            )
            return recent_cached
    token = _idempotency_digest(action, body, idempotency_key)
    if not uses_live_external_state and not force:
        cached = _load_idempotent_result(token)
        if cached is not None:
            if cached.get("status") == "QUEUED" and (
                cached.get("reason") == "no_rows"
            ):
                pass
            else:
                return cached

    event_id: UUID | None = None
    detail: Dict[str, Any]
    result: Dict[str, Any]
    if _is_quantity_update_action(act):
        try:
            response = _process_quantity_update_via_live_bl(body)
        except Exception as exc:
            log.exception("Live BaseLinker quantity processing failed for %s", act)
            detail = {
                "reason": "discogs_error",
                "error": str(exc),
                "exchange_rows": [],
                "method": "live_baselinker_lookup",
            }
            if persist:
                event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
            result = {
                "status": "QUEUED",
                "reason": "discogs_error",
                "detail": detail,
                "event_id": str(event_id) if event_id else None,
            }
            if token:
                result["idempotency_token"] = token
            return result

        detail = {
            "discogs_action": "change",
            "rows": response.get("rows") or [],
            "response": response,
            "exchange_rows": [],
            "basecom_rows": [],
            "basecom_export_record_id": None,
            "discogs_csv_record_id": None,
        }
        if persist:
            event_id = _persist_event(action, body, "processed", detail, idempotency_token=token)

        result = {
            "status": "OK",
            "discogs_action": "change",
            "rows_sent": len(detail["rows"]),
            "detail": detail,
            "event_id": str(event_id) if event_id else None,
        }
        if token:
            result["idempotency_token"] = token
        if recent_token:
            _store_recent_success_result(recent_token, result)
        return result

    if _is_price_update_action(act):
        try:
            response = _process_price_update_via_live_bl(body)
        except Exception as exc:
            log.exception("Live BaseLinker price processing failed for %s", act)
            detail = {
                "reason": "discogs_error",
                "error": str(exc),
                "exchange_rows": [],
                "method": "live_baselinker_price_lookup",
            }
            if persist:
                event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
            result = {
                "status": "QUEUED",
                "reason": "discogs_error",
                "detail": detail,
                "event_id": str(event_id) if event_id else None,
            }
            if token:
                result["idempotency_token"] = token
            return result

        detail = {
            "discogs_action": "change",
            "rows": response.get("rows") or [],
            "response": response,
            "exchange_rows": [],
            "basecom_rows": [],
            "basecom_export_record_id": None,
            "discogs_csv_record_id": None,
        }
        if persist:
            event_id = _persist_event(action, body, "processed", detail, idempotency_token=token)

        result = {
            "status": "OK",
            "discogs_action": "change",
            "rows_sent": len(detail["rows"]),
            "detail": detail,
            "event_id": str(event_id) if event_id else None,
        }
        if token:
            result["idempotency_token"] = token
        if recent_token:
            _store_recent_success_result(recent_token, result)
        return result

    # Source-of-truth guardrail: Discogs updates are driven by BaseLinker events only.
    exchange_rows = exchange_rows_from_baselinker_event(body)
    if not discogs_action:
        detail = {
            "reason": "unsupported_action",
            "action": action,
            "exchange_rows": exchange_rows,
        }
        if persist:
            event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
        result = {
            "status": "QUEUED",
            "reason": "unsupported_action",
            "detail": detail,
            "event_id": str(event_id) if event_id else None,
        }
        if token:
            result["idempotency_token"] = token
        if token:
            _store_idempotent_result(token, result)
        return result

    if _is_product_add_action(act):
        rows = _build_product_add_rows_from_webhook(body)
        if not rows:
            rows = _build_discogs_rows(body)
    else:
        rows = _build_discogs_rows(body)
    if not rows:
        detail = {"reason": "no_rows", "exchange_rows": exchange_rows}
        if persist:
            event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
        result = {
            "status": "QUEUED",
            "reason": "no_rows",
            "detail": detail,
            "event_id": str(event_id) if event_id else None,
        }
        if token:
            result["idempotency_token"] = token
        if token:
            _store_idempotent_result(token, result)
        return result

    # Decide whether to use direct API or CSV bulk upload
    # For "add" actions, always use direct API so we can get listing_id and update BaseLinker
    # For "change" and "delete" actions, use CSV bulk upload for large batches
    use_direct_api = len(rows) <= DIRECT_API_THRESHOLD or discogs_action == "add"
    discogs_csv_record_id: Optional[str] = None
    response: Dict[str, Any]

    if use_direct_api:
        # Use direct API for small batches (faster, no CSV overhead)
        log.info(
            "Using direct API for %s items (%s action)",
            len(rows),
            discogs_action,
        )
        try:
            response = _process_via_direct_api(discogs_action, rows)
            if response.get("failed", 0) > 0 and response.get("processed", 0) == 0:
                # All items failed - treat as error
                raise Exception(f"All {len(rows)} items failed: {response.get('errors')}")
        except Exception as exc:
            log.exception("Direct API processing failed for %s", act)
            detail = {
                "reason": "discogs_error",
                "error": str(exc),
                "exchange_rows": exchange_rows,
                "method": "direct_api",
            }
            if persist:
                event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
            result = {
                "status": "QUEUED",
                "reason": "discogs_error",
                "detail": detail,
                "event_id": str(event_id) if event_id else None,
            }
            if token:
                result["idempotency_token"] = token
                _store_idempotent_result(token, result)
            return result
    else:
        # Use CSV bulk upload for larger batches (more efficient)
        log.info(
            "Using CSV bulk upload for %s items (%s action)",
            len(rows),
            discogs_action,
        )
        try:
            discogs_csv_record_id = write_discogs_csv_file(
                discogs_action, rows, idempotency_token=token
            )
        except Exception as exc:  # pragma: no cover - filesystem/env specific
            log.exception("Failed to persist Discogs CSV for %s", act)
            detail = {
                "reason": "discogs_csv_error",
                "error": str(exc),
                "exchange_rows": exchange_rows,
            }
            if persist:
                event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
            result = {
                "status": "QUEUED",
                "reason": "discogs_csv_error",
                "detail": detail,
                "event_id": str(event_id) if event_id else None,
            }
            if token:
                result["idempotency_token"] = token
                _store_idempotent_result(token, result)
            return result

        try:
            response = upload_inventory_csv(discogs_action, rows, idempotency_key=token)
            response["method"] = "csv_upload"
        except Exception as exc:  # pragma: no cover - external dependency
            log.exception("Discogs inventory upload failed for %s", act)
            detail = {
                "reason": "discogs_error",
                "error": str(exc),
                "exchange_rows": exchange_rows,
                "discogs_csv_record_id": discogs_csv_record_id,
            }
            if persist:
                event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
            result = {
                "status": "QUEUED",
                "reason": "discogs_error",
                "detail": detail,
                "event_id": str(event_id) if event_id else None,
            }
            if token:
                result["idempotency_token"] = token
            if token:
                _store_idempotent_result(token, result)
            return result

        if discogs_csv_record_id:
            try:
                with get_session() as session:
                    repo = DiscogsCsvRepository(session)
                    repo.update_upload_status(
                        UUID(discogs_csv_record_id),
                        uploaded_at=datetime.now(timezone.utc),
                        upload_response=response,
                    )
                    session.commit()
            except Exception as exc:  # pragma: no cover - db/env specific
                log.warning(
                    "Failed to update Discogs CSV upload status %s: %s",
                    discogs_csv_record_id,
                    exc,
                )

    basecom_rows = build_basecom_rows(exchange_rows, rows)
    basecom_export_record_id: Optional[str] = None
    if basecom_rows:
        try:
            basecom_export_record_id = write_basecom_file(
                basecom_rows, action=action, event_token=token
            )
        except Exception as exc:  # pragma: no cover - environment specific
            log.exception("Failed to produce Base.com file for %s", act)
            detail = {
                "reason": "basecom_error",
                "error": str(exc),
                "exchange_rows": exchange_rows,
                "discogs_action": discogs_action,
                "rows": rows,
                "response": response,
                "basecom_rows": basecom_rows,
                "discogs_csv_record_id": discogs_csv_record_id,
            }
            if persist:
                event_id = _persist_event(action, body, "queued", detail, idempotency_token=token)
            result = {
                "status": "QUEUED",
                "reason": "basecom_error",
                "detail": detail,
                "event_id": str(event_id) if event_id else None,
            }
            if token:
                result["idempotency_token"] = token
                _store_idempotent_result(token, result)
            return result

    detail = {
        "discogs_action": discogs_action,
        "rows": rows,
        "response": response,
        "exchange_rows": exchange_rows,
        "basecom_rows": basecom_rows,
        "basecom_export_record_id": basecom_export_record_id,
        "discogs_csv_record_id": discogs_csv_record_id,
    }
    if persist:
        event_id = _persist_event(action, body, "processed", detail, idempotency_token=token)

    result = {
        "status": "OK",
        "discogs_action": discogs_action,
        "rows_sent": len(rows),
        "detail": detail,
        "event_id": str(event_id) if event_id else None,
    }
    if basecom_export_record_id:
        result["basecom_export_record_id"] = basecom_export_record_id
    if discogs_csv_record_id:
        result["discogs_csv_record_id"] = discogs_csv_record_id
    if token:
        result["idempotency_token"] = token
    if token:
        _store_idempotent_result(token, result)
    if recent_token:
        _store_recent_success_result(recent_token, result)
    return result


def reprocess_event(event_id: UUID) -> Dict[str, Any]:
    """Reprocess an event from the database.

    Args:
        event_id: The event UUID

    Returns:
        Result dictionary
    """
    with get_session() as session:
        repo = EventRepository(session)
        event = repo.get_event_by_id(event_id)
        if not event:
            return {
                "status": "ERROR",
                "reason": "event_not_found",
                "event_id": str(event_id),
            }

        # Extract data from event
        action = event.action
        payload = event.payload
        token = event.idempotency_token

        # Process
        result = process_inventory_event(
            action,
            payload,
            persist=False,
            idempotency_key=token,
            force=True,
        )

        # Update event status in DB
        new_status = result.get("status", "QUEUED")

        # EventRepository.update_event_status updates status and processed_at
        processed_at = datetime.now(timezone.utc) if new_status == "OK" else None
        repo.update_event_status(event_id, new_status, processed_at=processed_at)

        session.commit()
        return {"event_id": str(event_id), **result}


def _parse_event_ref(value: Path | str | UUID) -> Optional[UUID]:
    if isinstance(value, UUID):
        return value
    text = str(value)
    if text.startswith("db:"):
        text = text[3:]
        if text.endswith(".json"):
            text = text[:-5]
    try:
        return UUID(text)
    except ValueError:
        return None


def reprocess_event_file(path: Path | str | UUID) -> Dict[str, Any]:
    """Reprocess a legacy event reference (DB-only).

    Deprecated: Accepts a DB event ID (UUID or db:UUID) and reprocesses from DB.
    Event file paths are no longer supported.

    Args:
        path: Event UUID or db: reference string.

    Returns:
        Result payload from ``reprocess_event`` or an error payload.
    """
    event_id = _parse_event_ref(path)
    if not event_id:
        return {"status": "ERROR", "reason": "invalid_event_ref", "path": str(path)}
    return reprocess_event(event_id)


__all__ = [
    "DIRECT_API_THRESHOLD",
    "process_inventory_event",
    "reprocess_event",
    "reprocess_event_file",
]
