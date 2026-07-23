"""Connector utilities for syncing Discogs listings into BaseLinker."""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from exchange.clients import discogs_client
from exchange.discogs_inventory_export import parse_discogs_inventory_export
from exchange.processors.order_mapping_grace import mark_order_mapping_grace
from exchange.translation import (
    PAYMENT_RECEIVED_STATUS,
    discogs_listing_to_bl_product,
    discogs_order_to_bl_order,
    enrich_discogs_order_release_details,
    extract_discogs_order_item_listing_id,
    is_payment_received_order,
)
from exchange.order_statuses import discogs_tracking_payload
from exchange.utils.recent_result_cache import load_recent_result, store_recent_result
from exchange.utils import iso_to_epoch, to_float, to_int

log = logging.getLogger("exchange.bl_connector")
load_dotenv(override=False)

_ORDER_LISTING_CLEANUP_CACHE_NAMESPACE = "order-listing-cleanup"
_PRODUCTS_LIST_PAGE_SIZE = 1000
_PRODUCTS_EXPORT_CACHE_TTL_SECONDS = max(
    1,
    to_int(os.getenv("DISCOGS_PRODUCTS_EXPORT_CACHE_TTL_SECONDS"), 900),
)
_PRODUCTS_EXPORT_TIMEOUT_SECONDS = max(
    30,
    to_int(os.getenv("DISCOGS_PRODUCTS_EXPORT_TIMEOUT_SECONDS"), 600),
)
_PRODUCTS_EXPORT_POLL_INTERVAL_SECONDS = max(
    1,
    to_int(os.getenv("DISCOGS_PRODUCTS_EXPORT_POLL_INTERVAL_SECONDS"), 5),
)
_PRODUCTS_EXPORT_DEFAULT_CURRENCY = (
    os.getenv("DISCOGS_EXPORT_CURRENCY")
    or os.getenv("DISCOGS_CURRENCY")
    or "GBP"
).upper()


def _order_update_lookback_seconds() -> int:
    days = os.getenv("DISCOGS_ORDER_UPDATE_LOOKBACK_DAYS")
    if days not in (None, ""):
        return max(0, to_int(days, 4) * 86400)
    return max(0, to_int(os.getenv("DISCOGS_ORDER_UPDATE_LOOKBACK_SECONDS"), 4 * 86400))


_ORDER_UPDATE_LOOKBACK_SECONDS = _order_update_lookback_seconds()
# Discogs order filters behave like the marketplace-created timezone it emits
# in order.created; UTC "Z" can return an empty window for recent orders.
_DISCOGS_ORDER_TIMEZONE = ZoneInfo(os.getenv("DISCOGS_ORDER_TIMEZONE", "America/Los_Angeles"))


def _discogs_created_filter_from_epoch(epoch: int) -> str:
    if not epoch:
        return ""
    return (
        datetime.fromtimestamp(epoch, tz=timezone.utc)
        .astimezone(_DISCOGS_ORDER_TIMEZONE)
        .replace(microsecond=0)
        .isoformat()
    )


def _product_id(product: Dict[str, Any]) -> str:
    """Extract product ID from a translated product dict."""
    return str(product.get("product_id") or product.get("id") or "")


def _order_changed_epoch(order: Dict[str, Any]) -> int:
    return to_int(order.get("date_confirmed") or order.get("date_add"))


def _order_created_epoch(order: Dict[str, Any]) -> int:
    return to_int(order.get("date_add"))


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_ids(value: Any) -> List[str]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(value).strip()]


def _base_product_fields(product: Dict[str, Any]) -> Dict[str, Any]:
    """Build the base product fields shared by ProductsList and ProductsData."""
    price = product.get("price", 0)
    return {
        "name": product.get("name", ""),
        "quantity": product.get("quantity", 0),
        "price": price,
        "sku": product.get("sku", ""),
        "ean": product.get("ean", ""),
        "location": product.get("location", ""),
    }


def _row_key(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def _row_value(row: Dict[str, Any], *candidates: str) -> Any:
    wanted = {_row_key(candidate) for candidate in candidates}
    for key, value in row.items():
        if _row_key(key) in wanted and value not in (None, ""):
            return value
    return None


def _row_text(row: Dict[str, Any], *candidates: str) -> str:
    value = _row_value(row, *candidates)
    return str(value).strip() if value not in (None, "") else ""


def _row_float(row: Dict[str, Any], *candidates: str) -> float:
    value = _row_value(row, *candidates)
    if isinstance(value, str) and "," in value and "." not in value:
        value = value.replace(",", ".")
    return round(to_float(value, 0.0), 2)


def _products_per_page(value: Any) -> int:
    return max(1, min(to_int(value, _PRODUCTS_LIST_PAGE_SIZE), _PRODUCTS_LIST_PAGE_SIZE))


def _discogs_export_row_name(row: Dict[str, Any], listing_id: str) -> str:
    artist = _row_text(row, "artist", "artists", "artist_name")
    title = _row_text(row, "title", "release_title", "release")
    format_text = _row_text(row, "format", "formats", "format_description")
    base = f"{artist} - {title}".strip(" -") if artist or title else listing_id
    return f"{base} ({format_text})" if format_text else base


def _discogs_export_row_to_product(row: Dict[str, Any]) -> tuple[str, Dict[str, Any]] | None:
    listing_id = _row_text(row, "listing_id", "listing id", "id")
    if not listing_id:
        return None

    sku = (
        _row_text(row, "external_id", "external id", "external_sku", "sku")
        or _row_text(row, "release_id", "release id")
        or listing_id
    )
    currency = (
        _row_text(row, "currency", "curr_abbr", "price_currency")
        or _PRODUCTS_EXPORT_DEFAULT_CURRENCY
    )
    quantity = max(0, to_int(_row_value(row, "quantity", "qty", "available"), 1))

    return listing_id, {
        "name": _discogs_export_row_name(row, listing_id),
        "quantity": quantity,
        "price": _row_float(row, "price"),
        "sku": sku,
        "ean": _row_text(row, "ean", "barcode", "barcodes"),
        "location": _row_text(row, "location", "item_location"),
        "currency": str(currency).upper(),
    }


def _row_status_matches(row: Dict[str, Any], status: Optional[str]) -> bool:
    if status in (None, ""):
        return True
    row_status = _row_text(row, "status")
    return row_status.lower() == str(status).strip().lower()


def _append_feature(features: List[List[str]], name: str, value: Any) -> None:
    if value in (None, ""):
        return
    text = str(value).strip()
    if text:
        features.append([name, text])


def _discogs_format_feature_value(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " | ".join(part.strip() for part in text.split(",") if part.strip())


class BaseLinkerConnector:
    """
    Proxy Discogs marketplace endpoints to BaseLinker-compatible payloads.
    """

    _PRODUCTS_CACHE_TTL = 45.0  # seconds
    _ORDER_LISTING_CLEANUP_TTL = 3600.0  # seconds
    _products_export_cache: Dict[str, tuple[float, Dict[str, Any]]] = {}
    _products_export_cache_lock = threading.Lock()

    def __init__(self) -> None:
        self._products_cache: Dict[str, tuple[float, Dict[str, Any]]] = {}

    def _enrich_order_release_details(self, order: Dict[str, Any]) -> Dict[str, Any]:
        return enrich_discogs_order_release_details(
            order,
            discogs_client.get_release,
            on_error=lambda release_id, exc: log.warning(
                "Unable to fetch Discogs release %s for order enrichment: %s",
                release_id,
                exc,
            ),
        )

    def _cleanup_cache_contains(self, listing_id: str) -> bool:
        return bool(load_recent_result(_ORDER_LISTING_CLEANUP_CACHE_NAMESPACE, listing_id))

    def _cleanup_cache_store(self, listing_id: str) -> None:
        store_recent_result(
            _ORDER_LISTING_CLEANUP_CACHE_NAMESPACE,
            listing_id,
            {"listing_id": listing_id},
            ttl_seconds=int(self._ORDER_LISTING_CLEANUP_TTL),
        )

    def _extract_order_listing_ids(self, order: Dict[str, Any]) -> List[str]:
        listing_ids: List[str] = []
        for item in order.get("items") or []:
            if not isinstance(item, dict):
                continue
            listing_id = extract_discogs_order_item_listing_id(item)
            if listing_id and listing_id not in listing_ids:
                listing_ids.append(listing_id)
        return listing_ids

    def _cleanup_sold_order_listings(self, orders: List[Dict[str, Any]]) -> None:
        for order in orders:
            order_id = str(order.get("id") or order.get("order_id") or "").strip()
            for listing_id in self._extract_order_listing_ids(order):
                if not listing_id or self._cleanup_cache_contains(listing_id):
                    continue
                try:
                    mark_order_mapping_grace(
                        listing_id,
                        source=f"order_read_path:{order_id or 'unknown'}",
                    )
                    self._cleanup_cache_store(listing_id)
                    log.info(
                        "Deferred sold Discogs listing %s cleanup during order read path for order %s",
                        listing_id,
                        order_id or "<unknown>",
                    )
                except Exception as exc:
                    log.warning(
                        "Failed to defer sold Discogs listing %s during order read path for order %s: %s",
                        listing_id,
                        order_id or "<unknown>",
                        exc,
                    )

    def _cache_key(
        self,
        page: int,
        per_page: int,
        status: Optional[str],
        action_token: Any = None,
    ) -> str:
        return f"{action_token or 'latest'}:{page}:{per_page}:{status or ''}"

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        entry = self._products_cache.get(key)
        if not entry:
            return None
        expires_at, payload = entry
        if expires_at < time.time():
            self._products_cache.pop(key, None)
            return None
        return payload

    def _cache_set(self, key: str, payload: Dict[str, Any]) -> None:
        self._products_cache[key] = (time.time() + self._PRODUCTS_CACHE_TTL, payload)

    def _export_cache_key(self, _payload: Dict[str, Any], status: Optional[str]) -> str:
        # BaseLinker may rotate action_token between ProductsList pages, while
        # Discogs exports are full-inventory snapshots. Reuse the same snapshot
        # for the current status filter instead of tying it to one request token.
        return f"inventory:{str(status or '').strip().lower()}"

    def _export_cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        with self._products_export_cache_lock:
            expired = [
                cached_key
                for cached_key, (expires_at, _) in self._products_export_cache.items()
                if expires_at < now
            ]
            for cached_key in expired:
                self._products_export_cache.pop(cached_key, None)
            entry = self._products_export_cache.get(key)
            if not entry:
                return None
            return entry[1]

    def _export_cache_set(self, key: str, snapshot: Dict[str, Any]) -> None:
        with self._products_export_cache_lock:
            self._products_export_cache[key] = (
                time.time() + _PRODUCTS_EXPORT_CACHE_TTL_SECONDS,
                snapshot,
            )

    def _products_from_inventory_export(
        self,
        payload: Dict[str, Any],
        status: Optional[str],
    ) -> Dict[str, Any]:
        cache_key = self._export_cache_key(payload, status)
        cached = self._export_cache_get(cache_key)
        if cached:
            return cached

        export_meta = discogs_client.start_inventory_export()
        export_id = export_meta.get("id") or export_meta.get("export_id")
        if not export_id:
            raise ValueError(f"Discogs inventory export did not return an id: {export_meta}")

        raw_export = discogs_client.download_inventory_export(
            export_id,
            poll_interval=_PRODUCTS_EXPORT_POLL_INTERVAL_SECONDS,
            timeout=_PRODUCTS_EXPORT_TIMEOUT_SECONDS,
        )
        rows = parse_discogs_inventory_export(raw_export)
        products: Dict[str, Any] = {}
        skipped_without_listing_id = 0
        for row in rows:
            if not _row_status_matches(row, status):
                continue
            product = _discogs_export_row_to_product(row)
            if not product:
                skipped_without_listing_id += 1
                continue
            listing_id, entry = product
            products[listing_id] = entry

        snapshot = {
            "export_id": str(export_id),
            "products": products,
            "row_count": len(rows),
            "skipped_without_listing_id": skipped_without_listing_id,
        }
        self._export_cache_set(cache_key, snapshot)
        return snapshot

    def products_list(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ProductsList from a single Discogs inventory export snapshot.

        Args:
            payload: BaseLinker-style request payload.

        Returns:
            BaseLinker-compatible ProductsList response.
        """
        page = max(1, to_int(payload.get("page"), 1))
        per_page = _products_per_page(payload.get("per_page"))
        status = payload.get("status")
        action_token = (
            payload.get("action_token") or payload.get("actionToken") or payload.get("token")
        )
        cache_key = self._cache_key(page, per_page, status, action_token)
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        snapshot = self._products_from_inventory_export(payload, status)
        products = snapshot.get("products") or {}
        product_ids = list(products.keys())
        total = len(product_ids)
        pages = max(1, (total + per_page - 1) // per_page)
        start = (page - 1) * per_page
        end = start + per_page
        products_dict = {pid: products[pid] for pid in product_ids[start:end]}

        result: Dict[str, Any] = dict(products_dict)
        result["pages"] = pages
        log.info(
            "ProductsList proxied",
            extra={
                "action": "ProductsList",
                "discogs_action": "inventory_export",
                "export_id": snapshot.get("export_id"),
                "rows": len(products_dict),
                "total_rows": total,
                "source_rows": snapshot.get("row_count"),
                "page": page,
                "pages": pages,
                "per_page": per_page,
                "status_filter": status,
            },
        )
        self._cache_set(cache_key, result)
        return result

    def products_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ProductsData by fetching Discogs listings.

        Args:
            payload: BaseLinker-style request payload.

        Returns:
            BaseLinker-compatible ProductsData response.
        """
        ids = _parse_ids(
            payload.get("products")
            or payload.get("products_id")
            or payload.get("ids")
            or payload.get("listing_ids")
        )
        if not ids:
            return {}
        max_ids = min(len(ids), to_int(payload.get("max_ids"), 100) or 100, 100)
        selected = ids[:max_ids]
        products: List[Dict[str, Any]] = []
        for listing_id in selected:
            try:
                listing = discogs_client.get_inventory_listing(listing_id)
            except Exception as exc:  # pragma: no cover - network dependency
                log.warning("Unable to fetch Discogs listing %s: %s", listing_id, exc)
                continue
            products.append(discogs_listing_to_bl_product(listing))

        products_dict: Dict[str, Any] = {}
        for product in products:
            pid = _product_id(product)
            if not pid:
                continue
            images = product.get("images") or []
            attributes = product.get("attributes") or []
            extra = product.get("extra") or {}
            if isinstance(extra, str):
                extra = {}
            release_id = product.get("release_id")
            features = [
                [attr.get("name", ""), attr.get("value", "")]
                for attr in attributes
                if isinstance(attr, dict)
            ]
            if release_id not in (None, ""):
                features.append(["Discogs Release ID", str(release_id)])
            format_text = extra.get("format", "") if isinstance(extra, dict) else ""
            _append_feature(features, "Discogs Comments", product.get("comments"))
            _append_feature(features, "Discogs Format Quantity", product.get("format_quantity"))
            _append_feature(features, "Discogs Format", _discogs_format_feature_value(format_text))
            _append_feature(features, "Discogs Artist(s)", product.get("artist"))
            _append_feature(features, "Discogs Title", product.get("title"))
            _append_feature(features, "Discogs Label(s)", product.get("labels"))
            _append_feature(features, "Discogs Catno", product.get("catno"))
            entry = _base_product_fields(product)
            entry.update({
                "currency": product.get("currency", ""),
                "tax": product.get("tax_rate", 0),
                "weight": product.get("weight", 0),
                "description": product.get("description", ""),
                "man_name": product.get("artist", ""),
                "category_name": "Discogs Marketplace",
                "url": product.get("url", ""),
                "images": [str(url) for url in images],
                "features": features,
                "variants": [],
                "description_extra1": format_text,
                "description_extra2": extra.get("genre", "") if isinstance(extra, dict) else "",
                "description_extra3": extra.get("style", "") if isinstance(extra, dict) else "",
                "extra_field_53325": format_text,
            })
            if release_id not in (None, ""):
                entry["extra_field_32141"] = str(release_id)
            products_dict[pid] = entry

        log.info(
            "ProductsData proxied",
            extra={
                "action": "ProductsData",
                "discogs_action": "get_listing",
                "requested": len(selected),
                "rows": len(products_dict),
            },
        )
        return products_dict

    def orders_list(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle OrdersList by proxying Discogs orders.

        Args:
            payload: BaseLinker-style request payload.

        Returns:
            BaseLinker-compatible OrdersList response.
        """
        page = max(1, to_int(payload.get("page"), 1))
        per_page = max(1, min(to_int(payload.get("per_page"), 50), 100))
        status = payload.get("status") or payload.get("order_status")
        only_paid = _coerce_bool(payload.get("only_paid") or payload.get("onlyPaid"))
        if only_paid:
            status = PAYMENT_RECEIVED_STATUS
        cutoff = iso_to_epoch(payload.get("date_from"))
        created_after_cutoff = max(0, cutoff - _ORDER_UPDATE_LOOKBACK_SECONDS)
        created_after = payload.get("created_after") or (
            _discogs_created_filter_from_epoch(created_after_cutoff)
            if created_after_cutoff
            else None
        )
        created_before = payload.get("created_before")

        def _filtered_pairs_for_orders(discogs_orders: List[Dict[str, Any]]) -> List[tuple[Dict[str, Any], Dict[str, Any]]]:
            pairs: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
            for discogs_order in discogs_orders:
                raw_bl_order = discogs_order_to_bl_order(discogs_order)
                if cutoff and _order_changed_epoch(raw_bl_order) < cutoff:
                    continue
                if only_paid and not is_payment_received_order(raw_bl_order):
                    continue

                if not cutoff or _order_created_epoch(raw_bl_order) >= cutoff:
                    enriched_order = self._enrich_order_release_details(discogs_order)
                    bl_order = discogs_order_to_bl_order(enriched_order)
                    pairs.append((enriched_order, bl_order))
                else:
                    pairs.append((discogs_order, raw_bl_order))
            return pairs

        discogs_response = discogs_client.list_orders(
            status=status,
            page=page,
            per_page=per_page,
            created_after=created_after,
            created_before=created_before,
            sort="last_activity" if cutoff else None,
            sort_order="desc" if cutoff else None,
        )
        discogs_orders = discogs_response.get("orders") or discogs_response.get("data") or []
        filtered_pairs = _filtered_pairs_for_orders(discogs_orders)
        self._cleanup_sold_order_listings([discogs_order for discogs_order, _ in filtered_pairs])
        bl_orders = [bl_order for _, bl_order in filtered_pairs]
        pagination = discogs_response.get("pagination") or {}
        result = {
            "status": "OK",
            "page": pagination.get("page", page),
            "per_page": pagination.get("per_page", per_page),
            "pages": pagination.get("pages") or 1,
            "counter": pagination.get("items", len(bl_orders)),
            "orders": bl_orders,
        }
        log.info(
            "OrdersList proxied",
            extra={
                "action": "OrdersList",
                "discogs_action": "list_orders",
                "rows": len(bl_orders),
                "status_filter": status,
            },
        )
        return result

    def orders_get(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle OrdersGet by fetching a Discogs order.

        Args:
            payload: BaseLinker-style request payload.

        Returns:
            BaseLinker-compatible OrdersGet response.

        Raises:
            ValueError: If order_id is missing.
        """
        order_id = payload.get("order_id") or payload.get("id")
        if not order_id:
            raise ValueError("order_id is required")
        discogs_order = discogs_client.get_order(order_id)
        enriched_order = self._enrich_order_release_details(discogs_order)
        self._cleanup_sold_order_listings([enriched_order])
        order = discogs_order_to_bl_order(enriched_order)
        log.info(
            "OrdersGet proxied",
            extra={
                "action": "OrdersGet",
                "discogs_action": "get_order",
                "order_id": str(order_id),
            },
        )
        return {"status": "OK", "order": order}

    def orders_status(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle OrdersStatus by updating Discogs order status/message.

        Args:
            payload: BaseLinker-style request payload.

        Returns:
            BaseLinker-compatible OrdersStatus response.

        Raises:
            ValueError: If order_id is missing.
        """
        order_id = payload.get("order_id") or payload.get("id")
        if not order_id:
            raise ValueError("order_id is required")
        updates: Dict[str, Any] = {}
        # Discogs order edit accepts status, shipping amount, and tracking object.
        for key in (
            "status",
            "shipping",
        ):
            value = payload.get(key)
            if value not in (None, ""):
                updates[key] = value
        tracking = discogs_tracking_payload(
            payload.get("tracking_number"),
            carrier=payload.get("shipping_provider")
            or payload.get("delivery_method_name")
            or payload.get("carrier"),
        )
        if tracking:
            updates["tracking"] = tracking
        edit_result: Optional[Dict[str, Any]] = None
        if updates:
            edit_result = discogs_client.edit_order(order_id, **updates)
        message_body = payload.get("message") or payload.get("comment")
        tracking_number = payload.get("tracking_number")
        if tracking_number and not message_body:
            parts = [f"Tracking: {tracking_number}"]
            if payload.get("shipping_provider"):
                parts.append(f"Carrier: {payload['shipping_provider']}")
            if payload.get("tracking_url"):
                parts.append(f"Tracking URL: {payload['tracking_url']}")
            message_body = " | ".join(parts)
        message_result: Optional[Dict[str, Any]] = None
        if message_body:
            message_status = payload.get("message_status") or payload.get("status")
            message_result = discogs_client.add_order_message(
                order_id, message_body, status=message_status
            )
        log.info(
            "OrdersStatus proxied",
            extra={
                "action": "OrdersStatus",
                "discogs_action": "edit_order",
                "order_id": str(order_id),
                "fields": list(updates.keys()),
                "message": bool(message_body),
            },
        )
        return {
            "status": "OK",
            "order_id": str(order_id),
            "updated_fields": list(updates.keys()),
            "message_sent": bool(message_body),
            "discogs_edit": edit_result,
            "discogs_message": message_result,
        }


__all__ = ["BaseLinkerConnector"]
