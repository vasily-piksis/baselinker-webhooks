# exchange/processors/discogs_csv_processor.py
"""Discogs CSV processor for generating inventory CSV data.

This module provides functions for:
- Converting BaseLinker events to exchange rows
- Converting exchange rows to Discogs inventory rows
- Persisting Discogs CSV row data in the database
- Generating CSV text on-demand from stored rows
"""

from __future__ import annotations

import csv
import html
import io
import json
import logging
from typing import Any, Dict, Iterable, List
from uuid import UUID

from exchange.utils.mapping import exchange_row_to_discogs_listing
from exchange.master import hydrate_exchange_row
from database.repositories.discogs_csv_repository import DiscogsCsvRepository
from database.session import get_session
from exchange.errors import NotFoundError, ValidationError

log = logging.getLogger("exchange.processors.discogs_csv")


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
        # BaseLinker sometimes sends products as a numerically-keyed dict:
        # {"0": {"product_id": "123", ...}, "1": {"product_id": "456", ...}}
        # Detect this pattern and extract the values as a list.
        if value and all(k.isdigit() for k in value.keys()):
            sorted_items = sorted(value.items(), key=lambda x: int(x[0]))
            return [v for _, v in sorted_items if isinstance(v, dict)]
        return [value]
    return []


def _extract_feature(product: Dict[str, Any], body: Dict[str, Any], feature_name: str) -> str:
    """Extract a feature value from BaseLinker features array.
    
    BaseLinker sends features as an array of [name, value] pairs:
    features: [["Condition", "Mint (M)"], ["Sleeve Condition", "Very Good Plus (VG+)"]]
    
    Args:
        product: Product dictionary
        body: Request body dictionary
        feature_name: Name of the feature to extract (case-insensitive)
        
    Returns:
        Feature value or empty string if not found
    """
    feature_name_lower = feature_name.lower()
    
    for source in (product, body):
        features = source.get("features")
        if isinstance(features, list):
            for feature in features:
                if isinstance(feature, (list, tuple)) and len(feature) >= 2:
                    if str(feature[0]).lower() == feature_name_lower:
                        return str(feature[1])
                elif isinstance(feature, dict):
                    # Also support dict format: {"name": "Condition", "value": "Mint (M)"}
                    if str(feature.get("name", "")).lower() == feature_name_lower:
                        return str(feature.get("value", ""))
    return ""


def _extract_extra_field(product: Dict[str, Any], body: Dict[str, Any], *field_names: str) -> str:
    """Extract a value from BaseLinker extra fields.
    
    BaseLinker sends extra fields as extra_field_123 where 123 is the field ID.
    This function searches for fields by name pattern (case-insensitive).
    
    Args:
        product: Product dictionary
        body: Request body dictionary
        field_names: Field name patterns to search for (case-insensitive)
        
    Returns:
        Field value or empty string if not found
    """
    field_names_lower = [name.lower() for name in field_names]
    
    for source in (product, body):
        for key, value in source.items():
            if key is None:
                continue
            # Check extra_field_* keys
            if key.startswith("extra_field_") and value not in (None, ""):
                return str(value)
            # Check keys matching field names directly
            if key.lower() in field_names_lower and value not in (None, ""):
                return str(value)
    return ""


def _extract_extra_field_by_pattern(
    product: Dict[str, Any], body: Dict[str, Any], *patterns: str
) -> str:
    """Extract a value from BaseLinker extra fields by name pattern.
    
    Searches all keys in product/body for patterns (case-insensitive substring match).
    
    Args:
        product: Product dictionary
        body: Request body dictionary  
        patterns: Patterns to search for in key names (case-insensitive)
        
    Returns:
        Field value or empty string if not found
    """
    patterns_lower = [p.lower() for p in patterns]
    
    for source in (product, body):
        for key, value in source.items():
            if key is None:
                continue
            key_lower = key.lower()
            for pattern in patterns_lower:
                if pattern in key_lower and value not in (None, ""):
                    return str(value)
    return ""


def _product_to_exchange_row(product: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    def pick(*keys: str, default: Any = "") -> Any:
        for key in keys:
            value = product.get(key)
            if value not in (None, ""):
                return value
            value = body.get(key)
            if value not in (None, ""):
                return value
        return default

    # Extract condition from multiple sources (in priority order):
    # 1. Direct fields: condition, media_condition
    # 2. Extra fields containing "media_condition" or "condition" in name
    # 3. Features array: ["Condition", "Mint (M)"] or ["Media condition", "Mint (M)"]
    condition = (
        pick("condition", "media_condition") 
        or _extract_extra_field_by_pattern(product, body, "media_condition", "media condition")
        or _extract_feature(product, body, "condition")
        or _extract_feature(product, body, "media_condition")
        or _extract_feature(product, body, "media condition")
    )
    
    # Extract sleeve_condition from multiple sources:
    # 1. Direct fields: sleeve_condition, sleeve
    # 2. Extra fields containing "sleeve" in name
    # 3. Features array
    sleeve_condition = (
        pick("sleeve_condition", "sleeve") 
        or _extract_extra_field_by_pattern(product, body, "sleeve_condition", "sleeve condition", "sleeve")
        or _extract_feature(product, body, "sleeve_condition")
        or _extract_feature(product, body, "sleeve condition")
        or _extract_feature(product, body, "sleeve")
    )
    
    # Extract release_id from extra fields or direct fields
    # BaseLinker extra field "Release ID" contains the Discogs release ID
    release_id = (
        pick("release_id", "discogs_release_id")
        or _extract_extra_field_by_pattern(product, body, "release_id", "release id", "discogs_release")
    )
    
    # Extract format_quantity from extra fields
    format_quantity = (
        pick("format_quantity")
        or _extract_extra_field_by_pattern(product, body, "format_quantity", "format quantity")
    )
    
    # Extract allow_offers from extra fields
    allow_offers = (
        pick("allow_offers", "accept_offers")
        or _extract_extra_field_by_pattern(product, body, "accept_offer", "allow_offer", "offers")
    )
    
    # Extract weight - BaseLinker stores in kg, we'll convert to grams for Discogs later
    weight_kg = pick("weight", default=None)

    return {
        "external_sku": pick("sku", "external_id"),
        "title": pick("name", "title"),
        "artist": pick("artist"),
        "format": pick("format") or release_id,  # format can contain release_id
        "release_id": release_id,
        "condition": condition,
        "sleeve_condition": sleeve_condition,
        "price": pick("price_brutto", "price", "price_net", default=0),
        "currency": pick("currency", default=body.get("currency", "USD") or "USD"),
        "quantity": pick("quantity", "stock", default=1),
        "location": pick("location", default=body.get("location", "")),
        "notes": pick("notes", "comment", "comments", "description", default=body.get("notes", "")),
        "format_quantity": format_quantity,
        "allow_offers": allow_offers,
        "weight_kg": weight_kg,  # Weight in kg from BaseLinker
        "created_at": body.get("created_at", ""),
    }


def _extract_indexed_products(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract products from BaseLinker indexed fields (product_id1, value1, etc.).

    BaseLinker sends product data in two formats simultaneously:
    - ``products`` JSON field (may be incomplete)
    - Indexed fields: product_id1, variant_id1, value1, product_id2, ...

    The indexed fields are the authoritative source for the full product list.
    """
    products: Dict[int, Dict[str, Any]] = {}
    action = (body.get("action") or "").lower()
    is_price = "price" in action

    for key, val in body.items():
        if not key.startswith("product_id") or key == "product_id":
            continue
        suffix = key[len("product_id"):]
        if not suffix.isdigit():
            continue
        idx = int(suffix)
        pid = str(val).strip()
        if not pid:
            continue
        entry: Dict[str, Any] = {"product_id": pid}
        vid = body.get(f"variant_id{suffix}", "")
        if vid:
            entry["variant_id"] = str(vid).strip()
        raw_value = body.get(f"value{suffix}", "")
        if raw_value != "":
            if is_price:
                entry["price"] = str(raw_value)
            else:
                entry["quantity"] = str(raw_value)
                entry["operation"] = "set"
        products[idx] = entry
    return [products[k] for k in sorted(products)]


def exchange_rows_from_baselinker_event(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a BaseLinker event to exchange rows.

    Args:
        body: The BaseLinker event payload

    Returns:
        List of exchange row dictionaries
    """
    products = _normalize_sequence(body.get("rows") or body.get("products") or body.get("items"))
    indexed = _extract_indexed_products(body)
    if indexed:
        # Merge: use indexed products as the authoritative source, but keep
        # any extra fields from the ``products`` JSON if available.
        products_by_pid: Dict[str, Dict[str, Any]] = {}
        for p in products:
            pid = str(p.get("product_id") or "")
            if pid:
                products_by_pid[pid] = p
        merged: List[Dict[str, Any]] = []
        for ip in indexed:
            pid = ip["product_id"]
            base = products_by_pid.get(pid, {}).copy()
            base.update(ip)
            merged.append(base)
        products = merged
    if not products and body:
        products = [_product_to_exchange_row(body, body)]
    exchange_rows: List[Dict[str, Any]] = []
    for product in products:
        exchange_row = _product_to_exchange_row(product, body)
        exchange_row = hydrate_exchange_row(exchange_row)
        exchange_rows.append(exchange_row)
    return exchange_rows


def inventory_rows_from_baselinker_event(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a BaseLinker event to Discogs inventory rows.

    Args:
        body: The BaseLinker event payload

    Returns:
        List of Discogs inventory row dictionaries
    """
    # Webhook updates only emit rows for the incoming payload, ensuring per-listing changes.
    rows: List[Dict[str, Any]] = []
    action = (body.get("action") or "").replace(".", "").lower()
    # Handle both singular and plural forms (ProductQuantityUpdate and ProductsQuantityUpdate)
    is_update = action in {
        "productquantityupdate", "productsquantityupdate",
        "productpriceupdate", "productspriceupdate",
        "productquantity", "productsquantity",
        "productdelete", "productsdelete",
    }

    # Get the raw products list so we can extract product_id for webhook updates.
    # For webhooks like ProductsQuantityUpdate, BaseLinker sends product_id inside
    # each product element — this IS the Discogs listing_id (from the shop link).
    # BaseLinker also sends indexed fields (product_id1, value1, etc.) which may
    # contain more products than the ``products`` JSON field.
    raw_products = _normalize_sequence(
        body.get("rows") or body.get("products") or body.get("items")
    )
    indexed_products = _extract_indexed_products(body)
    if indexed_products:
        raw_by_pid = {str(p.get("product_id", "")): p for p in raw_products}
        merged_raw: List[Dict[str, Any]] = []
        for ip in indexed_products:
            pid = ip["product_id"]
            base = raw_by_pid.get(pid, {}).copy()
            base.update(ip)
            merged_raw.append(base)
        raw_products = merged_raw

    exchange_rows = exchange_rows_from_baselinker_event(body)
    for idx, exchange_row in enumerate(exchange_rows):
        listing = exchange_row_to_discogs_listing(exchange_row)
        release_id = listing.pop("release_id", None)

        # For webhook updates, product_id from the raw product element is the
        # Discogs listing_id (BaseLinker stores it in the shop link).
        raw_pid = ""
        if idx < len(raw_products):
            raw_pid = str(raw_products[idx].get("product_id") or "")
        listing_id = raw_pid or exchange_row.get("external_sku") or body.get("product_id")

        if is_update:
            # For updates/deletes, we need listing_id, not release_id
            if not listing_id:
                continue
            csv_row: Dict[str, Any] = {"listing_id": str(listing_id)}
        else:
            # For adds, we need release_id; external_id (SKU) is optional
            if not release_id:
                continue

            external_id = listing.get("external_id")
            if not external_id:
                log.warning(
                    "SKU (external_id) missing for ProductAdd - release_id=%s. "
                    "Listing will be created without external_id.",
                    release_id,
                )

            csv_row = {"release_id": release_id}

        media_condition = listing.get("condition") or exchange_row.get("condition") or "Mint (M)"
        if (
            isinstance(media_condition, str)
            and "(" not in media_condition
            and media_condition.lower() == "mint"
        ):
            media_condition = "Mint (M)"
        csv_row["media_condition"] = media_condition
        
        # Add sleeve_condition - default to Mint (M) if not provided
        sleeve_condition = listing.get("sleeve_condition") or exchange_row.get("sleeve_condition") or "Mint (M)"
        csv_row["sleeve_condition"] = sleeve_condition
        
        # Always include external_id (SKU) so Discogs stays in sync with BaseLinker
        keys_to_copy = ["price", "comments", "location", "external_id"]
        if not is_update:
            keys_to_copy.append("status")
            keys_to_copy.append("quantity")
        for key in keys_to_copy:
            value = listing.get(key)
            if value not in (None, "", []):
                csv_row[key] = value
        
        # For updates, handle quantity and status based on quantity value:
        # - quantity == 0: set status to "Draft" (hide from sale)
        # - quantity > 0: set status to "For Sale" (show listing)
        if is_update:
            quantity_raw = listing.get("quantity") or exchange_row.get("quantity")
            quantity_int = 0
            if quantity_raw is not None:
                try:
                    quantity_int = int(quantity_raw)
                except (TypeError, ValueError):
                    quantity_int = 0
            
            # Always include quantity in CSV for updates
            csv_row["quantity"] = quantity_int
            
            if quantity_int == 0:
                csv_row["status"] = "Draft"
            else:
                csv_row["status"] = "For Sale"
        
        # Default allow_offers to N; only set Y if explicitly truthy
        allow_offers_val = listing.get("allow_offers")
        csv_row["allow_offers"] = "Y" if allow_offers_val is True else "N"

        currency = exchange_row.get("currency") or body.get("currency")
        if currency:
            csv_row["currency"] = currency
        rows.append(csv_row)
    return rows


def _fieldnames_from_rows(rows: Iterable[Dict[str, Any]]) -> List[str]:
    rows_list = list(rows)
    if not rows_list:
        return []
    fieldnames = list(rows_list[0].keys())
    for row in rows_list[1:]:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    return fieldnames


def _csv_text_from_rows(rows: Iterable[Dict[str, Any]]) -> str:
    rows_list = list(rows)
    if not rows_list:
        raise ValidationError("rows must not be empty", error_code="validation_error")
    fieldnames = _fieldnames_from_rows(rows_list)
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows_list:
        writer.writerow(row)
    return buf.getvalue()


def write_discogs_csv_file(
    action: str,
    rows: List[Dict[str, Any]],
    *,
    idempotency_token: str | None,
) -> str:
    """Store Discogs CSV rows in the database.

    Args:
        action: The action name
        rows: List of inventory row dictionaries
        idempotency_token: Optional idempotency token

    Returns:
        Record id for the stored CSV rows

    Raises:
        ValidationError: If rows is empty
    """
    if not rows:
        raise ValidationError("rows must not be empty", error_code="validation_error")
    with get_session() as session:
        repo = DiscogsCsvRepository(session)
        if idempotency_token:
            existing = repo.get_by_idempotency_token(idempotency_token)
            if existing:
                return str(existing.record_id)
        record = repo.create_csv_record(
            action=action.replace(".", "").lower(),
            rows=rows,
            idempotency_token=idempotency_token,
        )
        session.commit()
        return str(record.record_id)


def generate_discogs_csv_text(rows: Iterable[Dict[str, Any]]) -> str:
    """Generate CSV text from row dictionaries (no persistence).

    Args:
        rows: Iterable of Discogs inventory row dictionaries.

    Returns:
        CSV content as a string.
    """
    return _csv_text_from_rows(rows)


def generate_discogs_csv_text_for_record(record_id: str | UUID) -> str:
    """Generate CSV text for a stored Discogs CSV record.

    Args:
        record_id: CSV record identifier.

    Returns:
        CSV content as a string.

    Raises:
        NotFoundError: If the CSV record does not exist.
    """
    record_uuid = UUID(str(record_id))
    with get_session() as session:
        repo = DiscogsCsvRepository(session)
        record = repo.get_by_record_id(record_uuid)
        if not record:
            raise NotFoundError(
                f"Discogs CSV record not found: {record_id}",
                error_code="discogs_csv_not_found",
            )
        rows = record.rows or []
        return _csv_text_from_rows(rows)


__all__ = [
    "exchange_rows_from_baselinker_event",
    "inventory_rows_from_baselinker_event",
    "generate_discogs_csv_text",
    "generate_discogs_csv_text_for_record",
    "write_discogs_csv_file",
]
