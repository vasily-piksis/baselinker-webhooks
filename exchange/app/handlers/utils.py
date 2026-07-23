"""Utility functions for handlers.

This module provides utility functions used by various handlers:
- Data transformation utilities
- Formatting utilities
- Response utilities
- Data collection utilities
- Entry builders
- Request utilities
- Product utilities
"""

from __future__ import annotations

import json
import logging
import math
import re
from typing import Any, Dict, List, Optional, Sequence, cast

from fastapi import Request

from exchange.delivery_methods import delivery_method_entries
from exchange.logging_utils import get_correlation_id
from exchange.order_statuses import DISCOGS_STATUS_ID_TO_NAME

log = logging.getLogger("exchange.app.handlers.utils")

# Default status names for orders
DEFAULT_STATUS_NAMES = dict(DISCOGS_STATUS_ID_TO_NAME)

# Default delivery methods
DEFAULT_DELIVERY_METHODS = delivery_method_entries()

# Default payment methods
DEFAULT_PAYMENT_METHODS = {
    "1": "Cash on Delivery",
    "2": "Bank transfer",
    "3": "PayPal",
}


def with_cid(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Add correlation ID to a payload.

    Args:
        payload: The payload dictionary

    Returns:
        The payload with correlation_id added
    """
    payload.setdefault("correlation_id", get_correlation_id())
    return payload


def to_int(value: Any, default: int = 0) -> int:
    """Convert a value to integer.

    Args:
        value: The value to convert
        default: Default value if conversion fails

    Returns:
        Integer value or default
    """
    try:
        if value in (None, "", [], {}):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    """Convert a value to float.

    Args:
        value: The value to convert
        default: Default value if conversion fails

    Returns:
        Float value or default
    """
    try:
        if value in (None, "", [], {}):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def format_decimal(value: Any) -> str:
    """Format a value as a decimal string with 2 decimal places.

    Args:
        value: The value to format

    Returns:
        Formatted decimal string
    """
    return f"{to_float(value):.2f}"


def ensure_list(value: Any) -> List[Any]:
    """Ensure a value is a list.

    Args:
        value: The value to convert

    Returns:
        List containing the value(s)
    """
    if isinstance(value, list):
        return [item for item in value]
    if isinstance(value, tuple):
        return list(value)
    if value in (None, "", []):
        return []
    return [value]


def parse_ids(value: Any) -> List[str]:
    """Parse a value into a list of IDs.

    Args:
        value: The value to parse (string, list, tuple, set, or comma-separated)

    Returns:
        List of non-empty ID strings
    """
    if value in (None, "", [], {}):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(value).strip()]


def coerce_bool(value: Any) -> bool:
    """Coerce a value to boolean.

    Args:
        value: The value to coerce

    Returns:
        Boolean value
    """
    if value in (None, "", [], {}, 0, "0", "false", "False", "FALSE", "no", "No", "NO"):
        return False
    return bool(value)


def paginate(items: Sequence[Any], page: int, per_page: int) -> tuple[List[Any], int, int]:
    """Paginate a sequence of items.

    Args:
        items: The items to paginate
        page: Page number (1-indexed)
        per_page: Items per page

    Returns:
        Tuple of (paged_items, total_count, total_pages)
    """
    total = len(items)
    pages = max(1, math.ceil(total / per_page)) if per_page else 1
    start = (page - 1) * per_page
    end = start + per_page
    return list(items[start:end]), total, pages


DEFAULT_WAREHOUSE_ID = "0"


def _extract_product_id(entry: Dict[str, Any]) -> str:
    """Extract a product ID from a catalog entry.

    Args:
        entry: Product/catalog entry dict

    Returns:
        Stripped product ID string (may be empty)
    """
    return str(
        entry.get("id") or entry.get("product_id") or entry.get("sku") or ""
    ).strip()


def _collect_field(
    entries: Sequence[Dict[str, Any]],
    field: str,
    transform: Any,
) -> Dict[str, Dict[str, str]]:
    """Collect a single field from entries into a warehouse-keyed dictionary.

    Args:
        entries: List of product entries
        field: Field name to extract from each entry
        transform: Callable that converts the raw value to a string

    Returns:
        Dictionary mapping product_id to {warehouse_id: value}
    """
    result: Dict[str, Dict[str, str]] = {}
    for entry in entries:
        product_id = _extract_product_id(entry)
        if not product_id:
            continue
        result[product_id] = {DEFAULT_WAREHOUSE_ID: transform(entry.get(field))}
    return result


def collect_prices(entries: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """Collect prices from entries into a dictionary.

    Args:
        entries: List of product entries

    Returns:
        Dictionary mapping product_id to warehouse-keyed price dict
    """
    return _collect_field(entries, "price", format_decimal)


def collect_quantities(entries: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """Collect quantities from entries into a dictionary.

    Args:
        entries: List of product entries

    Returns:
        Dictionary mapping product_id to warehouse-keyed quantity dict
    """
    return _collect_field(entries, "quantity", lambda v: str(to_int(v)))


def build_status_entries() -> Dict[str, str]:
    """Build status entries dictionary.

    Returns:
        Dictionary mapping status id to status name
    """
    return {str(ident): name for ident, name in DEFAULT_STATUS_NAMES.items()}


def build_delivery_entries() -> Dict[str, str]:
    """Build delivery method entries dictionary.

    Returns:
        Dictionary mapping delivery id to delivery name
    """
    return {str(ident): name for ident, name in DEFAULT_DELIVERY_METHODS.items()}


def build_payment_entries() -> Dict[str, str]:
    """Build payment method entries dictionary.

    Returns:
        Dictionary mapping payment id to payment name
    """
    return {str(ident): name for ident, name in DEFAULT_PAYMENT_METHODS.items()}


async def read_request_body(req: Request) -> Dict[str, Any]:
    """Read and parse the request body.

    Handles both JSON and form-encoded data.

    Args:
        req: The FastAPI request

    Returns:
        Parsed request body as dictionary

    Raises:
        BaseDiscogsError: If JSON body is not an object
    """
    from exchange.errors import raise_error
    from urllib.parse import parse_qs

    try:
        raw = await req.body()
    except Exception:
        raw = b""

    if not raw:
        try:
            form = await req.form()
        except Exception:
            return {}
        return merge_parameters(dict(form))

    # Try JSON first
    try:
        decoded = raw.decode("utf-8-sig")
        data = json.loads(decoded)
        if not isinstance(data, dict):
            raise_error("invalid_body", "JSON body must be an object")
        return merge_parameters(data)
    except json.JSONDecodeError:
        pass

    # Try form-urlencoded parsing directly from raw body
    # (req.form() may not work after req.body() consumed the stream)
    try:
        decoded = raw.decode("utf-8")
        parsed = parse_qs(decoded, keep_blank_values=True)
        # Convert lists to single values where appropriate
        flat_dict: Dict[str, Any] = {}
        for key, values in parsed.items():
            if len(values) == 1:
                flat_dict[key] = values[0]
            else:
                flat_dict[key] = values
        return merge_parameters(flat_dict)
    except Exception:
        pass

    # Fallback to req.form()
    try:
        form = await req.form()
        return merge_parameters(dict(form))
    except Exception:
        return {}


def _parse_php_array_notation(flat_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Parse PHP-style array notation into nested Python structures.

    Converts keys like 'products[0][product_id]' into nested dicts/lists.
    Also handles the case where 'products' is a single string containing
    URL-encoded array data (as BaseLinker sometimes sends).

    Args:
        flat_dict: Dictionary with PHP-style array keys

    Returns:
        Dictionary with nested structures
    """
    from urllib.parse import parse_qs
    
    # Check if 'products' is a string containing URL-encoded data
    products_val = flat_dict.get("products")
    if isinstance(products_val, str) and "[" in products_val and "=" in products_val:
        # BaseLinker tester sends products as a single string like:
        # "products[0][product_id]=123&products[0][quantity]=10"
        # It may also use &amp; instead of & (HTML entity from textarea)
        try:
            # Clean up the string
            cleaned = products_val.strip()
            cleaned = cleaned.replace("&amp;", "&")  # HTML entity to &
            cleaned = cleaned.replace("\r\n", "").replace("\n", "")  # Remove newlines
            
            parsed = parse_qs(cleaned, keep_blank_values=True)
            # Merge parsed products into flat_dict
            expanded_dict = dict(flat_dict)
            del expanded_dict["products"]
            for key, values in parsed.items():
                # Strip whitespace from values
                expanded_dict[key] = values[0].strip() if len(values) == 1 else [v.strip() for v in values]
            flat_dict = expanded_dict
        except Exception:
            pass
    
    result: Dict[str, Any] = {}
    array_pattern = re.compile(r'^([^\[]+)(\[.+\])$')

    for key, value in flat_dict.items():
        match = array_pattern.match(key)
        if not match:
            # Regular key, just copy
            result[key] = value
            continue

        base_key = match.group(1)
        indices_str = match.group(2)

        # Parse indices like [0][product_id] into ['0', 'product_id']
        indices = re.findall(r'\[([^\]]*)\]', indices_str)

        # Build nested structure
        if base_key not in result:
            # Determine if first index is numeric (list) or not (dict)
            if indices and indices[0].isdigit():
                result[base_key] = []
            else:
                result[base_key] = {}

        current = result
        path = [base_key] + indices

        for i, idx in enumerate(path[:-1]):
            next_idx = path[i + 1]
            is_next_numeric = next_idx.isdigit()

            if isinstance(current, list):
                idx_int = int(idx)
                # Extend list if needed
                while len(current) <= idx_int:
                    current.append({} if not is_next_numeric else [])
                current = current[idx_int]
            else:
                if idx not in current:
                    current[idx] = [] if is_next_numeric else {}
                current = current[idx]

        # Set the final value
        final_idx = path[-1]
        if isinstance(current, list):
            idx_int = int(final_idx)
            while len(current) <= idx_int:
                current.append(None)
            current[idx_int] = value
        else:
            current[final_idx] = value

    return result


def merge_parameters(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Merge nested parameters into the payload.

    Args:
        payload: The payload dictionary

    Returns:
        Payload with merged parameters
    """
    # First, parse PHP-style array notation
    payload = _parse_php_array_notation(payload)

    params = payload.get("parameters")
    parsed: Optional[Dict[str, Any]] = None
    if isinstance(params, str):
        try:
            parsed = json.loads(params)
        except Exception:
            parsed = None
    elif isinstance(params, dict):
        parsed = params
    if isinstance(parsed, dict):
        payload["parameters"] = parsed
        for key, value in parsed.items():
            payload.setdefault(key, value)
    return payload


def product_summary(product: Dict[str, Any]) -> Dict[str, Any]:
    """Create a product summary from a product dictionary.

    Args:
        product: The product dictionary

    Returns:
        Product summary dictionary
    """
    raw_value = product.get("raw")
    raw: Dict[str, Any] = cast(Dict[str, Any], raw_value) if isinstance(raw_value, dict) else {}
    product_id = str(
        product.get("id")
        or raw.get("id")
        or raw.get("product_id")
        or product.get("product_id")
        or product.get("sku")
        or raw.get("sku")
        or ""
    ).strip()
    sku = str(product.get("sku") or raw.get("sku") or product_id or "").strip()
    if not product_id:
        product_id = sku
    if not product_id:
        return {}
    name = str(product.get("name") or raw.get("name") or raw.get("title") or product_id).strip()
    currency = str(product.get("currency") or raw.get("currency") or "USD").strip() or "USD"
    summary: Dict[str, Any] = {
        "id": product_id,
        "name": name,
        "quantity": to_int(product.get("quantity") or raw.get("quantity")),
        "price": to_float(product.get("price") or raw.get("price") or raw.get("price_brutto")),
        "price_brutto": to_float(
            product.get("price_brutto")
            or raw.get("price_brutto")
            or product.get("price")
            or raw.get("price")
        ),
        "currency": currency,
    }
    if sku:
        summary["sku"] = sku
    ean = product.get("ean") or raw.get("ean")
    if ean not in (None, ""):
        summary["ean"] = str(ean)
    location = product.get("location") or raw.get("location")
    if location not in (None, ""):
        summary["location"] = str(location)
    return summary


def product_detail(product: Dict[str, Any]) -> Dict[str, Any]:
    """Create a detailed product representation from a product dictionary.

    Args:
        product: The product dictionary

    Returns:
        Detailed product dictionary
    """
    summary = product_summary(product)
    if not summary:
        return {}
    raw_value = product.get("raw")
    raw: Dict[str, Any] = cast(Dict[str, Any], raw_value) if isinstance(raw_value, dict) else {}
    detail: Dict[str, Any] = {
        **summary,
        "price": summary.get("price", 0.0),
        "price_brutto": summary.get("price_brutto", summary.get("price", 0.0)),
        "quantity": summary.get("quantity", 0),
        "currency": summary.get("currency", "USD"),
    }
    for key in (
        "description",
        "description_extra1",
        "description_extra2",
        "description_extra3",
        "description_extra4",
        "man_name",
        "man_image",
        "category_id",
        "category_name",
        "url",
        "location",
    ):
        value = product.get(key)
        if value in (None, "") and isinstance(raw, dict):
            value = raw.get(key)
        if value not in (None, ""):
            detail[key] = value
    detail["tax"] = to_int(product.get("tax") or raw.get("tax"))
    for dim in ("weight", "height", "length", "width"):
        value = product.get(dim) if product.get(dim) not in (None, "") else raw.get(dim)
        if value not in (None, ""):
            detail[dim] = to_float(value)
    images = product.get("images") or raw.get("images")
    detail["images"] = [str(url) for url in ensure_list(images) if str(url).strip()]
    features = product.get("features") or raw.get("features")
    feature_list: List[List[str]] = []
    for entry in ensure_list(features):
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            name, value = entry[0], entry[1]
            if name not in (None, "") and value not in (None, ""):
                feature_list.append([str(name), str(value)])
        elif isinstance(entry, dict):
            name = entry.get("name") or entry.get("label")
            value = entry.get("value") or entry.get("data")
            if name not in (None, "") and value not in (None, ""):
                feature_list.append([str(name), str(value)])
    detail["features"] = feature_list
    variants = product.get("variants") or raw.get("variants") or {}
    variant_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(variants, dict):
        for key, value in variants.items():
            if isinstance(value, dict):
                variant_map[str(key)] = value
    elif isinstance(variants, list):
        for index, value in enumerate(variants, start=1):
            if isinstance(value, dict):
                ident = str(
                    value.get("id")
                    or value.get("variant_id")
                    or value.get("sku")
                    or f"variant-{index}"
                )
                variant_map[ident] = value
    if variant_map:
        detail["variants"] = variant_map
    delivery_time = product.get("delivery_time") or raw.get("delivery_time")
    if delivery_time not in (None, ""):
        detail["delivery_time"] = to_int(delivery_time)
    for idx in range(1, 5):
        key = f"extra_field_{idx}"
        value = product.get(key) if product.get(key) not in (None, "") else raw.get(key)
        if value not in (None, ""):
            detail[key] = value
    return detail


def idempotency_hint(body: Dict[str, Any]) -> Optional[str]:
    """Extract idempotency hint from request body.

    Args:
        body: The request body dictionary

    Returns:
        Idempotency hint string or None
    """
    for key in ("idempotency_key", "idempotency_hint", "request_id"):
        value = body.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return None
