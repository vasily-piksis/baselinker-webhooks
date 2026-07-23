"""Translate between Discogs and BaseLinker order payloads."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Tuple

from exchange.order_statuses import (
    discogs_tracking_payload,
    normalize_discogs_order_status,
)
from exchange.settings import BL_SHOP_ID, BL_STATUS_ID_MAP, BL_WAREHOUSE_ID
from exchange.status_map import status_name_for
from exchange.utils import iso_to_epoch, to_float, to_int

_STATUS_MAP = {
    "new order": 100,
    "buyer contacted": 120,
    "invoice sent": 130,
    "payment pending": 150,
    "payment received": 300,
    "in progress": 350,
    "shipped": 400,
    "cancelled": 500,
    "cancelled (non-payment)": 510,
    "invalid": 520,
    "refunded": 530,
}

_INBOX_STATUSES = {"ok", "queued", "processed", "failed", "error"}
PAYMENT_RECEIVED_STATUS = "Payment Received"
_MARKETPLACE_LISTING_URL_RE = re.compile(r"/marketplace/listings/(\d+)")
_RELEASE_URL_RE = re.compile(r"/releases?/(\d+)")


def _discogs_status_from_value(value: Any, *, allow_status_id: bool = False) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered in _INBOX_STATUSES:
        return None
    if not allow_status_id and text.isdigit():
        return None
    return normalize_discogs_order_status(text)


def _discogs_status_from_order(order: Dict[str, Any]) -> Optional[str]:
    candidates = [
        order.get("status_name"),
        order.get("status"),
        order.get("order_status_id"),
        order.get("status_id"),
    ]

    update_type = str(order.get("update_type") or "").strip().lower()
    if update_type == "status":
        status = normalize_discogs_order_status(order.get("update_value"))
        if status:
            return status
    elif update_type == "paid":
        paid_value = str(order.get("update_value") or "").strip().lower()
        if paid_value in {"1", "true", "yes", "paid"}:
            candidates.insert(0, "Payment Received")

    for candidate in candidates:
        status = _discogs_status_from_value(candidate)
        if status:
            return status
    return None


def _tracking_from_order(order: Dict[str, Any]) -> Any:
    tracking = order.get("delivery_package_nr") or order.get("tracking_number")
    update_type = str(order.get("update_type") or "").strip().lower()
    if not tracking and update_type == "delivery_number":
        tracking = order.get("update_value")
    return tracking


def _listing_id_from_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.isdigit():
        return raw
    match = _MARKETPLACE_LISTING_URL_RE.search(raw)
    if match:
        return match.group(1)
    return ""


def extract_discogs_order_item_listing_id(item: Dict[str, Any]) -> str:
    listing = item.get("listing") or {}
    candidate_values = [
        item.get("listing_id"),
        item.get("id"),
        item.get("resource_url"),
        item.get("uri"),
    ]
    if isinstance(listing, dict):
        candidate_values.extend(
            [
                listing.get("listing_id"),
                listing.get("id"),
                listing.get("resource_url"),
                listing.get("uri"),
            ]
        )
    for candidate in candidate_values:
        listing_id = _listing_id_from_value(candidate)
        if listing_id:
            return listing_id
    return ""


def extract_discogs_order_item_release_id(item: Dict[str, Any]) -> str:
    release = item.get("release") or {}
    candidate_values = [
        item.get("release_id"),
        item.get("discogs_release_id"),
        item.get("release_resource_url"),
    ]
    if isinstance(release, dict):
        candidate_values.extend(
            [
                release.get("id"),
                release.get("resource_url"),
                release.get("url"),
                release.get("uri"),
            ]
        )
    for candidate in candidate_values:
        text = _scalar_text(candidate)
        if text.isdigit():
            return text
        match = _RELEASE_URL_RE.search(text)
        if match:
            return match.group(1)
    return ""


def _merge_release_details(
    release: Dict[str, Any],
    release_details: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(release_details)
    for key, value in release.items():
        if value not in (None, "", [], {}):
            merged[key] = value
    return merged


def _merge_listing_details(
    item: Dict[str, Any],
    listing_details: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(listing_details)
    if isinstance(merged.get("release"), dict) and isinstance(item.get("release"), dict):
        merged["release"] = _merge_release_details(item["release"], merged["release"])
    for key, value in item.items():
        if key == "release" and isinstance(value, dict):
            merged.setdefault("release", value)
            continue
        if value not in (None, "", [], {}):
            merged[key] = value
    return merged


def enrich_discogs_order_listing_details(
    order: Dict[str, Any],
    get_listing: Callable[[str], Dict[str, Any]],
    *,
    cache: Optional[Dict[str, Dict[str, Any]]] = None,
    on_error: Optional[Callable[[str, Exception], None]] = None,
) -> Dict[str, Any]:
    listing_cache = cache if cache is not None else {}
    enriched_order = deepcopy(order)
    for item in enriched_order.get("items") or []:
        if not isinstance(item, dict):
            continue
        listing_id = extract_discogs_order_item_listing_id(item)
        if not listing_id:
            continue

        if listing_id not in listing_cache:
            try:
                listing_details = get_listing(listing_id)
                listing_cache[listing_id] = (
                    listing_details if isinstance(listing_details, dict) else {}
                )
            except Exception as exc:
                if on_error:
                    on_error(listing_id, exc)
                listing_cache[listing_id] = {}
        listing_details = listing_cache.get(listing_id) or {}
        if not listing_details:
            continue

        item.update(_merge_listing_details(item, listing_details))
    return enriched_order


def enrich_discogs_order_release_details(
    order: Dict[str, Any],
    get_release: Callable[[str], Dict[str, Any]],
    *,
    on_error: Optional[Callable[[str, Exception], None]] = None,
) -> Dict[str, Any]:
    enriched_order = deepcopy(order)
    for item in enriched_order.get("items") or []:
        if not isinstance(item, dict):
            continue
        release_id = extract_discogs_order_item_release_id(item)
        if not release_id:
            continue

        try:
            release_details = get_release(release_id)
            release_details = release_details if isinstance(release_details, dict) else {}
        except Exception as exc:
            if on_error:
                on_error(release_id, exc)
            release_details = {}
        if not release_details:
            continue

        release = item.get("release") if isinstance(item.get("release"), dict) else {}
        item["release"] = _merge_release_details(release, release_details)
    return enriched_order


def _discogs_order_item_sku(item: Dict[str, Any], listing_id: str) -> str:
    listing = item.get("listing") or {}
    candidate_values = [
        item.get("private_comments"),
        item.get("external_id"),
        item.get("sku"),
    ]
    if isinstance(listing, dict):
        candidate_values.extend(
            [
                listing.get("private_comments"),
                listing.get("external_id"),
                listing.get("sku"),
            ]
        )

    for candidate in candidate_values:
        text = str(candidate or "").strip()
        if text:
            return text
    return listing_id


def _tracking_carrier_from_order(order: Dict[str, Any]) -> Any:
    return (
        order.get("delivery_method_name")
        or order.get("shipping_provider")
        or order.get("delivery_package_module")
        or order.get("carrier")
    )


def _first_text(value: Any, fallback: str = "") -> str:
    if isinstance(value, list):
        for item in value:
            text = _first_text(item)
            if text:
                return text
        return fallback
    if isinstance(value, dict):
        for key in ("name", "value", "description", "title", "text"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return fallback


def _listify_text(items: Any) -> List[str]:
    if isinstance(items, list):
        result: List[str] = []
        for item in items:
            text = _first_text(item)
            if text:
                result.append(text)
        return result
    text = _first_text(items)
    return [text] if text else []


def _joined_text(*values: Any) -> str:
    parts: List[str] = []
    for value in values:
        text = _first_text(value)
        if text and text not in parts:
            parts.append(text)
    return "\n".join(parts)


def _normalized_address_lines(address: str) -> List[str]:
    lines = [line.strip() for line in address.splitlines() if line.strip()]
    filtered: List[str] = []
    for line in lines:
        lowered = line.lower()
        if lowered.startswith("phone:") or lowered.startswith("paypal address:"):
            continue
        filtered.append(line)
    return filtered


def _normalize_postcode(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return "".join(text.split()).upper()


def _phone_from_address(address: Any) -> str:
    if isinstance(address, dict):
        value = address.get("phone") or address.get("phone_number")
        return str(value).strip() if value not in (None, "") else ""
    if not isinstance(address, str):
        return ""
    for line in address.splitlines():
        text = line.strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("phone:"):
            return text.split(":", 1)[1].strip()
    return ""


def _image_urls(images: Any) -> List[str]:
    urls: List[str] = []
    if not images:
        return urls
    if isinstance(images, list):
        for image in images:
            if isinstance(image, str) and image.strip():
                urls.append(image.strip())
            elif isinstance(image, dict):
                for key in ("uri", "uri150", "resource_url"):
                    val = image.get(key)
                    if isinstance(val, str) and val.strip():
                        urls.append(val.strip())
                        break
    elif isinstance(images, dict):
        for key in ("uri", "uri150", "resource_url"):
            val = images.get(key)
            if isinstance(val, str) and val.strip():
                urls.append(val.strip())
                break
    elif isinstance(images, str) and images.strip():
        urls.append(images.strip())
    return urls


def _status_id(name: Optional[str]) -> int:
    if not isinstance(name, str):
        return 0
    normalized = name.lower()
    return BL_STATUS_ID_MAP.get(normalized, _STATUS_MAP.get(normalized, 0))


def _country_code(address: Dict[str, Any]) -> str:
    code = address.get("country_code")
    if isinstance(code, str) and code.strip():
        return code.strip().upper()
    country = str(address.get("country") or "").strip().lower()
    fallback_codes = {
        "united states": "US",
        "usa": "US",
        "united kingdom": "GB",
        "great britain": "GB",
        "lithuania": "LT",
    }
    return fallback_codes.get(country, "")


def _attributes_text(attributes: Any) -> str:
    if isinstance(attributes, str):
        return attributes.strip()
    if not isinstance(attributes, list):
        return ""
    parts: List[str] = []
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        name = str(attribute.get("name") or "").strip()
        value = str(attribute.get("value") or "").strip()
        if name and value:
            parts.append(f"{name}: {value}")
        elif value:
            parts.append(value)
    return "; ".join(parts)


def _attributes_array(attributes: Any) -> List[Dict[str, str]]:
    if isinstance(attributes, str):
        text = attributes.strip()
        return [{"name": "Attributes", "value": text, "price": ""}] if text else []
    if not isinstance(attributes, list):
        return []

    result: List[Dict[str, str]] = []
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        name = str(attribute.get("name") or "").strip()
        value = str(attribute.get("value") or "").strip()
        if not value:
            continue
        result.append(
            {
                "name": name,
                "value": value,
                "price": str(attribute.get("price") or ""),
            }
        )
    return result


def _scalar_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return _first_text(value)


def _append_attribute(attributes: List[Dict[str, str]], name: str, value: Any) -> None:
    text = _scalar_text(value)
    if text:
        attributes.append({"name": name, "value": text})


def _format_item_text(format_item: Any) -> str:
    if isinstance(format_item, str):
        return format_item.strip()
    if not isinstance(format_item, dict):
        return _scalar_text(format_item)

    parts: List[str] = []
    name = _scalar_text(format_item.get("name"))
    if name:
        parts.append(name)
    for description in _listify_text(
        format_item.get("descriptions") or format_item.get("description") or []
    ):
        if description not in parts:
            parts.append(description)
    text = _scalar_text(format_item.get("text"))
    if text and text not in parts:
        parts.append(text)

    formatted = ", ".join(parts)
    qty = _scalar_text(format_item.get("qty") or format_item.get("quantity"))
    if qty and qty not in {"1", "1.0"} and formatted:
        return f"{qty} x {formatted}"
    return formatted


def _format_item_variant_text(format_item: Any) -> str:
    if isinstance(format_item, dict):
        return _scalar_text(format_item.get("text"))
    return ""


def _format_variant_texts(formats: Any) -> List[str]:
    if isinstance(formats, dict):
        formats = [formats]
    if not isinstance(formats, list):
        return []

    texts: List[str] = []
    for item in formats:
        text = _format_item_variant_text(item)
        if text and text not in texts:
            texts.append(text)
    return texts


def _release_format_text(release: Dict[str, Any]) -> str:
    variant_texts = _format_variant_texts(release.get("formats"))
    if variant_texts:
        return "; ".join(variant_texts)

    return ""


def _release_date(release: Dict[str, Any]) -> str:
    return _scalar_text(
        release.get("released")
        or release.get("released_formatted")
        or release.get("release_date")
        or release.get("year")
    )


def _identifiers_text(identifiers: Any) -> str:
    if isinstance(identifiers, str):
        return identifiers.strip()
    if isinstance(identifiers, dict):
        identifiers = [identifiers]
    if not isinstance(identifiers, list):
        return _scalar_text(identifiers)

    parts: List[str] = []
    for identifier in identifiers:
        if isinstance(identifier, dict):
            identifier_type = _scalar_text(identifier.get("type") or identifier.get("name"))
            value = _scalar_text(identifier.get("value"))
            descriptions = _listify_text(
                identifier.get("description") or identifier.get("descriptions") or []
            )
            if descriptions and value:
                value = f"{value} ({', '.join(descriptions)})"
            if identifier_type and value:
                parts.append(f"{identifier_type}: {value}")
            elif value:
                parts.append(value)
            elif identifier_type:
                parts.append(identifier_type)
        else:
            text = _scalar_text(identifier)
            if text:
                parts.append(text)
    return "; ".join(parts)


def _thumbnail_url(release: Dict[str, Any], item: Dict[str, Any]) -> str:
    for source in (release, item):
        for key in ("thumbnail", "thumb", "uri150"):
            text = _scalar_text(source.get(key))
            if text:
                return text

    images = release.get("images") or item.get("images")
    if isinstance(images, list):
        for image in images:
            if isinstance(image, dict):
                for key in ("uri150", "thumbnail", "thumb", "uri", "resource_url"):
                    text = _scalar_text(image.get(key))
                    if text:
                        return text
            else:
                text = _scalar_text(image)
                if text:
                    return text
    elif isinstance(images, dict):
        for key in ("uri150", "thumbnail", "thumb", "uri", "resource_url"):
            text = _scalar_text(images.get(key))
            if text:
                return text
    return ""


def _listing_format_text(formats: Any) -> str:
    if isinstance(formats, dict):
        formats = [formats]
    if not isinstance(formats, list):
        return _scalar_text(formats)

    parts: List[str] = []
    for item in formats:
        text = _format_item_text(item)
        if text and text not in parts:
            parts.append(text)
    return ", ".join(parts)


def _listing_display_name(
    listing: Dict[str, Any],
    release: Dict[str, Any],
    *,
    artist: str,
    format_text: str,
    product_id: str,
) -> str:
    title = _scalar_text(release.get("title") or listing.get("title"))
    description = _scalar_text(release.get("description"))
    base = title or description

    if artist and title:
        artist_prefix = f"{artist} - "
        base = title if title.lower().startswith(artist_prefix.lower()) else f"{artist} - {title}"
    elif not base and artist:
        base = artist
    elif not base:
        base = f"Discogs Listing {product_id}"

    if format_text:
        return f"{base} ({format_text})"
    return base


def _listing_price_and_currency(listing: Dict[str, Any]) -> Tuple[float, str]:
    original_price = listing.get("original_price") or {}
    price = listing.get("price") or {}

    price_source = original_price if original_price.get("value") not in (None, "") else price
    price_value = round(to_float(price_source.get("value")), 2)
    currency = (
        price_source.get("currency")
        or price_source.get("curr_abbr")
        or listing.get("currency")
        or price.get("currency")
        or "USD"
    )
    return price_value, str(currency).upper()


def _is_cod(payment_method: Any) -> int:
    if not isinstance(payment_method, str):
        return 0
    return 1 if "cod" in payment_method.lower() or "cash on delivery" in payment_method.lower() else 0


def _is_paid(order: Dict[str, Any]) -> int:
    status = str(order.get("status") or "").strip().lower()
    payment_state = str(order.get("payment_state") or order.get("payment_status") or "").strip().lower()
    paid_states = {
        "payment received",
        "shipped",
        "in progress",
    }
    if status in paid_states or payment_state in {"paid", "payment received", "completed"}:
        return 1
    return 0


def is_payment_received_order(order: Dict[str, Any]) -> bool:
    """Return True only for Discogs orders currently at Payment Received."""
    status = str(order.get("status") or order.get("status_name") or "").strip().lower()
    return status == PAYMENT_RECEIVED_STATUS.lower()


def _identifier_int(value: Any, prefixes: Tuple[str, ...] = ()) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    return to_int(text, default=0)


def _address_dict(address: Any) -> Dict[str, Any]:
    if isinstance(address, dict):
        return address
    if isinstance(address, str):
        lines = _normalized_address_lines(address)
        result: Dict[str, Any] = {}
        address_lines = lines
        if len(lines) >= 4:
            result["name"] = lines[0]
            address_lines = lines[1:]
        if address_lines:
            result["address"] = ", ".join(address_lines)
            result["street"] = address_lines[0]
        if len(address_lines) >= 3:
            locality = address_lines[-2]
            parts = [part.strip() for part in locality.split(",")]
            if parts:
                if not parts[-1]:
                    parts = parts[:-1]
                if parts:
                    result["city"] = parts[0]
            if len(parts) >= 3:
                if parts[1]:
                    result["state"] = parts[1]
                result["postal_code"] = parts[-1]
            elif len(parts) == 2:
                locality_parts = parts[1].rsplit(" ", 1)
                if len(locality_parts) == 2:
                    result["state"] = locality_parts[0]
                    result["postal_code"] = locality_parts[1]
                else:
                    result["postal_code"] = parts[1]
            street_lines = address_lines[:-2]
            if street_lines:
                result["street"] = ", ".join(street_lines)
        if address_lines:
            result["country"] = address_lines[-1]
        return result
    return {}


def _condensed_address(address: Any) -> Tuple[str, str]:
    if isinstance(address, str):
        lines = _normalized_address_lines(address)
        if not lines:
            return ("", "")
        if len(lines) == 1:
            return (lines[0], "")
        return (", ".join(lines[:-1]), lines[-1])
    if not isinstance(address, dict):
        return ("", "")
    parts: List[str] = []
    for key in ("street", "street_2", "address", "address_1", "address_2"):
        val = address.get(key)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())
    city = address.get("city") or address.get("town") or ""
    state = address.get("state") or ""
    postal = address.get("zip") or address.get("postal_code") or address.get("postcode") or ""
    country = address.get("country") or address.get("country_code") or ""
    locality = ", ".join(filter(None, [city, state, postal]))
    return (
        " ".join(parts).strip(),
        ", ".join(filter(None, [locality, country])).strip(),
    )


def _address_has_content(address: Any) -> bool:
    if isinstance(address, str):
        return bool(address.strip())
    if isinstance(address, dict):
        return any(
            str(address.get(key) or "").strip()
            for key in (
                "street",
                "street_2",
                "address",
                "address_1",
                "address_2",
                "city",
                "town",
                "zip",
                "postal_code",
                "postcode",
                "country",
                "country_code",
            )
        )
    return False


def discogs_listing_to_bl_product(listing: Dict[str, Any]) -> Dict[str, Any]:
    """Map a Discogs listing to a BaseLinker product payload.

    Args:
        listing: Discogs listing payload.

    Returns:
        BaseLinker product payload dictionary.
    """
    release = listing.get("release") or {}
    product_id = str(listing.get("id") or release.get("id") or listing.get("listing_id") or "")
    sku = (
        listing.get("external_id")
        or release.get("catalog_number")
        or release.get("catno")
        or release.get("id")
        or product_id
    )
    sku_str = str(sku) if sku is not None else product_id
    price_value, currency = _listing_price_and_currency(listing)
    condition = listing.get("condition") or release.get("condition")
    sleeve_condition = listing.get("sleeve_condition")
    formats = release.get("format") or release.get("formats") or []
    format_text = _listing_format_text(formats) if formats else ""
    genres = ", ".join(_listify_text(release.get("genre") or release.get("genres") or []))
    styles = ", ".join(_listify_text(release.get("style") or release.get("styles") or []))
    labels = ", ".join(_listify_text(release.get("label") or release.get("labels") or []))
    catno = _first_text(
        release.get("catno")
        or release.get("catalog_number")
        or release.get("catalog_number_text")
        or listing.get("catno")
    )
    if not catno:
        for label in release.get("labels") or []:
            if isinstance(label, dict):
                catno = _first_text(label.get("catno") or label.get("catalog_number"))
                if catno:
                    break
    images = _image_urls(release.get("images") or listing.get("images"))
    artist = _first_text(
        release.get("artist") or release.get("artists") or release.get("artist_name") or ""
    )
    title = _scalar_text(release.get("title") or listing.get("title"))
    attributes: List[Dict[str, str]] = []
    if condition:
        attributes.append({"name": "Media Condition", "value": str(condition)})
    if sleeve_condition:
        attributes.append({"name": "Sleeve Condition", "value": str(sleeve_condition)})
    if format_text:
        attributes.append({"name": "Format", "value": format_text})
    if genres:
        attributes.append({"name": "Genre", "value": genres})
    if styles:
        attributes.append({"name": "Style", "value": styles})

    quantity = listing.get("quantity") or listing.get("available") or listing.get("qty")
    description = listing.get("comments") or release.get("notes") or ""
    listed_at = listing.get("listed") or listing.get("created") or ""
    updated_at = listing.get("modified") or listing.get("last_activity") or listed_at

    return {
        "id": product_id,
        "product_id": product_id,
        "sku": str(sku_str),
        "ean": _first_text(release.get("barcode") or release.get("barcodes") or ""),
        "name": _listing_display_name(
            listing,
            release,
            artist=artist,
            format_text=format_text,
            product_id=product_id,
        ),
        "artist": artist,
        "title": title,
        "price": price_value,
        "price_brutto": price_value,
        "currency": currency,
        "quantity": to_int(quantity, default=1),
        "availability": 1 if (listing.get("status") or "").lower() == "for sale" else 0,
        "tax_rate": 0,
        # Discogs stores weight in grams, BaseLinker expects kg
        "weight": to_float(listing.get("weight"), default=0.0) / 1000.0,
        "description": description,
        "description_long": description,
        "comments": listing.get("comments") or "",
        "format_quantity": listing.get("format_quantity"),
        "location": listing.get("location") or release.get("country") or "",
        "labels": labels,
        "catno": catno,
        "release_id": release.get("id"),
        "release_year": release.get("year"),
        "status": listing.get("status"),
        "url": listing.get("uri") or release.get("resource_url"),
        "listed_at": listed_at,
        "updated_at": updated_at,
        "images": images,
        "attributes": attributes,
        "categories": [{"id": "discogs", "name": "Discogs Marketplace"}],
        "extra": {
            "sleeve_condition": sleeve_condition,
            "format": format_text,
            "genre": genres,
            "style": styles,
            "release_resource": release.get("resource_url"),
        },
    }


def discogs_order_to_bl_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """Map a Discogs order payload to a BaseLinker order payload.

    Args:
        order: Discogs order payload.

    Returns:
        BaseLinker order payload dictionary.
    """
    buyer = order.get("buyer") or {}
    buyer_address_raw = buyer.get("address") or order.get("invoice_address") or {}
    ship_addr_raw = order.get("shipping_address") or order.get("shipping_address_details") or {}
    if not _address_has_content(ship_addr_raw):
        ship_addr_raw = order.get("shipping") or buyer_address_raw or {}
    if not _address_has_content(buyer_address_raw):
        buyer_address_raw = ship_addr_raw
    buyer_address = _address_dict(buyer_address_raw)
    ship_addr = _address_dict(ship_addr_raw)
    total = order.get("total") or {}
    shipping = order.get("shipping") or {}
    currency = total.get("currency") or shipping.get("currency") or "USD"
    payment = order.get("payment") or order.get("payment_method") or ""
    raw_order_id = order.get("id") or order.get("order_id") or ""
    try:
        order_id: int | str = int(raw_order_id)
    except Exception:
        order_id = str(raw_order_id)
    created_epoch = iso_to_epoch(order.get("created"))
    updated_epoch = iso_to_epoch(order.get("last_activity"))
    items = order.get("items") or []

    bl_items: List[Dict[str, Any]] = []
    for item in items:
        release = item.get("release") or {}
        price = item.get("price") or {}
        attributes: List[Dict[str, str]] = []
        condition = (
            item.get("media_condition")
            or item.get("condition")
            or release.get("condition")
        )
        sleeve_condition = item.get("sleeve_condition") or release.get("sleeve_condition")
        _append_attribute(attributes, "Format", _release_format_text(release))
        _append_attribute(attributes, "Media Condition", condition)
        _append_attribute(attributes, "Sleeve Condition", sleeve_condition)
        _append_attribute(attributes, "Country", release.get("country"))
        _append_attribute(attributes, "Release Date", _release_date(release))
        _append_attribute(attributes, "Release ID", extract_discogs_order_item_release_id(item))
        _append_attribute(attributes, "Notes", release.get("notes"))
        _append_attribute(attributes, "Identifiers", _identifiers_text(release.get("identifiers")))
        _append_attribute(attributes, "Cat#", release.get("catalog_number"))
        thumbnail = _thumbnail_url(release, item)
        release_description = _first_text(release.get("description") or release.get("title") or "")
        artist = _first_text(
            release.get("artist") or release.get("artists") or release.get("artist_name") or ""
        )
        item_title = _first_text(item.get("description") or item.get("title") or "")
        display_name = release_description or item_title
        if not display_name and artist and release.get("title"):
            display_name = f"{artist} - {release.get('title')}"
        if not display_name:
            display_name = release.get("title") or f"Discogs Item {item.get('id') or ''}".strip()
        product_comment = _joined_text(
            item.get("comments"),
            item.get("status"),
            item.get("seller_comments"),
        )
        product_description = _joined_text(release_description, item_title, product_comment)
        listing_id = extract_discogs_order_item_listing_id(item)
        product_id = listing_id or item.get("id") or item.get("listing_id") or release.get("id") or ""
        try:
            product_id_int: int | str = int(listing_id)
        except Exception:
            product_id_int = listing_id
        price_value = to_float(price.get("value"))
        sku = _discogs_order_item_sku(item, listing_id)
        location = item.get("item_location") or item.get("location") or ""
        bl_items.append(
            {
                "id": product_id_int,
                "product_id": product_id,
                "name": display_name,
                "sku": sku,
                "location": location,
                "ean": _first_text(release.get("barcode") or release.get("barcodes") or ""),
                "quantity": to_int(item.get("quantity"), default=1),
                "price": price_value,
                "price_brutto": price_value,
                "currency": price.get("currency") or currency,
                "description": product_description,
                "comments": product_comment,
                "attributes": attributes,
                "thumbnail": thumbnail,
                "release_id": release.get("id"),
                "listing_id": listing_id,
            }
        )

    messages = order.get("messages") or []
    message_text = "\n".join(
        f"{msg.get('posted')} - {msg.get('message') or msg.get('body')}"
        for msg in messages
        if isinstance(msg, dict)
    ).strip()

    status_id_value = _status_id(order.get("status"))
    status_name = status_name_for(status_id_value)
    invoice_country_code = _country_code(buyer_address)
    delivery_country_code = _country_code(ship_addr)
    phone = buyer.get("phone") or _phone_from_address(ship_addr_raw) or _phone_from_address(buyer_address_raw)

    return {
        "order_id": order_id,
        "order_source": "discogs",
        "order_source_id": order_id,
        "status": order.get("status"),
        "status_id": status_id_value,
        "order_status_id": status_id_value,
        "status_name": status_name,
        "date_add": created_epoch,
        "date_confirmed": updated_epoch or created_epoch,
        "currency": currency,
        "payment_method": payment,
        "payment_status": order.get("payment_state") or order.get("payment_status"),
        "paid": _is_paid(order),
        "user_login": buyer.get("username"),
        "email": buyer.get("email"),
        "phone": phone,
        "delivery_method": order.get("shipping_method") or shipping.get("method") or "",
        "delivery_price": to_float(shipping.get("value")),
        "total_price": to_float(total.get("value")),
        "buyer_login": buyer.get("username"),
        "buyer_email": buyer.get("email"),
        "buyer_phone": phone,
        "invoice_fullname": ship_addr.get("name") or buyer.get("name") or buyer.get("username"),
        "invoice_company": buyer.get("company"),
        "invoice_address": buyer_address.get("street"),
        "invoice_city": buyer_address.get("city"),
        "invoice_state": buyer_address.get("state"),
        "invoice_postcode": _normalize_postcode(
            buyer_address.get("zip")
            or buyer_address.get("postal_code")
            or buyer_address.get("postcode")
        ),
        "invoice_country": buyer_address.get("country"),
        "invoice_country_code": invoice_country_code,
        "delivery_fullname": ship_addr.get("name") or buyer.get("name") or buyer.get("username"),
        "delivery_company": ship_addr.get("company"),
        "delivery_address": ship_addr.get("street"),
        "delivery_city": ship_addr.get("city"),
        "delivery_state": ship_addr.get("state"),
        "delivery_postcode": _normalize_postcode(
            ship_addr.get("zip")
            or ship_addr.get("postal_code")
            or ship_addr.get("postcode")
        ),
        "delivery_country": ship_addr.get("country"),
        "delivery_country_code": delivery_country_code,
        "admin_comments": message_text,
        "user_comments": order.get("additional_instructions") or "",
        "products": bl_items,
        "extra": {
            "discogs_order_url": order.get("resource_url") or order.get("uri"),
            "last_activity": order.get("last_activity"),
            "shipping_tracking": order.get("tracking_number") or order.get("tracking"),
        },
    }


def discogs_order_to_bl_add_order_payload(order: Dict[str, Any]) -> Dict[str, Any]:
    """Build a strict BaseLinker addOrder payload from a Discogs order."""
    mapped = discogs_order_to_bl_order(order)
    products: List[Dict[str, Any]] = []
    for item in mapped.get("products") or []:
        product_payload = {
            "storage": "shop",
            "storage_id": _identifier_int(BL_SHOP_ID, prefixes=("shop_",)),
            "product_id": item.get("product_id") or item.get("id") or "",
            "variant_id": "",
            "name": item.get("name") or "",
            "sku": str(item.get("sku") or item.get("release_id") or ""),
            "ean": item.get("ean") or "",
            "location": item.get("location") or "",
            "warehouse_id": _identifier_int(BL_WAREHOUSE_ID, prefixes=("bl_", "warehouse_")),
            "attributes": _attributes_array(item.get("attributes")),
            "price_brutto": to_float(item.get("price_brutto")),
            "tax_rate": to_float(item.get("tax_rate")),
            "quantity": to_int(item.get("quantity"), default=1),
            "weight": to_float(item.get("weight")),
        }
        products.append(product_payload)

    payload = {
        "order_status_id": mapped.get("order_status_id"),
        "date_add": mapped.get("date_add"),
        "currency": mapped.get("currency"),
        "payment_method": mapped.get("payment_method") or "",
        "payment_method_cod": _is_cod(mapped.get("payment_method")),
        "paid": _is_paid(order),
        "user_comments": mapped.get("user_comments") or "",
        "admin_comments": mapped.get("admin_comments") or "",
        "email": mapped.get("email") or "",
        "phone": mapped.get("phone") or "",
        "user_login": mapped.get("user_login") or "",
        "delivery_method": mapped.get("delivery_method") or "",
        "delivery_price": to_float(mapped.get("delivery_price")),
        "delivery_fullname": mapped.get("delivery_fullname") or "",
        "delivery_company": mapped.get("delivery_company") or "",
        "delivery_address": mapped.get("delivery_address") or "",
        "delivery_postcode": mapped.get("delivery_postcode") or "",
        "delivery_city": mapped.get("delivery_city") or "",
        "delivery_state": mapped.get("delivery_state") or "",
        "delivery_country_code": mapped.get("delivery_country_code") or "",
        "invoice_fullname": mapped.get("invoice_fullname") or "",
        "invoice_company": mapped.get("invoice_company") or "",
        "invoice_address": mapped.get("invoice_address") or "",
        "invoice_postcode": mapped.get("invoice_postcode") or "",
        "invoice_city": mapped.get("invoice_city") or "",
        "invoice_state": mapped.get("invoice_state") or "",
        "invoice_country_code": mapped.get("invoice_country_code") or "",
        "want_invoice": 1 if mapped.get("invoice_address") else 0,
        "products": products,
    }
    return payload


def baselinker_order_to_discogs_update(
    order: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build a Discogs update payload from a BaseLinker order record.

    Args:
        order: BaseLinker order payload.

    Returns:
        Update payload for Discogs, or None when not a Discogs order.
    """
    if str(order.get("order_source", "")).lower() != "discogs":
        return None

    discogs_id = order.get("order_source_id") or order.get("discogs_id")
    if discogs_id is not None:
        discogs_id = str(discogs_id).strip()
    if not discogs_id:
        discogs_id = None

    status = _discogs_status_from_order(order)
    tracking = _tracking_from_order(order)
    comment = order.get("admin_comments") or order.get("user_comments") or ""

    update_fields: Dict[str, Any] = {}
    if status:
        update_fields["status"] = status
    if tracking:
        tracking_payload = discogs_tracking_payload(
            tracking,
            carrier=_tracking_carrier_from_order(order),
        )
        if tracking_payload:
            update_fields["tracking"] = tracking_payload

    # Construct a user-facing message summarising the update
    parts: List[str] = []
    if status:
        parts.append(f"Status: {status}")
    if tracking:
        parts.append(f"Tracking: {tracking}")
    if comment:
        parts.append(str(comment))
    message = " | ".join(parts)

    if not update_fields and not message:
        return None

    return {
        "discogs_id": discogs_id,
        "update_fields": update_fields,
        "message": message,
    }


__all__ = [
    "PAYMENT_RECEIVED_STATUS",
    "extract_discogs_order_item_listing_id",
    "extract_discogs_order_item_release_id",
    "enrich_discogs_order_listing_details",
    "enrich_discogs_order_release_details",
    "discogs_listing_to_bl_product",
    "discogs_order_to_bl_order",
    "discogs_order_to_bl_add_order_payload",
    "baselinker_order_to_discogs_update",
    "is_payment_received_order",
]
