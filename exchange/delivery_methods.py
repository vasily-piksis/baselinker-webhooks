"""Delivery methods exposed through the BaseLinker Exchange protocol."""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote

DELIVERY_METHOD_ID_TO_NAME = {
    "1": "Evri",
    "2": "FedEx",
    "3": "DHL Express",
    "4": "UPS",
    "5": "GlobalPost",
    "6": "DPD",
    "7": "Yodel",
}

_DELIVERY_METHOD_ALIASES = {
    "evri": "Evri",
    "hermes": "Evri",
    "myhermes": "Evri",
    "parcelshop": "Evri",
    "fedex": "FedEx",
    "fed ex": "FedEx",
    "dhl express": "DHL Express",
    "ups": "UPS",
    "globalpost": "GlobalPost",
    "global post": "GlobalPost",
    "dpd": "DPD",
    "yodel": "Yodel",
    "inpost": "InPost",
    "inpost uk": "InPost",
    "royal mail": "Royal Mail",
    "royalmail": "Royal Mail",
}

_DELIVERY_METHOD_CONTAINS = (
    ("royalmail", "Royal Mail"),
    ("myhermes", "Evri"),
    ("parcelshop", "Evri"),
    ("evri", "Evri"),
    ("collectplusdropoffservice", "Yodel"),
    ("collectplus", "Yodel"),
    ("yodel", "Yodel"),
    ("inpost", "InPost"),
    ("dpd", "DPD"),
)

_TRACKING_URL_TEMPLATES = {
    "Evri": "https://www.evri.com/track/parcel/{tracking_number}",
    "FedEx": "https://www.fedex.com/fedextrack/no-results-found?rknbr={tracking_number}",
    "DHL Express": "https://www.dhl.com/en/express/tracking.html?AWB={tracking_number}",
    "UPS": "https://www.ups.com/track?tracknum={tracking_number}",
    "GlobalPost": "https://www.goglobalpost.com/track-detail/?t={tracking_number}",
    "DPD": "https://track.dpd.co.uk/search?reference={tracking_number}",
    "Yodel": "https://inpost.co.uk/tracking/result?parcel_code={tracking_number}",
    "InPost": "https://inpost.co.uk/tracking/result?parcel_code={tracking_number}",
    "Royal Mail": "https://www.royalmail.com/portal/rm/track?trackNumber={tracking_number}",
}


def delivery_method_entries() -> dict[str, str]:
    """Return the delivery method list expected by DeliveryMethodsList."""
    return dict(DELIVERY_METHOD_ID_TO_NAME)


def normalize_delivery_method(value: Any) -> Optional[str]:
    """Return a canonical delivery method name from BaseLinker input."""
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text in DELIVERY_METHOD_ID_TO_NAME:
        return DELIVERY_METHOD_ID_TO_NAME[text]
    canonical = _DELIVERY_METHOD_ALIASES.get(text.lower())
    if canonical:
        return canonical
    compact = "".join(char for char in text.lower() if char.isalnum())
    for needle, canonical in _DELIVERY_METHOD_CONTAINS:
        if needle in compact:
            return canonical
    return text


def tracking_number_from_payload(payload: dict[str, Any]) -> Optional[str]:
    """Extract the tracking number from a BaseLinker delivery_number update."""
    for key in ("update_value", "tracking_number", "delivery_package_nr", "tracking_url"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def tracking_url_for_delivery_method(
    delivery_method: Any,
    tracking_number: Any,
) -> Optional[str]:
    """Build the customer-facing tracking URL for a supported delivery method."""
    method = normalize_delivery_method(delivery_method)
    if not method or tracking_number in (None, ""):
        return None
    tracking_text = str(tracking_number).strip()
    if not tracking_text:
        return None
    template = _TRACKING_URL_TEMPLATES.get(method)
    if not template:
        return None
    return template.format(tracking_number=quote(tracking_text, safe=""))


def discogs_tracking_message(payload: dict[str, Any]) -> Optional[str]:
    """Build the Discogs BBCode tracking message for a delivery_number update."""
    tracking_number = tracking_number_from_payload(payload)
    if not tracking_number:
        return None

    method = normalize_delivery_method(payload.get("delivery_method_name")) or "your courier"
    tracking_url = tracking_url_for_delivery_method(method, tracking_number)
    tracking_value = str(tracking_number).strip()
    tracking_link = (
        f"[url={tracking_url}]{tracking_value}[/url]" if tracking_url else tracking_value
    )
    progress_text = (
        "using the link below"
        if tracking_url
        else "using the tracking number below"
    )
    return (
        f"Your order has been shipped via {method}. "
        f"You can track your parcel's progress {progress_text}:\n\n"
        f"Tracking Number: {tracking_link}\n\n"
        "Best regards"
    )


__all__ = [
    "DELIVERY_METHOD_ID_TO_NAME",
    "delivery_method_entries",
    "discogs_tracking_message",
    "normalize_delivery_method",
    "tracking_number_from_payload",
    "tracking_url_for_delivery_method",
]
