"""Discogs order status definitions used by the Exchange protocol."""

from __future__ import annotations

from typing import Any, Optional

DISCOGS_ORDER_STATUSES = (
    "New Order",
    "Buyer Contacted",
    "Invoice Sent",
    "Payment Pending",
    "Payment Received",
    "In Progress",
    "Shipped",
    "Refund Sent",
    "Cancelled (Non-Paying Buyer)",
    "Cancelled (Item Unavailable)",
    "Cancelled (Per Buyer's Request)",
)

DISCOGS_STATUS_ID_TO_NAME = {
    str(index): status for index, status in enumerate(DISCOGS_ORDER_STATUSES, start=1)
}

_DISCOGS_STATUS_BY_NAME = {status.lower(): status for status in DISCOGS_ORDER_STATUSES}


def normalize_discogs_order_status(value: Any) -> Optional[str]:
    """Return a Discogs order status for a StatusesList ID or exact status name."""
    if value in (None, ""):
        return None

    text = str(value).strip()
    if not text:
        return None

    if text in DISCOGS_STATUS_ID_TO_NAME:
        return DISCOGS_STATUS_ID_TO_NAME[text]

    lowered = text.lower()
    return _DISCOGS_STATUS_BY_NAME.get(lowered)


def discogs_order_status_text(value: Any) -> Optional[str]:
    """Return a comparable Discogs status string from API text or simple objects."""
    if isinstance(value, dict):
        value = value.get("status") or value.get("name") or value.get("value")
    status = normalize_discogs_order_status(value)
    if status:
        return status
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def discogs_order_next_statuses(order: dict[str, Any]) -> set[str]:
    """Return the normalized next_status set from a Discogs order payload."""
    raw_statuses = order.get("next_status") or order.get("next_statuses") or []
    if isinstance(raw_statuses, (str, dict)):
        raw_statuses = [raw_statuses]
    statuses: set[str] = set()
    for raw_status in raw_statuses:
        status = discogs_order_status_text(raw_status)
        if status:
            statuses.add(status)
    return statuses


def discogs_tracking_payload(number: Any, carrier: Any = None) -> Optional[dict[str, str]]:
    """Build a Discogs tracking object from BaseLinker-provided values."""
    if number in (None, ""):
        return None

    number_text = str(number).strip()
    if not number_text:
        return None

    tracking = {"number": number_text}
    if carrier not in (None, ""):
        carrier_text = str(carrier).strip()
        if carrier_text:
            tracking["carrier"] = carrier_text
    return tracking


__all__ = [
    "DISCOGS_ORDER_STATUSES",
    "DISCOGS_STATUS_ID_TO_NAME",
    "discogs_order_next_statuses",
    "discogs_order_status_text",
    "discogs_tracking_payload",
    "normalize_discogs_order_status",
]
