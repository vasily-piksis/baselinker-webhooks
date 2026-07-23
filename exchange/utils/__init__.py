"""Utilities package for exchange.

This package contains shared utility functions used across the exchange module,
the webhook service. Consolidates common patterns to avoid duplication.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


# =============================================================================
# Boolean parsing
# =============================================================================


def resolve_bool(value: Optional[str | bool], default: bool = False) -> bool:
    """Normalize a bool-like value from env/variable sources.

    Args:
        value: String or boolean value to normalize.
        default: Fallback value when ``value`` is None.

    Returns:
        Parsed boolean value.

    Examples:
        >>> resolve_bool("true")
        True
        >>> resolve_bool("0")
        False
        >>> resolve_bool(None, default=True)
        True
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_bool_or_none(value: Any) -> Optional[bool]:
    """Normalize a bool-like value, preserving None.

    Args:
        value: Value to normalize.

    Returns:
        Parsed boolean value or None if input is None.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


# =============================================================================
# Numeric parsing
# =============================================================================


def to_int(value: Any, default: int = 0) -> int:
    """Convert a value to integer with fallback.

    Handles None, empty strings, lists, dicts, and numeric strings.
    Uses float() first to handle strings like "1.5" -> 1.

    Args:
        value: The value to convert.
        default: Default value if conversion fails.

    Returns:
        Integer value or default.

    Examples:
        >>> to_int("42")
        42
        >>> to_int("3.7")
        3
        >>> to_int(None, default=10)
        10
    """
    try:
        if value in (None, "", [], {}):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    """Convert a value to float with fallback.

    Handles None, empty strings, lists, dicts, and numeric strings.

    Args:
        value: The value to convert.
        default: Default value if conversion fails.

    Returns:
        Float value or default.

    Examples:
        >>> to_float("3.14")
        3.14
        >>> to_float(None)
        0.0
    """
    try:
        if value in (None, "", [], {}):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# =============================================================================
# String normalization
# =============================================================================


def normalize_action(action: str) -> str:
    """Normalize an action string by removing dots and lowercasing.

    Used for consistent action comparison across inventory operations.

    Args:
        action: Action string like "inventory.add" or "ADD".

    Returns:
        Normalized action string like "inventoryadd" or "add".

    Examples:
        >>> normalize_action("inventory.add")
        'inventoryadd'
        >>> normalize_action("DELETE")
        'delete'
    """
    return action.replace(".", "").lower()


# =============================================================================
# Timestamp utilities
# =============================================================================


def iso_to_epoch(value: Any) -> int:
    """Parse an ISO-8601 string or numeric timestamp to Unix epoch seconds.

    Args:
        value: ISO-8601 string, int, or float timestamp.

    Returns:
        Unix epoch seconds, or 0 if parsing fails.

    Examples:
        >>> iso_to_epoch("2025-01-15T12:00:00Z")
        1736942400
        >>> iso_to_epoch(1736942400)
        1736942400
    """
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str) or not value.strip():
        return 0
    cleaned = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def epoch_to_iso(epoch: Any) -> str:
    """Convert Unix epoch (seconds or milliseconds) to ISO-8601 UTC string.

    Automatically detects milliseconds (values > 1e12) and converts.

    Args:
        epoch: Unix timestamp in seconds or milliseconds.

    Returns:
        ISO-8601 UTC string with 'Z' suffix, or empty string on error.

    Examples:
        >>> epoch_to_iso(1736942400)
        '2025-01-15T12:00:00Z'
        >>> epoch_to_iso(1736942400000)  # milliseconds
        '2025-01-15T12:00:00Z'
    """
    if epoch in (None, "", 0, "0"):
        return ""
    try:
        ts = float(epoch)
        # Heuristic: treat very large values as milliseconds
        if ts > 1e12:
            ts /= 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return ""


__all__ = [
    # Boolean
    "resolve_bool",
    "resolve_bool_or_none",
    # Numeric
    "to_int",
    "to_float",
    # String
    "normalize_action",
    # Timestamp
    "iso_to_epoch",
    "epoch_to_iso",
]
