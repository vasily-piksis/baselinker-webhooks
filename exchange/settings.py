"""Exchange service configuration settings."""

from __future__ import annotations

import json
import os
from pathlib import Path

# Centralized runtime configuration. EXCHANGE_DIR is used for optional export artifacts.

EXCHANGE_DIR = Path(os.getenv("EXCHANGE_DIR", "./data/exchange")).resolve()
EXCHANGE_DIR.mkdir(parents=True, exist_ok=True)

BASECOM_FILE_PREFIX = os.getenv("BASECOM_FILE_PREFIX", "basecom_export")
BASECOM_FILE_FORMAT = os.getenv("BASECOM_FILE_FORMAT", "csv").lower()

DISCOGS_CSV_PREFIX = os.getenv("DISCOGS_CSV_PREFIX", "discogs_upload")
_DEFAULT_DISCOGS_LISTING_COMMENTS = (
    "All items are factory new and sealed if originally manufacturer-sealed. "
    "Make a note our current handling time for this item is up to 3 working day(s). "
    "Please ensure you have read and accepted our Seller Terms regarding our service process. "
    "Thank you for shopping with us!"
)
DISCOGS_DEFAULT_LISTING_COMMENTS = os.getenv(
    "DISCOGS_DEFAULT_LISTING_COMMENTS",
    _DEFAULT_DISCOGS_LISTING_COMMENTS,
)

# BaseLinker inventory and shop configuration for product linking
# BL_INVENTORY_ID: The BaseLinker catalog/inventory ID where products are stored
# BL_SHOP_ID: The shop ID used for linking products (format: "shop_123" or just "123")
BL_INVENTORY_ID = os.getenv("BL_INVENTORY_ID", "")
BL_SHOP_ID = os.getenv("BL_SHOP_ID", "")
BL_WAREHOUSE_ID = os.getenv("BL_WAREHOUSE_ID", "bl_1")
BL_PRICE_GROUP_ID = os.getenv("BL_PRICE_GROUP_ID", "1")

_status_map_raw = os.getenv("BL_STATUS_MAP") or os.getenv("DEFAULT_STATUS_MAP") or "{}"
BL_STATUS_ID_MAP: dict[str, int] = {}
BL_STATUS_NAME_MAP: dict[str, str] = {}
try:
    parsed = json.loads(_status_map_raw)
    if isinstance(parsed, dict):
        for name, identifier in parsed.items():
            if identifier in (None, ""):
                continue
            try:
                ident_int = int(identifier)
            except (TypeError, ValueError):
                continue
            BL_STATUS_ID_MAP[str(name).lower()] = ident_int
            BL_STATUS_NAME_MAP[str(ident_int)] = str(name)
except Exception:
    BL_STATUS_ID_MAP = {}
    BL_STATUS_NAME_MAP = {}

__all__ = [
    "EXCHANGE_DIR",
    "BASECOM_FILE_PREFIX",
    "BASECOM_FILE_FORMAT",
    "DISCOGS_CSV_PREFIX",
    "DISCOGS_DEFAULT_LISTING_COMMENTS",
    "BL_STATUS_ID_MAP",
    "BL_STATUS_NAME_MAP",
    "BL_INVENTORY_ID",
    "BL_SHOP_ID",
    "BL_WAREHOUSE_ID",
    "BL_PRICE_GROUP_ID",
]
