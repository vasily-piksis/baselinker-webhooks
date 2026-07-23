"""Mapping helpers between BaseLinker, Exchange, and Discogs formats."""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

from exchange.clients.discogs_client import search_release

log = logging.getLogger(__name__)

_RELEASE_MAP_PATH = os.getenv("DISCOGS_RELEASE_MAP", "")


def _load_release_map() -> Dict[str, int]:
    if not _RELEASE_MAP_PATH:
        return {}
    path = Path(_RELEASE_MAP_PATH)
    if not path.exists():
        log.warning("Discogs release map path %s not found", path)
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            log.warning(
                "Discogs release map %s must be an object of external_sku -> release_id",
                path,
            )
            return {}
        out: Dict[str, int] = {}
        for key, val in data.items():
            try:
                out[str(key)] = int(val)
            except (TypeError, ValueError):
                log.warning("Skipping invalid release map entry %s: %r", key, val)
        return out
    except Exception:
        log.exception("Failed to read Discogs release map %s", path)
        return {}


@lru_cache(maxsize=1)
def _cached_release_map() -> Dict[str, int]:
    return _load_release_map()


@lru_cache(maxsize=256)
def _lookup_release_via_discogs(external: str, title: str) -> Optional[int]:
    """Query Discogs search endpoint and return the first release id if available.

    Args:
        external: External SKU or catalog number for lookup.
        title: Title string used for search fallback.

    Returns:
        Discogs release id if found, otherwise None.
    """
    barcode: Optional[str] = external if external.isdigit() else None
    try:
        resp = search_release(
            query=title or external,
            barcode=barcode,
            catno=external if not barcode else None,
            title=title or None,
        )
    except Exception as exc:
        log.warning(
            "Discogs release lookup failed for external_sku=%s title=%s: %s",
            external,
            title,
            exc,
        )
        return None

    for item in resp.get("results", []):
        rid = item.get("id")
        if rid:
            try:
                return int(rid)
            except (TypeError, ValueError):
                continue
    return None


def _resolve_release_id(row: Dict[str, Any]) -> Optional[int]:
    """Best-effort resolver for Discogs release id, with caching.

    Args:
        row: Exchange row containing release identifiers or metadata.

    Returns:
        Discogs release id if resolved, otherwise None.
    """
    if not isinstance(row, dict):
        return None

    explicit = row.get("release_id") or row.get("discogs_release_id")
    if isinstance(explicit, (int, float)) and int(explicit):
        return int(explicit)
    if isinstance(explicit, str) and explicit.strip().isdigit():
        return int(explicit.strip())

    # 1) explicit numeric value already provided in the row (e.g., enriched format column)
    fmt = str(row.get("format") or "").strip()
    if fmt.isdigit():
        return int(fmt)

    # 2) external SKU match from optional mapping file
    external = str(row.get("external_sku") or "").strip()
    if external:
        release_map = _cached_release_map()
        mapped = release_map.get(external)
        if mapped:
            return mapped

    # 3) Discogs search fallback (requires Seller mode token)
    title = str(row.get("title") or "").strip()
    if not external and not title:
        return None

    return _lookup_release_via_discogs(external, title)


def exchange_row_to_discogs_listing(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map an exchange CSV row into a Discogs listing payload.

    Args:
        row: Exchange CSV row dictionary.

    Returns:
        Discogs listing payload dictionary.
    """
    release_id = _resolve_release_id(row)
    qty_raw = row.get("quantity")
    if isinstance(qty_raw, (int, float, str)):
        try:
            quantity = int(qty_raw)
        except (TypeError, ValueError):
            quantity = 1
    else:
        quantity = 1
    
    # Parse format_quantity if provided
    format_qty_raw = row.get("format_quantity")
    format_quantity = None
    if format_qty_raw not in (None, ""):
        try:
            format_quantity = int(format_qty_raw)
        except (TypeError, ValueError):
            pass
    
    # Parse allow_offers - accept various truthy values, default to False
    allow_offers_raw = row.get("allow_offers")
    allow_offers = False
    if allow_offers_raw not in (None, ""):
        allow_offers_str = str(allow_offers_raw).lower().strip()
        if allow_offers_str in ("y", "yes", "true", "1", "on"):
            allow_offers = True
    
    # Parse weight - convert from kg (BaseLinker) to grams (Discogs)
    weight_raw = row.get("weight_kg") or row.get("weight")
    weight_grams = None
    if weight_raw not in (None, ""):
        try:
            weight_kg = float(weight_raw)
            # If weight is already in grams (> 100), don't convert
            # Otherwise assume it's in kg and convert to grams
            if weight_kg > 100:
                weight_grams = int(weight_kg)  # Already in grams
            else:
                weight_grams = int(weight_kg * 1000)  # Convert kg to grams
        except (TypeError, ValueError):
            pass
    
    return {
        "release_id": release_id,
        "price": str(row.get("price") or "0.00"),
        "condition": row.get("condition") or "Mint (M)",
        "sleeve_condition": row.get("sleeve_condition") or "Mint (M)",
        "status": "For Sale",
        "comments": row.get("notes") or None,
        "external_id": row.get("external_sku") or row.get("external_id") or None,
        "location": row.get("location") or None,
        "quantity": quantity,
        "format_quantity": format_quantity,
        "allow_offers": allow_offers,
        "weight": weight_grams,  # Weight in grams for Discogs
    }
