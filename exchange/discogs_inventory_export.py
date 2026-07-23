"""Lightweight helpers for reading Discogs inventory export CSV files."""

from __future__ import annotations

import csv
import io
import zipfile
from typing import Any, Dict, List, Optional, Tuple


def _clean_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def _value_for(cleaned: Dict[str, Any], *candidates: str) -> Optional[str]:
    for candidate in candidates:
        for key, value in cleaned.items():
            if key.strip().lower() == candidate:
                if value not in (None, ""):
                    return str(value).strip()
    return None


def _normalize_export_row(
    row: Dict[str, Any],
    *,
    listing_key: Optional[str],
    external_key: Optional[str],
) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in row.items():
        if key is None:
            continue
        key_text = str(key).strip()
        if not key_text:
            continue
        cleaned[key_text] = _clean_value(value)

    listing_id = _value_for(cleaned, "listing_id", "listing id")
    if not listing_id and listing_key:
        listing_id = _clean_value(cleaned.get(listing_key))
    external_id = _value_for(cleaned, "external_id", "external id", "external_sku", "sku")
    if not external_id and external_key:
        external_id = _clean_value(cleaned.get(external_key))

    if listing_id not in (None, ""):
        cleaned["listing_id"] = listing_id
    if external_id not in (None, ""):
        cleaned["external_id"] = external_id
    return cleaned


def decode_discogs_inventory_export(payload: bytes | str) -> str:
    """Decode raw Discogs inventory export bytes to CSV text.

    Supports both plain CSV and ZIP-wrapped CSV exports.
    """
    if isinstance(payload, bytes):
        payload_bytes = payload
        if zipfile.is_zipfile(io.BytesIO(payload_bytes)):
            with zipfile.ZipFile(io.BytesIO(payload_bytes)) as zf:
                csv_names = [
                    name
                    for name in zf.namelist()
                    if not name.endswith("/") and name.lower().endswith(".csv")
                ]
                if not csv_names:
                    raise ValueError("Discogs export ZIP does not contain a CSV file")
                payload_bytes = zf.read(csv_names[0])
        return payload_bytes.decode("utf-8-sig", errors="replace")
    return str(payload)


def read_discogs_inventory_export_table(
    payload: bytes | str,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Read Discogs export into raw CSV rows while preserving header order."""
    text = decode_discogs_inventory_export(payload)
    reader = csv.DictReader(io.StringIO(text, newline=""))
    fieldnames = list(reader.fieldnames or [])
    rows: List[Dict[str, Any]] = []
    for row in reader:
        cleaned: Dict[str, Any] = {}
        for key, value in row.items():
            if key is None:
                continue
            cleaned[str(key)] = _clean_value(value)
        rows.append(cleaned)
    return fieldnames, rows


def parse_discogs_inventory_export(payload: bytes | str) -> List[Dict[str, Any]]:
    """Parse Discogs inventory export CSV bytes into normalized row dictionaries."""
    fieldnames, raw_rows = read_discogs_inventory_export_table(payload)
    listing_key = fieldnames[0] if fieldnames else None
    external_key = fieldnames[14] if len(fieldnames) > 14 else None
    rows: List[Dict[str, Any]] = []
    for row in raw_rows:
        normalized = _normalize_export_row(row, listing_key=listing_key, external_key=external_key)
        if normalized:
            rows.append(normalized)
    return rows


__all__ = [
    "decode_discogs_inventory_export",
    "read_discogs_inventory_export_table",
    "parse_discogs_inventory_export",
]
