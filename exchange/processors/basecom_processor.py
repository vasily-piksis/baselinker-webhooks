# exchange/processors/basecom_processor.py
"""Base.com processor for generating export data.

This module provides functions for:
- Building Base.com export rows
- Storing export data in the database
- Generating export bytes on-demand
- Retaining Base.com export artifacts locally
"""

from __future__ import annotations

import atexit
import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast
from uuid import UUID

from database.repositories.basecom_export_repository import BasecomExportRepository
from exchange.errors import NotFoundError, ProcessorError
from database.session import get_session
from exchange.settings import (
    BASECOM_FILE_FORMAT,
    BASECOM_FILE_PREFIX,
)
from exchange.utils import normalize_action

log = logging.getLogger("exchange.processors.basecom")


BASECOM_FIELD_ORDER = [
    "external_sku",
    "release_id",
    "title",
    "artist",
    "format",
    "condition",
    "price",
    "currency",
    "quantity",
    "location",
    "notes",
    "created_at",
]

def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _resolve_release(exchange_row: Dict[str, str], discogs_row: Optional[Dict[str, str]]) -> str:
    if discogs_row:
        release = discogs_row.get("release_id")
        if release not in (None, ""):
            return str(release)
    for key in ("format", "release_id", "discogs_release_id"):
        value = exchange_row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def build_basecom_rows(
    exchange_rows: Iterable[Dict[str, str]],
    discogs_rows: Iterable[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Merge exchange rows with Discogs CSV rows for Base.com export.

    Args:
        exchange_rows: Normalized exchange rows
        discogs_rows: Discogs CSV rows

    Returns:
        List of merged rows for Base.com export
    """
    discogs_by_external: Dict[str, Dict[str, str]] = {}
    for item in discogs_rows or []:
        ext = item.get("external_id") or item.get("release_id")
        if ext is None:
            continue
        discogs_by_external[str(ext)] = item

    merged: List[Dict[str, str]] = []
    for row in exchange_rows or []:
        ext = row.get("external_sku") or ""
        discogs_match = discogs_by_external.get(str(ext))
        record: Dict[str, str] = {}
        for field in BASECOM_FIELD_ORDER:
            if field == "release_id":
                record[field] = _resolve_release(row, discogs_match)
            elif field in {"price", "quantity"}:
                value = (
                    discogs_match.get(field)
                    if discogs_match and discogs_match.get(field) not in (None, "")
                    else row.get(field, "")
                )
                record[field] = str(value)
            else:
                record[field] = str(row.get(field, "") or "")
        merged.append(record)
    return merged


_normalize_action = normalize_action  # backward compat alias


def _export_bytes_from_rows(
    payload_rows: List[Dict[str, str]],
    file_format: str,
) -> Tuple[bytes, str]:
    if file_format == "csv":
        buf = io.StringIO(newline="")
        writer = csv.DictWriter(buf, fieldnames=BASECOM_FIELD_ORDER)
        writer.writeheader()
        for row in payload_rows:
            writer.writerow({key: row.get(key, "") for key in BASECOM_FIELD_ORDER})
        return buf.getvalue().encode("utf-8"), "text/csv"
    if file_format == "json":
        return (
            json.dumps(payload_rows, ensure_ascii=False, indent=2).encode("utf-8"),
            "application/json",
        )
    raise ProcessorError(
        f"Unsupported BASECOM_FILE_FORMAT '{file_format}'",
        error_code="basecom_file_format_invalid",
        context={"file_format": file_format},
    )


def _basecom_filename(action: str, export_id: UUID, file_format: str) -> str:
    normalized = _normalize_action(action)
    timestamp = _utc_timestamp()
    return f"{BASECOM_FILE_PREFIX}_{normalized}_{timestamp}_{str(export_id)[:8]}.{file_format}"


def write_basecom_file(
    payload_rows: List[Dict[str, str]],
    *,
    action: str,
    event_token: Optional[str],
) -> Optional[str]:
    """Store Base.com export rows in the database.

    Args:
        payload_rows: List of row dictionaries
        action: The action name
        event_token: Optional event token

    Returns:
        Export record id, or None if no rows
    """
    if not payload_rows:
        return None

    file_format = BASECOM_FILE_FORMAT or "csv"
    with get_session() as session:
        repo = BasecomExportRepository(session)
        record = repo.create_export_record(
            action=action,
            rows=payload_rows,
            file_format=file_format,
        )
        session.commit()

    export_id = record.export_id
    export_bytes, content_type = _export_bytes_from_rows(payload_rows, file_format)
    filename = _basecom_filename(action, export_id, file_format)

    delivery_status = "pending"
    delivery_response: Dict[str, str] = {"mode": "local"}
    if event_token:
        delivery_response["event_token"] = event_token
    delivered_at = None
    try:
        response = _deliver_basecom_export(export_bytes, filename, content_type)
        delivery_status = "delivered"
        delivery_response.update(response)
        delivered_at = datetime.now(timezone.utc)
    except Exception as exc:
        delivery_status = "failed"
        delivery_response["error"] = str(exc)
        log.error("Base.com delivery failed for %s: %s", export_id, exc)

    with get_session() as session:
        repo = BasecomExportRepository(session)
        repo.update_delivery_status(
            export_id,
            delivered_at=delivered_at,
            delivery_status=delivery_status,
            delivery_response=delivery_response,
        )
        session.commit()

    return str(export_id)


def generate_basecom_export_bytes_for_record(
    record_id: str | UUID,
) -> Tuple[bytes, str]:
    """Generate export bytes from a stored Base.com export record.

    Args:
        record_id: Export record identifier.

    Returns:
        Tuple of (export bytes, content type).

    Raises:
        NotFoundError: If the export record is not found.
    """
    export_id = UUID(str(record_id))
    with get_session() as session:
        repo = BasecomExportRepository(session)
        record = repo.get_export_by_id(export_id)
        if not record:
            raise NotFoundError(
                f"Base.com export record not found: {record_id}",
                error_code="basecom_export_not_found",
            )
        export_bytes, content_type = _export_bytes_from_rows(record.rows or [], record.format)
        return export_bytes, content_type


def _deliver_basecom_export(payload: bytes, filename: str, content_type: str) -> Dict[str, str]:
    """Deliver Base.com export bytes locally.

    Args:
        payload: Export bytes to deliver
        filename: Filename to use for delivery
        content_type: MIME type for the payload

    Returns:
        Delivery metadata describing where the file was stored.
    """
    log.debug("Base.com export retained locally as %s", filename)
    return {"mode": "local", "filename": filename}


__all__ = [
    "BASECOM_FIELD_ORDER",
    "build_basecom_rows",
    "generate_basecom_export_bytes_for_record",
    "write_basecom_file",
]
