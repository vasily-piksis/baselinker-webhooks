"""Stateless handlers for Exchange order callbacks."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from exchange.app.handlers.utils import parse_ids
from exchange.clients.discogs_client import DiscogsClient
from exchange.delivery_methods import discogs_tracking_message
from exchange.order_statuses import discogs_order_next_statuses, discogs_order_status_text
from exchange.translation import baselinker_order_to_discogs_update

log = logging.getLogger("exchange.app.handlers.order")


def _order_update_ids(payload: Dict[str, Any]) -> List[str]:
    ids = parse_ids(payload.get("order_ids") or payload.get("orders_ids"))
    if ids:
        return ids
    return parse_ids(
        payload.get("order_id")
        or payload.get("id")
        or payload.get("order_source_id")
        or payload.get("discogs_id")
    )


def _prepare_status_edit(
    client: DiscogsClient, order_id: str, edit_fields: Dict[str, Any]
) -> tuple[Dict[str, Any], bool]:
    desired_status = discogs_order_status_text(edit_fields.get("status"))
    if not desired_status:
        return edit_fields, False

    prepared = dict(edit_fields)
    prepared["status"] = desired_status
    order = client.get_order(order_id)
    current_status = discogs_order_status_text(order.get("status"))
    if current_status == desired_status:
        prepared.pop("status", None)
        return prepared, True

    next_statuses = discogs_order_next_statuses(order)
    if next_statuses and desired_status not in next_statuses:
        raise ValueError(
            f"Discogs status {desired_status!r} is not allowed for order {order_id}; "
            f"current={current_status!r}; next_status={sorted(next_statuses)!r}"
        )
    return prepared, False


def sync_order_update_to_discogs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Apply a BaseLinker ``OrderUpdate`` directly to Discogs.

    There is intentionally no inbox table or Airflow trigger here.  The
    BaseLinker order ID is the Discogs marketplace order ID, so the update can
    be processed synchronously by this service.
    """
    update_type = str(payload.get("update_type") or "").strip().lower()
    if update_type not in {"status", "paid", "delivery_number"}:
        return {"status": "SKIPPED", "reason": "unsupported_update_type", "processed": 0, "failed": []}
    if update_type == "paid" and str(payload.get("update_value") or "").strip().lower() not in {"1", "true", "yes", "paid"}:
        return {"status": "SKIPPED", "reason": "unpaid_update_not_mapped", "processed": 0, "failed": []}

    order_ids = _order_update_ids(payload)
    if not order_ids:
        return {"status": "SKIPPED", "reason": "missing_order_ids", "processed": 0, "failed": []}

    client = DiscogsClient()
    processed = 0
    failed: List[str] = []
    for order_id in order_ids:
        order_payload = dict(payload, order_source="discogs", order_source_id=order_id)
        update = baselinker_order_to_discogs_update(order_payload)
        if not update:
            failed.append(f"{order_id}:empty_update")
            continue
        edit_fields = {
            key: value
            for key, value in (update.get("update_fields") or {}).items()
            if key in {"status", "shipping", "tracking"} and value not in (None, "")
        }
        message = update.get("message")
        if update_type == "delivery_number":
            edit_fields = {}
            message = discogs_tracking_message(payload)
        try:
            accepted_noop = False
            if edit_fields.get("status"):
                edit_fields, accepted_noop = _prepare_status_edit(client, order_id, edit_fields)
            changed = False
            if edit_fields:
                client.edit_order(order_id, **edit_fields)
                changed = True
            if message and update_type == "delivery_number":
                client.add_order_message(order_id, message, status=None)
                changed = True
            if changed or accepted_noop:
                processed += 1
        except Exception as exc:  # pragma: no cover - network failures
            failed.append(f"{order_id}:{exc}")
            log.error("Failed to sync BaseLinker OrderUpdate to Discogs %s: %s", order_id, exc)
    return {
        "status": "OK" if not failed else "PARTIAL" if processed else "ERROR",
        "processed": processed,
        "failed": failed,
        "order_ids": order_ids,
    }
