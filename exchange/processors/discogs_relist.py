from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from exchange.processors.order_mapping_grace import load_order_mapping_grace


def execute_discogs_relist_flow(
    *,
    create_payload: Dict[str, Any],
    create_listing_fn: Callable[..., Dict[str, Any]],
    link_back_fn: Callable[[str], Dict[str, Any]],
    reuse_listing_id: str = "",
    old_listing_id: str = "",
    old_listing_status: str = "",
    delete_old_listing_fn: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    delete_requires_link_update: bool = True,
    respect_order_mapping_grace: bool = True,
) -> Dict[str, Any]:
    reused_listing_id = str(reuse_listing_id or "").strip()
    if reused_listing_id:
        create_result = {
            "listing_id": reused_listing_id,
            "status": "reused_recent",
        }
    else:
        create_result = create_listing_fn(**create_payload)
    new_listing_id = str(create_result.get("listing_id") or "").strip()
    if not new_listing_id:
        raise ValueError("Discogs create_listing did not return listing_id")

    link_update = link_back_fn(new_listing_id)
    link_status = str(link_update.get("status") or "").strip().lower()
    link_confirmed = link_status in {"updated", "noop", "skipped", "success"}

    delete_result = None
    delete_skipped_reason = ""
    retired_listing_id = str(old_listing_id or "").strip()
    retired_status = str(old_listing_status or "").strip().lower()
    if (
        delete_old_listing_fn
        and retired_listing_id
        and retired_listing_id != "0"
        and retired_listing_id != new_listing_id
        and retired_status == "sold"
    ):
        grace_payload = (
            load_order_mapping_grace(retired_listing_id)
            if respect_order_mapping_grace
            else None
        )
        if grace_payload:
            delete_result = {
                "status": "skipped",
                "listing_id": retired_listing_id,
                "reason": "order_mapping_grace",
                "grace": grace_payload,
            }
        elif delete_requires_link_update and not link_confirmed:
            delete_skipped_reason = "link_update_not_confirmed"
        else:
            delete_result = delete_old_listing_fn(retired_listing_id)

    return {
        "listing_id": new_listing_id,
        "discogs_create": create_result,
        "bl_update": link_update,
        "discogs_delete": delete_result,
        "delete_skipped_reason": delete_skipped_reason,
    }


__all__ = ["execute_discogs_relist_flow"]
