"""Mappings between BaseLinker status ids and names."""

from __future__ import annotations

from typing import Optional

from exchange.settings import BL_STATUS_NAME_MAP


def status_name_for(status_id: Optional[int]) -> Optional[str]:
    """Return the status name for a BaseLinker status id.

    Args:
        status_id: BaseLinker status identifier.

    Returns:
        Status name string if known, otherwise None.
    """
    if status_id is None:
        return None
    key = str(status_id)
    return BL_STATUS_NAME_MAP.get(key)


__all__ = ["status_name_for"]
