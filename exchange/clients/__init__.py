"""Environment-only shared API client helpers."""

from __future__ import annotations

import os
from typing import Optional


def _resolve_secret(
    env_keys: tuple[str, ...],
    *,
    variable_key: Optional[str] = None,
    conn_id: Optional[str] = None,
    conn_field: str = "password",
) -> Optional[str]:
    """Return the first configured environment value.

    Compatibility keyword arguments are deliberately ignored: this service
    reads secrets exclusively from environment variables.
    """
    del variable_key, conn_id, conn_field
    for key in env_keys:
        if value := os.getenv(key):
            return value
    return None


from exchange.utils import resolve_bool as _resolve_bool

__all__ = ["_resolve_secret", "_resolve_bool"]
