"""Environment-only configuration for the webhook service."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    bl_api_token: str
    bl_allowed_passes: set[str]
    discogs_token: str
    discogs_ua: str
    discogs_base: str
    app_database_url: str
    bl_inventory_id: str
    bl_shop_id: str
    bl_warehouse_id: str
    bl_price_group_id: str
    exchange_dir: str
    baselinker_reqs_per_min: int
    discogs_reqs_per_min: int
    baselinker_http_timeout: float
    rate_limiter_redis_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv(override=False)
        allowed = {
            value.strip()
            for value in os.getenv("BL_ALLOWED_PASSES", "").split(",")
            if value.strip()
        }
        if secret := os.getenv("BL_PASS", "").strip():
            allowed.add(secret)
        return cls(
            bl_api_token=os.getenv("BL_API_TOKEN", ""),
            bl_allowed_passes=allowed,
            discogs_token=os.getenv("DISCOGS_TOKEN", ""),
            discogs_ua=(
                os.getenv("DISCOGS_UA")
                or os.getenv("DISCOGS_USER_AGENT")
                or "BaseLinker-Webhooks/1.0"
            ),
            discogs_base=os.getenv("DISCOGS_BASE", "https://api.discogs.com"),
            app_database_url=os.getenv(
                "APP_DATABASE_URL",
                "postgresql+psycopg2://exchange:exchange@localhost:5434/exchange",
            ),
            bl_inventory_id=os.getenv("BL_INVENTORY_ID", ""),
            bl_shop_id=os.getenv("BL_SHOP_ID", ""),
            bl_warehouse_id=os.getenv("BL_WAREHOUSE_ID", "bl_1"),
            bl_price_group_id=os.getenv("BL_PRICE_GROUP_ID", "1"),
            exchange_dir=os.getenv("EXCHANGE_DIR", "./data/exchange"),
            baselinker_reqs_per_min=_int("BASELINKER_REQS_PER_MIN", 100),
            discogs_reqs_per_min=_int("DISCOGS_REQS_PER_MIN", 60),
            baselinker_http_timeout=float(
                os.getenv("BASELINKER_HTTP_TIMEOUT")
                or os.getenv("BL_HTTP_TIMEOUT")
                or "60"
            ),
            rate_limiter_redis_url=os.getenv("RATE_LIMITER_REDIS_URL", ""),
        )
