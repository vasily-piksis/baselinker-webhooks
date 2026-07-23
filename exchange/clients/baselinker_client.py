"""BaseLinker API client with retry and rate limiting."""

from __future__ import annotations

import atexit
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, cast

import httpx
from exchange.errors import BaseLinkerAPIError, RateLimitError
from exchange.retry import rate_limit_retry, standard_retry

from exchange.clients import _resolve_bool, _resolve_secret
from exchange.utils.rate_limiter import RateLimiter

log = logging.getLogger("exchange.clients.baselinker")
_RATE_LIMITER_LOCK = threading.Lock()
_RATE_LIMITERS: Dict[tuple[int, str], RateLimiter] = {}
_SUMMARY_REDACTED = "<redacted>"
_SENSITIVE_KEYS = {
    "token",
    "password",
    "pass",
    "bl_pass",
    "email",
    "phone",
    "telephone",
}
_SCALAR_SUMMARY_KEYS = {
    "inventory_id",
    "product_id",
    "order_id",
    "status_id",
    "page",
    "filter_id",
    "filter_locations",
    "warehouse_id",
    "date_from",
    "date_confirmed_from",
    "shop_order_id",
}


def _create_http_client(timeout: float) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout, connect=5.0, read=timeout),
        http2=False,
    )


def _shared_rate_limiter(requests_per_min: int, limiter_key: str) -> RateLimiter:
    with _RATE_LIMITER_LOCK:
        cache_key = (requests_per_min, limiter_key)
        limiter = _RATE_LIMITERS.get(cache_key)
        if limiter is None:
            limiter = RateLimiter(requests_per_min, key=limiter_key)
            _RATE_LIMITERS[cache_key] = limiter
        return limiter


def _blocked_retry_after(error_message: str) -> float | None:
    marker = "blocked until "
    lowered = error_message.lower()
    idx = lowered.find(marker)
    if idx < 0:
        return None
    raw_value = error_message[idx + len(marker):].strip()
    try:
        blocked_until = datetime.strptime(raw_value, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    retry_after = blocked_until.timestamp() - time.time()
    return retry_after if retry_after > 0 else None


def _summarize_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 2:
        if isinstance(value, dict):
            return f"<dict:{len(value)}>"
        if isinstance(value, list):
            return f"<list:{len(value)}>"
        return repr(value)
    if isinstance(value, dict):
        summary: Dict[str, Any] = {}
        for key, inner in list(value.items())[:5]:
            key_str = str(key)
            if key_str.lower() in _SENSITIVE_KEYS:
                summary[key_str] = _SUMMARY_REDACTED
                continue
            if isinstance(inner, (str, int, float, bool)) or inner is None:
                summary[key_str] = inner
            elif isinstance(inner, list):
                summary[key_str] = f"<list:{len(inner)}>"
            elif isinstance(inner, dict):
                summary[key_str] = f"<dict:{len(inner)}>"
            else:
                summary[key_str] = type(inner).__name__
        if len(value) > 5:
            summary["..."] = f"+{len(value) - 5} keys"
        return summary
    if isinstance(value, list):
        preview = [_summarize_value(item, depth=depth + 1) for item in value[:3]]
        if len(value) > 3:
            preview.append(f"... +{len(value) - 3} more")
        return preview
    return value


def _summarize_request(method: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"method": method, "keys": sorted(parameters.keys())}
    for key in _SCALAR_SUMMARY_KEYS:
        value = parameters.get(key)
        if isinstance(value, (str, int, float, bool)) and value not in ("", None):
            summary[key] = value

    for key in ("external_id", "sku"):
        value = parameters.get(key)
        if isinstance(value, str) and value.strip():
            summary[key] = value.strip()
    for key in ("filter_sku", "filter_ean", "filter_name"):
        value = parameters.get(key)
        if isinstance(value, str) and value.strip():
            summary[key] = value.strip()

    products = parameters.get("products")
    if isinstance(products, dict):
        summary["products_count"] = len(products)
        summary["products_preview"] = list(products.keys())[:3]
    elif isinstance(products, list):
        summary["products_count"] = len(products)
        summary["products_preview"] = [
            item for item in products[:3] if isinstance(item, (str, int, float))
        ]

    links = parameters.get("links")
    if isinstance(links, dict):
        summary["link_channels"] = sorted(str(key) for key in links.keys())[:5]

    order_items = parameters.get("products") or parameters.get("items")
    if isinstance(order_items, list):
        summary["items_count"] = len(order_items)

    if method == "addOrder":
        if "email" in parameters:
            summary["email"] = _SUMMARY_REDACTED
        if "phone" in parameters:
            summary["phone"] = _SUMMARY_REDACTED

    if method == "addInventoryProduct":
        for key in ("text_fields", "images", "features", "stock", "prices"):
            value = parameters.get(key)
            if value is not None:
                summary[key] = _summarize_value(value)

    return summary


def _summarize_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"keys": sorted(payload.keys())[:12]}
    for key in ("status", "product_id", "order_id", "inventory_id", "listing_id", "page"):
        value = payload.get(key)
        if isinstance(value, (str, int, float, bool)) and value not in ("", None):
            summary[key] = value
    products = payload.get("products")
    if isinstance(products, dict):
        summary["products_count"] = len(products)
    elif isinstance(products, list):
        summary["products_count"] = len(products)
    orders = payload.get("orders")
    if isinstance(orders, list):
        summary["orders_count"] = len(orders)
    return summary


class BaseLinkerClient:
    """Client for the BaseLinker connector API."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        use_form_token: Optional[bool] = None,
        requests_per_min: Optional[int] = None,
        http_timeout: Optional[float] = None,
        export_add_url: Optional[str] = None,
        export_update_url: Optional[str] = None,
        connection_id: Optional[str] = None,
    ) -> None:
        """Initialize the client.

        Args:
            base_url: BaseLinker API base URL.
            token: API token for authentication.
            use_form_token: Whether to send token as form field.
            requests_per_min: Rate limit in requests per minute.
            http_timeout: Timeout in seconds for HTTP calls.
            export_add_url: Optional export CSV URL override for new products.
            export_update_url: Optional inventory update/change export URL.
            connection_id: Unused compatibility identifier.

        Raises:
            BaseLinkerAPIError: If no API token is configured.
        """
        conn_id = connection_id or os.getenv("BASELINKER_CONN_ID") or "baselinker_default"
        self.base_url = (
            base_url
            or os.getenv("BL_API_URL")
            or os.getenv("BASELINKER_API")
            or "https://api.baselinker.com/connector.php"
        ).rstrip("/")
        self.token = token or _resolve_secret(
            ("BL_API_TOKEN", "BASELINKER_TOKEN"),
            variable_key="BL_API_TOKEN",
            conn_id=conn_id,
            conn_field="password",
        )
        if not self.token:
            raise BaseLinkerAPIError(
                "BaseLinker token missing; set BL_API_TOKEN or BASELINKER_TOKEN.",
                error_code="baselinker_not_configured",
                http_status=503,
            )
        self.use_form_token = _resolve_bool(
            (use_form_token if use_form_token is not None else os.getenv("BL_USE_FORM_TOKEN")),
            default=True,
        )
        rpm = requests_per_min or int(os.getenv("BASELINKER_REQS_PER_MIN", "100"))
        timeout_val = http_timeout or float(
            os.getenv("BASELINKER_HTTP_TIMEOUT") or os.getenv("BL_HTTP_TIMEOUT") or "60"
        )
        self._http_timeout = timeout_val
        self._http_client = _create_http_client(timeout_val)
        self._closed = False
        atexit.register(self.close)
        limiter_hash = hashlib.sha256(f"{self.base_url}|{self.token}".encode("utf-8")).hexdigest()[:16]
        self._rl = _shared_rate_limiter(rpm, f"baselinker:{limiter_hash}")
        self.export_add_url = export_add_url or _resolve_secret(
            ("BASELINKER_EXPORT_ADD_URL",),
            variable_key="BASELINKER_EXPORT_ADD_URL",
            conn_id=os.getenv("BASELINKER_EXPORT_CONN_ID") or conn_id,
            conn_field="host",
        )
        self.export_update_url = export_update_url or _resolve_secret(
            ("BASELINKER_EXPORT_UPDATE_URL",),
            variable_key="BASELINKER_EXPORT_UPDATE_URL",
            conn_id=os.getenv("BASELINKER_EXPORT_CONN_ID") or conn_id,
            conn_field="host",
        )

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        if self._closed:
            return
        self._closed = True
        self._http_client.close()
        try:
            atexit.unregister(self.close)
        except ValueError:
            pass

    def __enter__(self) -> "BaseLinkerClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "BL-Discogs-Bridge/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if not self.use_form_token and self.token:
            headers["X-BLToken"] = self.token
        return headers

    def _payload(self, method: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "method": method,
            "parameters": json.dumps(parameters, ensure_ascii=False),
        }

    def _handle_http_errors(self, resp: httpx.Response) -> None:
        if resp.status_code in (429, 503):
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    time.sleep(float(retry_after))
                except Exception:
                    pass
        if resp.status_code == 429:
            raise RateLimitError(
                "BaseLinker rate limit exceeded",
                error_code="rate_limit",
                http_status=429,
                context={"status": resp.status_code, "body": resp.text},
            )
        if resp.status_code >= 400:
            raise BaseLinkerAPIError(
                f"BaseLinker HTTP {resp.status_code}: {resp.text}",
                error_code="baselinker_http_error",
                http_status=502,
                context={"status": resp.status_code, "body": resp.text},
            )
        resp.raise_for_status()

    def _parse_response(self, method: str, payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {"response": payload}

        status = str(payload.get("status", "")).upper()
        if status and status not in {"SUCCESS", "OK"}:
            error_code = payload.get("error_code") or payload.get("code") or ""
            error_message = (
                payload.get("error_message")
                or payload.get("error")
                or payload.get("message")
                or ""
            )
            detail = " ".join(str(part).strip() for part in (error_code, error_message) if part)
            message = f"BaseLinker {method} failed"
            if detail:
                message = f"{message}: {detail}"
            retry_after = _blocked_retry_after(str(error_message))
            if error_code == "ERROR_BLOCKED_TOKEN" or "query limit exceeded" in str(
                error_message
            ).lower():
                raise RateLimitError(
                    message,
                    error_code="rate_limit",
                    http_status=429,
                    context={
                        "payload": payload,
                        "status": status,
                        "error_code": error_code,
                        "error_message": error_message,
                        "retry_after": retry_after,
                    },
                )
            raise BaseLinkerAPIError(
                message,
                error_code="baselinker_error",
                http_status=502,
                context={
                    "payload": payload,
                    "status": status,
                    "error_code": error_code,
                    "error_message": error_message,
                },
            )

        response = payload.get("response")
        if isinstance(response, dict):
            merged = {k: v for k, v in payload.items() if k != "response"}
            merged.update(response)
            return merged
        return payload

    @rate_limit_retry
    @standard_retry
    def _post(self, method: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = parameters or {}
        request_summary = _summarize_request(method, params)
        started = time.monotonic()
        log.info("BaseLinker request: %s", request_summary)
        self._rl.wait()
        data = self._payload(method, params)
        if self.use_form_token and self.token:
            data["token"] = self.token

        try:
            resp = self._http_client.post(
                self.base_url,
                headers=self._headers(),
                data=data,
                timeout=self._http_timeout,
            )
        except httpx.HTTPError as exc:
            raise BaseLinkerAPIError(
                f"BaseLinker {method} request failed",
                error_code="baselinker_connection_error",
                http_status=503,
                context={"error": str(exc)},
            ) from exc

        self._handle_http_errors(resp)
        duration_ms = int((time.monotonic() - started) * 1000)
        try:
            payload = resp.json()
        except Exception:
            log.exception("Non-JSON response from BaseLinker for %s", method)
            resp.raise_for_status()
            return {}
        parsed = self._parse_response(method, payload)
        log.info(
            "BaseLinker response: method=%s http_status=%s duration_ms=%s summary=%s",
            method,
            resp.status_code,
            duration_ms,
            _summarize_response(parsed),
        )
        return parsed

    # --- Public helpers --------------------------------------------------
    def get_orders(
        self,
        date_from: Optional[int] = None,
        date_confirmed_from: Optional[int] = None,
        status_id: Optional[int] = None,
        page: int = 1,
        get_unconfirmed: bool = True,
    ) -> Dict[str, Any]:
        """Fetch orders from BaseLinker.

        Args:
            date_from: Unix timestamp to filter orders by creation date.
            date_confirmed_from: Unix timestamp to filter by confirmation date.
            status_id: Optional BaseLinker status id.
            page: Page number to fetch.
            get_unconfirmed: Whether to include unconfirmed orders.

        Returns:
            Response payload from BaseLinker.
        """
        params: Dict[str, Any] = {"page": int(page)}
        if date_confirmed_from is not None:
            params["date_confirmed_from"] = int(date_confirmed_from)
        elif date_from is not None:
            params["date_from"] = int(date_from)
        if status_id is not None:
            params["status_id"] = int(status_id)
        if get_unconfirmed:
            params["get_unconfirmed_orders"] = 1
        return cast(Dict[str, Any], self._post("getOrders", params))

    def add_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new BaseLinker order.

        Args:
            order_data: BaseLinker order payload.

        Returns:
            Response payload from BaseLinker.
        """
        return cast(Dict[str, Any], self._post("addOrder", order_data))

    def set_order_status(self, order_id: int, status_id: int) -> Dict[str, Any]:
        """Update an order status in BaseLinker.

        Args:
            order_id: BaseLinker order id.
            status_id: Status id to set.

        Returns:
            Response payload from BaseLinker.
        """
        return cast(
            Dict[str, Any],
            self._post(
                "setOrderStatus",
                {"order_id": int(order_id), "status_id": int(status_id)},
            ),
        )

    def get_order_status_list(self) -> Dict[str, Any]:
        """Return the BaseLinker order status list.

        Returns:
            Response payload containing available statuses.
        """
        return cast(Dict[str, Any], self._post("getOrderStatusList", {}))

    def get_inventory_products_list(
        self,
        inventory_id: int,
        page: int = 1,
        *,
        filter_id: Optional[int | str] = None,
        filter_sku: Optional[str] = None,
        filter_locations: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        """Fetch inventory product list.

        Args:
            inventory_id: Catalog ID (required by BaseLinker API).
            page: Page number to fetch.
            filter_id: Optional exact BaseLinker product id filter.
            filter_sku: Optional exact SKU filter.
            filter_locations: Optional location filter.

        Returns:
            Response payload from BaseLinker.
        """
        params: Dict[str, Any] = {
            "inventory_id": int(inventory_id),
            "page": int(page),
        }
        if filter_id not in (None, ""):
            params["filter_id"] = int(filter_id)
        if isinstance(filter_sku, str) and filter_sku.strip():
            params["filter_sku"] = filter_sku.strip()
        if filter_locations not in (None, ""):
            params["filter_locations"] = str(filter_locations).strip()
        return cast(Dict[str, Any], self._post("getInventoryProductsList", params))

    def get_inventory_products_data(
        self, inventory_id: int, products: list[int | str]
    ) -> Dict[str, Any]:
        """Fetch detailed inventory product data.

        Args:
            inventory_id: Catalog ID (required by BaseLinker API).
            products: BaseLinker product identifiers to load.

        Returns:
            Response payload from BaseLinker.
        """
        params: Dict[str, Any] = {
            "inventory_id": int(inventory_id),
            "products": list(products or []),
        }
        return cast(Dict[str, Any], self._post("getInventoryProductsData", params))

    def get_inventory_products_stock(
        self,
        inventory_id: int,
        page: int = 1,
        *,
        filter_id: Optional[int | str] = None,
        filter_sku: Optional[str] = None,
        filter_locations: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        """Fetch inventory product stock levels.

        Args:
            inventory_id: Catalog ID (required by BaseLinker API).
            page: Page number to fetch.
            filter_id: Optional exact BaseLinker product id filter.
            filter_sku: Optional exact SKU filter.
            filter_locations: Optional location filter.

        Returns:
            Response payload from BaseLinker.
        """
        params: Dict[str, Any] = {
            "inventory_id": int(inventory_id),
            "page": int(page),
        }
        if filter_id not in (None, ""):
            params["filter_id"] = int(filter_id)
        if isinstance(filter_sku, str) and filter_sku.strip():
            params["filter_sku"] = filter_sku.strip()
        if filter_locations not in (None, ""):
            params["filter_locations"] = str(filter_locations).strip()
        return cast(Dict[str, Any], self._post("getInventoryProductsStock", params))

    def update_inventory_products_stock(
        self, inventory_id: int, products: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update stock quantities for inventory products.

        Args:
            inventory_id: Catalog ID (required by BaseLinker API).
            products: Mapping of product ids to warehouse stock updates.

        Returns:
            Response payload from BaseLinker.
        """
        return cast(
            Dict[str, Any],
            self._post(
                "updateInventoryProductsStock",
                {"inventory_id": int(inventory_id), "products": products},
            ),
        )

    def add_inventory_product(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Add a product to BaseLinker inventory.

        Args:
            product: Product payload to add.

        Returns:
            Response payload from BaseLinker.
        """
        return cast(Dict[str, Any], self._post("addInventoryProduct", product))

    def get_export_add_file(
        self, export_url: Optional[str] = None, *, dest_path: Optional[Path] = None
    ) -> str:
        """Download the configured add/new-products export CSV and return the local path.

        Args:
            export_url: Optional override export URL.
            dest_path: Optional destination file path.

        Returns:
            Path to the downloaded CSV file.

        Raises:
            BaseLinkerAPIError: If no export URL is configured.
        """
        url = export_url or self.export_add_url
        if not url:
            raise BaseLinkerAPIError(
                "BaseLinker export add URL is not configured "
                "(BASELINKER_EXPORT_ADD_URL)",
                error_code="baselinker_export_missing",
                http_status=503,
            )
        target = dest_path or Path(f"/tmp/bl_export_{int(time.time())}.csv")
        resp = httpx.get(url, timeout=self._http_timeout)
        resp.raise_for_status()
        target.write_bytes(resp.content)
        return str(target)

    def get_export_update_file(
        self, export_url: Optional[str] = None, *, dest_path: Optional[Path] = None
    ) -> str:
        """Download the inventory update/change export CSV and return the local path.

        Args:
            export_url: Optional override export URL.
            dest_path: Optional destination file path.

        Returns:
            Path to the downloaded CSV file.

        Raises:
            BaseLinkerAPIError: If no export update URL is configured.
        """
        url = export_url or self.export_update_url
        if not url:
            raise BaseLinkerAPIError(
                "BaseLinker export update URL is not configured "
                "(BASELINKER_EXPORT_UPDATE_URL)",
                error_code="baselinker_export_update_missing",
                http_status=503,
            )
        target = dest_path or Path(f"/tmp/bl_export_update_{int(time.time())}.csv")
        resp = httpx.get(url, timeout=self._http_timeout)
        resp.raise_for_status()
        target.write_bytes(resp.content)
        return str(target)


_client: Optional[BaseLinkerClient] = None


def _get_client() -> BaseLinkerClient:
    """Return a cached BaseLinker client instance.

    Returns:
        BaseLinkerClient singleton instance.
    """
    global _client
    if _client is None:
        _client = BaseLinkerClient()
    return _client


# Names that map directly to BaseLinkerClient methods (used by __getattr__)
_CLIENT_METHODS = {
    "get_orders",
    "add_order",
    "set_order_status",
    "get_order_status_list",
    "get_inventory_products_list",
    "get_inventory_products_data",
    "get_inventory_products_stock",
    "update_inventory_products_stock",
    "add_inventory_product",
    "get_export_add_file",
    "get_export_update_file",
}


def __getattr__(name: str) -> Any:
    """Lazy proxy: module-level access to BaseLinkerClient singleton methods."""
    if name in _CLIENT_METHODS:
        return getattr(_get_client(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BaseLinkerClient",
    "_get_client",
    "get_orders",
    "add_order",
    "set_order_status",
    "get_order_status_list",
    "get_inventory_products_list",
    "get_inventory_products_data",
    "get_inventory_products_stock",
    "update_inventory_products_stock",
    "add_inventory_product",
    "get_export_add_file",
    "get_export_update_file",
]
