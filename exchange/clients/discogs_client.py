"""Discogs API client and convenience wrappers."""

from __future__ import annotations

import atexit
import csv
import hashlib
import io
import logging
import os
import threading
import time
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, Optional, cast

import httpx
from exchange.errors import DiscogsAPIError, RateLimitError, ValidationError
from exchange.retry import rate_limit_retry, standard_retry

from exchange.clients import _resolve_secret
from exchange.utils.rate_limiter import RateLimiter

log = logging.getLogger("exchange.clients.discogs")
_RATE_LIMITER_LOCK = threading.Lock()
_RATE_LIMITERS: Dict[tuple[int, str], RateLimiter] = {}
_RATE_LIMIT_HEADERS = (
    "Retry-After",
    "X-Discogs-Ratelimit",
    "X-Discogs-Ratelimit-Used",
    "X-Discogs-Ratelimit-Remaining",
)
_RATE_LIMIT_FALLBACK_COOLDOWN = float(os.getenv("DISCOGS_RATE_LIMIT_FALLBACK_COOLDOWN", "5"))


def _create_http_client(timeout: httpx.Timeout) -> httpx.Client:
    return httpx.Client(timeout=timeout, http2=False)


def _shared_rate_limiter(requests_per_min: int, limiter_key: str) -> RateLimiter:
    with _RATE_LIMITER_LOCK:
        cache_key = (requests_per_min, limiter_key)
        limiter = _RATE_LIMITERS.get(cache_key)
        if limiter is None:
            limiter = RateLimiter(requests_per_min, key=limiter_key)
            _RATE_LIMITERS[cache_key] = limiter
        return limiter


def _parse_retry_after(raw_value: Optional[str]) -> float | None:
    if not raw_value:
        return None
    try:
        retry_after = float(raw_value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        retry_after = retry_at.timestamp() - time.time()
    return retry_after if retry_after > 0 else None


def _rate_limit_context(response: httpx.Response) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "status": response.status_code,
        "body": response.text,
    }
    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
    if retry_after is not None:
        context["retry_after"] = retry_after
    headers = {
        header_name: response.headers[header_name]
        for header_name in _RATE_LIMIT_HEADERS
        if header_name in response.headers
    }
    if headers:
        context["headers"] = headers
    return context


def _rate_limit_cooldown_seconds(context: Dict[str, Any]) -> float | None:
    retry_after = context.get("retry_after")
    if isinstance(retry_after, (int, float)) and retry_after > 0:
        return float(retry_after)
    remaining = None
    headers = context.get("headers")
    if isinstance(headers, dict):
        remaining = headers.get("X-Discogs-Ratelimit-Remaining")
    if remaining == "0" and _RATE_LIMIT_FALLBACK_COOLDOWN > 0:
        return _RATE_LIMIT_FALLBACK_COOLDOWN
    return None


class DiscogsClient:
    """
    Wrapper around Discogs marketplace endpoints with rate limiting,
    circuit breaker, and CSV upload helpers.
    """

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        user_agent: Optional[str] = None,
        requests_per_min: Optional[int] = None,
        connection_id: Optional[str] = None,
        http_timeout: Optional[float] = None,
    ) -> None:
        """Initialize the Discogs API client.

        Args:
            base_url: Base Discogs API URL.
            token: OAuth token for Discogs API.
            user_agent: User agent string for Discogs requests.
            requests_per_min: Rate limit in requests per minute.
            connection_id: Unused compatibility identifier.
            http_timeout: Request timeout in seconds.

        Raises:
            DiscogsAPIError: If no Discogs token is configured.
        """
        conn_id = connection_id or os.getenv("DISCOGS_CONN_ID") or "discogs_default"
        self.base_url = (base_url or os.getenv("DISCOGS_BASE") or "https://api.discogs.com").rstrip(
            "/"
        )
        self.token = token or _resolve_secret(
            ("DISCOGS_TOKEN",),
            variable_key="DISCOGS_TOKEN",
            conn_id=conn_id,
            conn_field="password",
        )
        if not self.token:
            raise DiscogsAPIError(
                "Discogs token missing; set DISCOGS_TOKEN.",
                error_code="discogs_not_configured",
                http_status=503,
            )
        self.user_agent = (
            user_agent
            or os.getenv("DISCOGS_USER_AGENT")
            or os.getenv("DISCOGS_UA")
            or "BL-Discogs-Bridge/1.0 (+integration)"
        )
        self.requests_per_min = int(requests_per_min or os.getenv("DISCOGS_REQS_PER_MIN") or 60)
        self._timeout = httpx.Timeout(http_timeout or 10.0, connect=5.0, read=http_timeout or 10.0)
        self._http_client = _create_http_client(self._timeout)
        atexit.register(self._http_client.close)
        limiter_hash = hashlib.sha256(f"{self.base_url}|{self.token}".encode("utf-8")).hexdigest()[:16]
        self._rl = _shared_rate_limiter(self.requests_per_min, f"discogs:{limiter_hash}")
        self._username: Optional[str] = os.getenv("DISCOGS_USERNAME") or None
        self._breaker_threshold = int(os.getenv("DISCOGS_BREAKER_THRESHOLD", "4"))
        self._breaker_reset = int(os.getenv("DISCOGS_BREAKER_RESET", "30"))
        self._breaker_failures = 0
        self._breaker_open_until = 0.0

    def _ensure_username(self) -> None:
        if self._username:
            return
        try:
            data = self.identity()
            self._username = data.get("username")
        except Exception:
            self._username = None

    def _breaker_open(self) -> bool:
        return bool(self._breaker_open_until and time.time() < self._breaker_open_until)

    def _record_failure(self) -> None:
        self._breaker_failures += 1
        if self._breaker_failures >= self._breaker_threshold:
            self._breaker_open_until = time.time() + self._breaker_reset
            log.warning("Discogs circuit breaker open for %s seconds", self._breaker_reset)

    def _reset_breaker(self) -> None:
        self._breaker_failures = 0
        self._breaker_open_until = 0.0

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "User-Agent": self.user_agent,
            "Authorization": f"Discogs token={self.token}",
            "Accept": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    @rate_limit_retry
    @standard_retry
    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        if self._breaker_open():
            raise DiscogsAPIError(
                "Discogs circuit breaker is open",
                error_code="discogs_unavailable",
                http_status=503,
            )
        self._rl.wait()
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", self._timeout)
        headers = self._headers(kwargs.pop("headers", None))
        try:
            response = self._http_client.request(
                method, url, headers=headers, timeout=timeout, **kwargs
            )
        except httpx.HTTPError:
            self._record_failure()
            raise
        if response.status_code == 429:
            context = _rate_limit_context(response)
            cooldown = _rate_limit_cooldown_seconds(context)
            if cooldown is not None:
                self._rl.impose_cooldown(cooldown)
            raise RateLimitError(
                "Discogs rate limit exceeded",
                error_code="rate_limit",
                http_status=429,
                context=context,
            )
        if response.status_code >= 400:
            if response.status_code >= 500:
                self._record_failure()
            raise httpx.HTTPStatusError(
                f"Discogs error {response.status_code}: {response.text}",
                request=response.request,
                response=response,
            )
        self._reset_breaker()
        return response

    def identity(self) -> Dict[str, Any]:
        """Fetch the authenticated account identity.

        Returns:
            Identity payload from Discogs.
        """
        return cast(Dict[str, Any], self._request("GET", "/oauth/identity").json())

    def list_orders(
        self,
        status: Optional[str] = None,
        page: int = 1,
        per_page: int = 50,
        *,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        sort: Optional[str] = None,
        sort_order: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List marketplace orders.

        Args:
            status: Optional order status filter.
            page: Page number to fetch.
            per_page: Number of results per page (max 100).
            created_after: Optional lower created-at filter accepted by Discogs.
            created_before: Optional upper created-at filter accepted by Discogs.
            sort: Optional Discogs sort field.
            sort_order: Optional Discogs sort order.

        Returns:
            Response payload from Discogs.
        """
        params: Dict[str, Any] = {"page": page, "per_page": min(per_page, 100)}
        if status:
            params["status"] = status
        if created_after:
            params["created_after"] = created_after
        if created_before:
            params["created_before"] = created_before
        if sort:
            params["sort"] = sort
        if sort_order:
            params["sort_order"] = sort_order
        return cast(
            Dict[str, Any],
            self._request("GET", "/marketplace/orders", params=params).json(),
        )

    def get_order(self, order_id: int | str) -> Dict[str, Any]:
        """Fetch a single marketplace order.

        Args:
            order_id: Discogs order identifier.

        Returns:
            Response payload from Discogs.
        """
        return cast(
            Dict[str, Any],
            self._request("GET", f"/marketplace/orders/{order_id}").json(),
        )

    def edit_order(self, order_id: int | str, **fields: Any) -> Dict[str, Any]:
        """Update fields on a marketplace order.

        Args:
            order_id: Discogs order identifier.
            **fields: Fields to update on the order.

        Returns:
            Response payload from Discogs.
        """
        body = {k: v for k, v in fields.items() if v is not None}
        return cast(
            Dict[str, Any],
            self._request("POST", f"/marketplace/orders/{order_id}", json=body).json(),
        )

    def add_order_message(
        self, order_id: int | str, message: str, *, status: Optional[str] = None
    ) -> Dict[str, Any]:
        """Post a message to an order thread.

        Args:
            order_id: Discogs order identifier.
            message: Message body to post.
            status: Optional order status to include.

        Returns:
            Response payload from Discogs.
        """
        payload: Dict[str, Any] = {"message": message}
        if status:
            payload["status"] = status
        return cast(
            Dict[str, Any],
            self._request("POST", f"/marketplace/orders/{order_id}/messages", json=payload).json(),
        )

    def list_order_messages(self, order_id: int | str) -> Dict[str, Any]:
        """List messages for a marketplace order.

        Args:
            order_id: Discogs order identifier.

        Returns:
            Response payload from Discogs.
        """
        return cast(
            Dict[str, Any],
            self._request("GET", f"/marketplace/orders/{order_id}/messages").json(),
        )

    def search_release(
        self,
        query: Optional[str] = None,
        *,
        barcode: Optional[str] = None,
        catno: Optional[str] = None,
        artist: Optional[str] = None,
        title: Optional[str] = None,
        per_page: int = 5,
    ) -> Dict[str, Any]:
        """Search the Discogs release database.

        Args:
            query: Free-text query string.
            barcode: Optional barcode filter.
            catno: Optional catalog number filter.
            artist: Optional artist filter.
            title: Optional title filter.
            per_page: Results per page (max 100).

        Returns:
            Response payload from Discogs.
        """
        params: Dict[str, Any] = {"type": "release", "per_page": min(per_page, 100)}
        if query:
            params["q"] = query
        if barcode:
            params["barcode"] = barcode
        if catno:
            params["catno"] = catno
        if artist:
            params["artist"] = artist
        if title:
            params["title"] = title
        return cast(
            Dict[str, Any],
            self._request("GET", "/database/search", params=params).json(),
        )

    def get_release(self, release_id: int | str) -> Dict[str, Any]:
        """Fetch detailed Discogs release metadata.

        Args:
            release_id: Discogs release identifier.

        Returns:
            Release payload from Discogs.

        Raises:
            ValidationError: If release_id is empty.
        """
        release_id = str(release_id).strip()
        if not release_id:
            raise ValidationError("release_id is required", error_code="validation_error")
        return cast(
            Dict[str, Any],
            self._request("GET", f"/releases/{release_id}").json(),
        )

    def _inventory_csv_payload(
        self,
        rows: Iterable[Dict[str, Any]],
        field_order: Optional[Iterable[str]] = None,
    ) -> str:
        """Build a UTF-8 CSV string from iterable rows.

        Args:
            rows: Iterable of inventory row dictionaries.
            field_order: Optional explicit field ordering.

        Returns:
            CSV content as a string.

        Raises:
            ValidationError: If no rows are provided.
        """
        rows_list = list(rows)
        if not rows_list:
            raise ValidationError("rows cannot be empty", error_code="validation_error")

        if field_order:
            fieldnames = list(field_order)
        else:
            fieldnames = list(rows_list[0].keys())
            for row in rows_list[1:]:
                for key in row.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

        buf = io.StringIO(newline="")
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row)
        csv_text = buf.getvalue()
        buf.close()
        return csv_text

    @rate_limit_retry
    @standard_retry
    def upload_inventory_csv(
        self,
        action: str,
        rows: Iterable[Dict[str, Any]],
        field_order: Optional[Iterable[str]] = None,
        *,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload an inventory CSV payload to Discogs.

        Args:
            action: Inventory action ("add", "change", "delete").
            rows: Iterable of inventory rows.
            field_order: Optional explicit field ordering.
            idempotency_key: Optional idempotency key for upload.

        Returns:
            Response payload from Discogs.

        Raises:
            ValidationError: If action is invalid.

        Examples:
            >>> client = DiscogsClient()
            >>> rows = [{"release_id": 123, "price": "12.00", "quantity": 1}]
            >>> client.upload_inventory_csv("add", rows)
        """
        normalized = action.lower().strip()
        if normalized not in {"add", "change", "delete"}:
            raise ValidationError("action must be add|change|delete", error_code="validation_error")

        csv_text = self._inventory_csv_payload(rows, field_order)
        files = {
            "upload": ("inventory.csv", csv_text.encode("utf-8"), "text/csv"),
        }
        headers = {
            "Authorization": f"Discogs token={self.token}",
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        self._rl.wait()
        with httpx.Client(http2=False, timeout=60.0) as client:
            response = client.post(
                f"{self.base_url}/inventory/upload/{normalized}",
                headers=headers,
                files=files,
            )
            if response.status_code == 429:
                context = _rate_limit_context(response)
                cooldown = _rate_limit_cooldown_seconds(context)
                if cooldown is not None:
                    self._rl.impose_cooldown(cooldown)
                raise RateLimitError(
                    "Discogs rate limit exceeded",
                    error_code="rate_limit",
                    http_status=429,
                    context=context,
                )
            if response.status_code == 409:
                body = response.text[:1000]
                raise DiscogsAPIError(
                    f"Discogs inventory upload conflict for action={normalized}",
                    error_code="discogs_inventory_upload_conflict",
                    http_status=409,
                    context={
                        "status": 409,
                        "action": normalized,
                        "body": body,
                    },
                )

        log.debug(
            "Discogs inventory upload %s status=%s body=%s",
            normalized,
            response.status_code,
            response.text,
        )

        response.raise_for_status()
        return cast(Dict[str, Any], response.json() if response.text else {})

    def add_inventory(
        self,
        rows: Iterable[Dict[str, Any]],
        field_order: Optional[Iterable[str]] = None,
        *,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload inventory rows with the ``add`` action.

        Args:
            rows: Inventory row dictionaries.
            field_order: Optional explicit field ordering.
            idempotency_key: Optional idempotency key for upload.

        Returns:
            Response payload from Discogs.
        """
        return self.upload_inventory_csv("add", rows, field_order, idempotency_key=idempotency_key)

    def change_inventory(
        self,
        rows: Iterable[Dict[str, Any]],
        field_order: Optional[Iterable[str]] = None,
        *,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload inventory rows with the ``change`` action.

        Args:
            rows: Inventory row dictionaries.
            field_order: Optional explicit field ordering.
            idempotency_key: Optional idempotency key for upload.

        Returns:
            Response payload from Discogs.
        """
        return self.upload_inventory_csv(
            "change", rows, field_order, idempotency_key=idempotency_key
        )

    def delete_inventory(
        self,
        rows: Iterable[Dict[str, Any]],
        field_order: Optional[Iterable[str]] = None,
        *,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload inventory rows with the ``delete`` action.

        Args:
            rows: Inventory row dictionaries.
            field_order: Optional explicit field ordering.
            idempotency_key: Optional idempotency key for upload.

        Returns:
            Response payload from Discogs.
        """
        return self.upload_inventory_csv(
            "delete", rows, field_order, idempotency_key=idempotency_key
        )

    def list_inventory(
        self, page: int = 1, per_page: int = 25, status: Optional[str] = None
    ) -> Dict[str, Any]:
        """List inventory listings for the authenticated seller.

        Args:
            page: Page number to fetch.
            per_page: Number of listings per page.
            status: Optional listing status filter.

        Returns:
            Response payload from Discogs.

        Raises:
            DiscogsAPIError: If the Discogs username cannot be resolved.
        """
        self._ensure_username()
        if not self._username:
            raise DiscogsAPIError(
                "Unable to resolve Discogs username. Set DISCOGS_USERNAME or ensure "
                "the identity endpoint is accessible.",
                error_code="discogs_not_configured",
                http_status=503,
            )
        params: Dict[str, Any] = {
            "page": int(page),
            "per_page": min(int(per_page), 100),
        }
        if status:
            params["status"] = status
        response = self._request("GET", f"/users/{self._username}/inventory", params=params)
        data = cast(Dict[str, Any], response.json())
        listings = data.get("listings", [])
        log.debug("Fetched %s Discogs listings on page %s", len(listings), page)
        return data

    def get_listing(self, listing_id: int | str) -> Dict[str, Any]:
        """Fetch a single inventory listing.

        Args:
            listing_id: Discogs listing identifier.

        Returns:
            Response payload from Discogs.

        Raises:
            ValidationError: If listing_id is empty.
        """
        listing_id = str(listing_id).strip()
        if not listing_id:
            raise ValidationError("listing_id is required", error_code="validation_error")
        return cast(
            Dict[str, Any],
            self._request("GET", f"/marketplace/listings/{listing_id}").json(),
        )

    # --- Direct Listing API (single-item operations) ------------------------------

    @rate_limit_retry
    @standard_retry
    def create_listing(
        self,
        release_id: int,
        condition: str,
        price: float,
        *,
        sleeve_condition: Optional[str] = None,
        comments: Optional[str] = None,
        allow_offers: bool = False,
        status: str = "For Sale",
        external_id: Optional[str] = None,
        location: Optional[str] = None,
        weight: Optional[int] = None,
        format_quantity: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Create a new marketplace listing via direct API.

        Use this for single-item additions instead of CSV upload.

        Args:
            release_id: Discogs release ID.
            condition: Media condition (e.g., "Mint (M)", "Near Mint (NM or M-)").
            price: Listing price in seller's currency.
            sleeve_condition: Optional sleeve condition.
            comments: Optional comments visible to buyers.
            allow_offers: Whether to allow buyer offers.
            status: Listing status ("For Sale" or "Draft").
            external_id: Private reference ID (SKU).
            location: Private storage location.
            weight: Weight in grams.
            format_quantity: Number of items for shipping calculation.

        Returns:
            Response payload with listing_id and resource_url.
        """
        payload: Dict[str, Any] = {
            "release_id": release_id,
            "condition": condition,
            "price": price,
            "status": status,
            "allow_offers": allow_offers,
        }
        if sleeve_condition:
            payload["sleeve_condition"] = sleeve_condition
        if comments:
            payload["comments"] = comments
        if external_id:
            payload["external_id"] = external_id
        if location:
            payload["location"] = location
        if weight is not None:
            payload["weight"] = weight
        if format_quantity is not None:
            payload["format_quantity"] = format_quantity

        return cast(
            Dict[str, Any],
            self._request("POST", "/marketplace/listings", json=payload).json(),
        )

    @rate_limit_retry
    @standard_retry
    def edit_listing(
        self,
        listing_id: int | str,
        *,
        condition: Optional[str] = None,
        sleeve_condition: Optional[str] = None,
        price: Optional[float] = None,
        comments: Optional[str] = None,
        allow_offers: Optional[bool] = None,
        status: Optional[str] = None,
        external_id: Optional[str] = None,
        location: Optional[str] = None,
        weight: Optional[int] = None,
        format_quantity: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Edit an existing marketplace listing via direct API.

        Use this for single-item updates instead of CSV upload.
        Only listings with status "For Sale", "Draft", or "Expired" can be modified.

        Args:
            listing_id: Discogs listing identifier.
            condition: Optional new media condition.
            sleeve_condition: Optional new sleeve condition.
            price: Optional new price.
            comments: Optional new comments.
            allow_offers: Optional allow offers setting.
            status: Optional new status ("For Sale" or "Draft").
            external_id: Optional private reference ID (SKU).
            location: Optional storage location.
            weight: Optional weight in grams.
            format_quantity: Optional item count for shipping.

        Returns:
            Empty response on success (204 No Content).
        """
        payload: Dict[str, Any] = {}
        if condition is not None:
            payload["condition"] = condition
        if sleeve_condition is not None:
            payload["sleeve_condition"] = sleeve_condition
        if price is not None:
            payload["price"] = price
        if comments is not None:
            payload["comments"] = comments
        if allow_offers is not None:
            payload["allow_offers"] = allow_offers
        if status is not None:
            payload["status"] = status
        if external_id is not None:
            payload["external_id"] = external_id
        if location is not None:
            payload["location"] = location
        if weight is not None:
            payload["weight"] = weight
        if format_quantity is not None:
            payload["format_quantity"] = format_quantity

        if not payload:
            return {}

        response = self._request("POST", f"/marketplace/listings/{listing_id}", json=payload)
        # Returns 204 No Content on success
        if response.status_code == 204:
            return {"status": "success", "listing_id": listing_id}
        try:
            return cast(Dict[str, Any], response.json())
        except ValueError:
            return {"status": "success", "listing_id": listing_id}

    @rate_limit_retry
    @standard_retry
    def delete_listing(self, listing_id: int | str) -> Dict[str, Any]:
        """Delete a marketplace listing via direct API.

        Use this for single-item deletions instead of CSV upload.

        Args:
            listing_id: Discogs listing identifier.

        Returns:
            Empty response on success (204 No Content).
        """
        response = self._request("DELETE", f"/marketplace/listings/{listing_id}")
        if response.status_code == 204:
            return {"status": "deleted", "listing_id": listing_id}
        try:
            return cast(Dict[str, Any], response.json())
        except ValueError:
            return {"status": "deleted", "listing_id": listing_id}

    # --- Inventory export helpers -------------------------------------------------

    def start_inventory_export(self) -> Dict[str, Any]:
        """Kick off an inventory export job.

        The Discogs API does not support any filter parameters for exports;
        it always exports the full inventory as CSV.

        Returns:
            Job metadata containing an ``id`` for polling.

        Raises:
            DiscogsAPIError: If Discogs does not return an export id.
        """
        response = self._request("POST", "/inventory/export")
        try:
            data = cast(Dict[str, Any], response.json())
        except ValueError:
            data = {}
        export_id = data.get("id")

        # Fallback: extract export id from the Location header.
        if not export_id:
            location = response.headers.get("Location", "")
            if location:
                try:
                    export_id = int(location.rstrip("/").split("/")[-1])
                    log.info("Extracted export id=%s from Location header", export_id)
                except (ValueError, IndexError):
                    log.warning("Could not parse export id from Location header: %s", location)

        if not export_id:
            raise DiscogsAPIError(
                f"Discogs inventory export did not return an id. "
                f"HTTP {response.status_code}: {response.text[:500]}",
                error_code="discogs_export_failed",
                http_status=502,
                context={"status": response.status_code, "body": response.text},
            )

        # Fetch full export metadata via GET /inventory/export/{id}.
        return self.get_inventory_export(export_id)

    def list_inventory_exports(self) -> Dict[str, Any]:
        """List recent inventory exports.

        Returns:
            Payload with ``items`` list of recent exports.
        """
        return cast(
            Dict[str, Any],
            self._request("GET", "/inventory/export").json(),
        )

    def get_inventory_export(self, export_id: int | str) -> Dict[str, Any]:
        """Fetch metadata for a previously created inventory export job.

        Args:
            export_id: Export job identifier.

        Returns:
            Export job metadata payload.
        """
        return cast(
            Dict[str, Any],
            self._request("GET", f"/inventory/export/{export_id}").json(),
        )

    def wait_for_inventory_export(
        self,
        export_id: int | str,
        *,
        poll_interval: int = 5,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        """Poll the export job until it completes or fails.

        Args:
            export_id: Export job identifier.
            poll_interval: Seconds between polling attempts.
            timeout: Max time to wait before raising TimeoutError.

        Returns:
            Final job metadata (including ``download_url``).

        Raises:
            DiscogsAPIError: If the export job fails.
            TimeoutError: If the export does not finish before timeout.
        """
        import time as _time

        _DONE_STATES = {"completed", "success"}
        _FAILED_STATES = {"failed", "error"}

        deadline = _time.time() + timeout
        while True:
            job = self.get_inventory_export(export_id)
            state = (job.get("status") or job.get("state") or "").lower()
            log.debug("Export %s status=%s", export_id, state)
            if state in _DONE_STATES:
                return job
            if state in _FAILED_STATES:
                raise DiscogsAPIError(
                    f"Discogs inventory export {export_id} failed (state={state})",
                    error_code="discogs_export_failed",
                    http_status=502,
                    context={"job": job},
                )
            if _time.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for Discogs export {export_id} "
                    f"(last state={state})"
                )
            _time.sleep(max(1, poll_interval))

    def download_inventory_export(
        self,
        export_id: int | str,
        *,
        poll_interval: int = 5,
        timeout: int = 300,
        dest_path: Optional[str] = None,
    ) -> str | bytes:
        """Download a completed inventory export.

        Uses ``GET /inventory/export/{id}/download`` (authenticated).

        Args:
            export_id: Export job identifier.
            poll_interval: Seconds between polling attempts.
            timeout: Max time to wait before raising TimeoutError.
            dest_path: Optional destination path for the downloaded CSV.

        Returns:
            File path when ``dest_path`` is provided; otherwise raw bytes.

        Raises:
            TimeoutError: If the export does not finish before timeout.
        """
        self.wait_for_inventory_export(
            export_id, poll_interval=poll_interval, timeout=timeout
        )
        response = self._request(
            "GET", f"/inventory/export/{export_id}/download", timeout=timeout
        )

        if dest_path:
            with open(dest_path, "wb") as fp:
                fp.write(response.content)
            return dest_path
        return cast(bytes, response.content)


_client: Optional[DiscogsClient] = None


def _get_client() -> DiscogsClient:
    """Return a cached Discogs client instance.

    Returns:
        DiscogsClient singleton instance.
    """
    global _client
    if _client is None:
        _client = DiscogsClient()
    return _client


# Names that map directly to DiscogsClient methods (used by __getattr__)
_CLIENT_METHODS = {
    "list_orders",
    "get_order",
    "edit_order",
    "add_order_message",
    "list_order_messages",
    "search_release",
    "get_release",
    "upload_inventory_csv",
    "add_inventory",
    "change_inventory",
    "delete_inventory",
    "list_inventory",
    "create_listing",
    "edit_listing",
    "delete_listing",
    "start_inventory_export",
    "list_inventory_exports",
    "get_inventory_export",
    "wait_for_inventory_export",
    "download_inventory_export",
}


def get_inventory_listing(listing_id: int | str) -> Dict[str, Any]:
    """Fetch a single inventory listing (alias for DiscogsClient.get_listing)."""
    return cast(Dict[str, Any], _get_client().get_listing(listing_id))


def get_release(release_id: int | str) -> Dict[str, Any]:
    """Fetch release details (alias for DiscogsClient.get_release)."""
    return cast(Dict[str, Any], _get_client().get_release(release_id))


def discogs_identity() -> Dict[str, Any]:
    """Fetch authenticated identity (alias for DiscogsClient.identity)."""
    return cast(Dict[str, Any], _get_client().identity())


def __getattr__(name: str) -> Any:
    """Lazy proxy: module-level access to DiscogsClient singleton methods."""
    if name in _CLIENT_METHODS:
        return getattr(_get_client(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DiscogsClient",
    "_get_client",
    "get_inventory_listing",
    "get_release",
    "discogs_identity",
    # All methods delegated via __getattr__
    "list_orders",
    "get_order",
    "edit_order",
    "add_order_message",
    "list_order_messages",
    "search_release",
    "get_release",
    "upload_inventory_csv",
    "add_inventory",
    "change_inventory",
    "delete_inventory",
    "create_listing",
    "edit_listing",
    "delete_listing",
    "list_inventory",
    "start_inventory_export",
    "list_inventory_exports",
    "get_inventory_export",
    "wait_for_inventory_export",
    "download_inventory_export",
]
