"""State management helpers for Exchange processing."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from database.models.catalog_state import CatalogState
from database.repositories.catalog_state_repository import CatalogStateRepository
from database.session import get_session
from exchange.utils import to_float, to_int

_CATALOG_LOCK = threading.Lock()
_CATEGORIES_KEY = "__categories__"

def _record_to_product(record: CatalogState) -> Dict[str, Any]:
    data = record.data if isinstance(record.data, dict) else {}
    product = dict(data)
    product.setdefault("id", record.product_id)
    if record.updated_at and not product.get("updated_at"):
        product["updated_at"] = record.updated_at.isoformat()
    return product


def _load_categories(repo: CatalogStateRepository) -> Dict[str, Dict[str, Any]]:
    record = repo.get_product(_CATEGORIES_KEY)
    if record and isinstance(record.data, dict):
        categories = record.data.get("categories")
        if isinstance(categories, dict):
            return categories
    return {}


def _save_categories(repo: CatalogStateRepository, categories: Dict[str, Dict[str, Any]]) -> None:
    now_dt = datetime.now(timezone.utc)
    repo.upsert_product(
        _CATEGORIES_KEY,
        {
            "categories": categories,
            "updated_at": now_dt.isoformat(),
            "kind": "categories",
        },
        updated_at=now_dt,
    )


def _as_list(candidate: Any) -> List[Dict[str, Any]]:
    if isinstance(candidate, list):
        return [item for item in candidate if isinstance(item, dict)]
    if isinstance(candidate, dict):
        return [candidate]
    return []


def _extract_products(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = (
        payload.get("products")
        or payload.get("items")
        or payload.get("rows")
        or payload.get("data")
        or []
    )
    return _as_list(items)


def _product_identity(product: Dict[str, Any]) -> Optional[str]:
    for key in ("product_id", "id", "external_sku", "sku", "release_id"):
        value = product.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return None


def _normalize_product(
    product: Dict[str, Any], fallback: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    product_id = _product_identity(product) or _product_identity(fallback) or ""
    product_id = product_id.strip()
    if not product_id:
        return None

    sku = product.get("sku") or product.get("external_sku") or fallback.get("sku") or product_id
    name = product.get("name") or product.get("title") or fallback.get("name") or ""
    currency = product.get("currency") or fallback.get("currency") or "USD"
    price = to_float(
        product.get("price_brutto")
        or product.get("price")
        or fallback.get("price_brutto")
        or fallback.get("price"),
        default=0.0,
    )
    quantity = to_int(
        product.get("quantity")
        or product.get("stock")
        or product.get("amount")
        or fallback.get("quantity")
        or fallback.get("stock"),
        default=0,
    )
    release_id = (
        product.get("release_id")
        or fallback.get("release_id")
        or product.get("format")
        or fallback.get("format")
    )

    return {
        "id": str(product_id),
        "sku": str(sku),
        "name": str(name),
        "currency": str(currency),
        "price": price,
        "quantity": quantity,
        "release_id": str(release_id) if release_id else None,
        "raw": product,
    }


def _index_discogs_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        external = row.get("external_id") or row.get("external_sku")
        release_id = row.get("release_id")
        key = None
        if external:
            key = str(external)
        elif release_id:
            key = str(release_id)
        if key:
            index[key] = row
    return index


def upsert_from_payload(
    action: str,
    payload: Dict[str, Any],
    discogs_rows: Optional[Iterable[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Upsert catalog entries from an inventory payload.

    Args:
        action: Source action name.
        payload: Inventory event payload.
        discogs_rows: Optional Discogs export rows for enrichment.

    Returns:
        List of catalog entries that were updated.
    """
    products = _extract_products(payload)
    if not products:
        return []
    catalog_updates: List[Dict[str, Any]] = []
    rows_index = _index_discogs_rows(discogs_rows or [])
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat(timespec="seconds")

    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            for product in products:
                normalized = _normalize_product(product, payload)
                if not normalized:
                    continue
                key = normalized["id"]
                existing = repo.get_product(key)
                entry = dict(existing.data) if existing and isinstance(existing.data, dict) else {}

                discogs_row = None
                if normalized["release_id"]:
                    discogs_row = rows_index.get(str(normalized["release_id"]))
                if not discogs_row and normalized["sku"]:
                    discogs_row = rows_index.get(str(normalized["sku"]))

                entry.update(
                    {
                        "id": key,
                        "sku": normalized["sku"],
                        "name": normalized["name"],
                        "currency": normalized["currency"],
                        "price": normalized["price"],
                        "quantity": normalized["quantity"],
                        "release_id": normalized["release_id"],
                        "updated_at": now_iso,
                        "source_action": action,
                        "raw": normalized["raw"],
                    }
                )
                if discogs_row:
                    entry["discogs_row"] = discogs_row
                repo.upsert_product(key, entry, updated_at=now_dt)
                catalog_updates.append(entry.copy())

    return catalog_updates


def delete_from_payload(payload: Dict[str, Any]) -> List[str]:
    """Delete catalog entries referenced by an inventory payload.

    Args:
        payload: Inventory event payload.

    Returns:
        List of deleted product identifiers.
    """
    products = _extract_products(payload)
    deleted: List[str] = []
    ids_to_remove: List[str] = []
    for product in products:
        ident = _product_identity(product)
        if ident:
            ids_to_remove.append(str(ident))

    if not ids_to_remove:
        return deleted

    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            for ident in ids_to_remove:
                if repo.delete_product(ident):
                    deleted.append(ident)

    return deleted


def list_products(page: int = 1, per_page: int = 100) -> Dict[str, Any]:
    """List catalog products with pagination.

    Args:
        page: Page number (1-based).
        per_page: Items per page.

    Returns:
        Paginated catalog response payload.
    """
    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            records = [
                record for record in repo.get_all_products() if record.product_id != _CATEGORIES_KEY
            ]
    records.sort(key=lambda record: record.updated_at or datetime.min, reverse=True)
    all_products = [_record_to_product(record) for record in records]
    updated_at = records[0].updated_at.isoformat() if records and records[0].updated_at else None

    total = len(all_products)
    page = max(1, page)
    per_page = max(1, per_page)
    start = (page - 1) * per_page
    end = start + per_page
    return {
        "status": "OK",
        "page": page,
        "per_page": per_page,
        "total": total,
        "products": all_products[start:end],
        "updated_at": updated_at,
    }


def get_products(product_ids: Sequence[str]) -> List[Dict[str, Any]]:
    """Fetch catalog products by identifiers.

    Args:
        product_ids: Iterable of product identifiers.

    Returns:
        List of catalog product dictionaries.
    """
    ids = {str(pid) for pid in product_ids if str(pid).strip()}
    if not ids:
        return []
    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            products: List[Dict[str, Any]] = []
            for pid in ids:
                record = repo.get_product(pid)
                if record and record.product_id != _CATEGORIES_KEY:
                    products.append(_record_to_product(record))
            return products


def _filter_products(ids: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            if not ids:
                records = [
                    record
                    for record in repo.get_all_products()
                    if record.product_id != _CATEGORIES_KEY
                ]
                return [_record_to_product(record) for record in records]
            wanted = {str(pid) for pid in ids if str(pid).strip()}
            products: List[Dict[str, Any]] = []
            for pid in wanted:
                record = repo.get_product(pid)
                if record and record.product_id != _CATEGORIES_KEY:
                    products.append(_record_to_product(record))
            return products


def get_prices(product_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    """Return price information for catalog products.

    Args:
        product_ids: Optional product identifiers to filter.

    Returns:
        List of price records.
    """
    products = _filter_products(product_ids)
    return [
        {
            "id": prod["id"],
            "price": prod.get("price", 0.0),
            "currency": prod.get("currency", "USD"),
            "updated_at": prod.get("updated_at"),
        }
        for prod in products
    ]


def get_quantities(product_ids: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    """Return quantity information for catalog products.

    Args:
        product_ids: Optional product identifiers to filter.

    Returns:
        List of quantity records.
    """
    products = _filter_products(product_ids)
    return [
        {
            "id": prod["id"],
            "quantity": prod.get("quantity", 0),
            "updated_at": prod.get("updated_at"),
        }
        for prod in products
    ]


def list_categories() -> Dict[str, str]:
    """List stored catalog categories.

    Returns:
        Mapping of category ids to names.
    """
    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            stored_categories = _load_categories(repo)
            records = [
                record for record in repo.get_all_products() if record.product_id != _CATEGORIES_KEY
            ]
            store = {_record.product_id: _record_to_product(_record) for _record in records}
        categories: Dict[str, str] = {}

        def _resolve_category_path(cat_id: str, memo: Dict[str, str]) -> str:
            if cat_id in memo:
                return memo[cat_id]
            entry = stored_categories.get(cat_id) or {}
            name = str(entry.get("name") or cat_id).strip()
            parent_id = str(entry.get("parent_id") or "").strip()
            if parent_id and parent_id != cat_id and parent_id in stored_categories:
                parent_path = _resolve_category_path(parent_id, memo)
                path = f"{parent_path}/{name}" if parent_path else name
            else:
                path = name
            memo[cat_id] = path
            return path

        memo: Dict[str, str] = {}
        for cat_id in stored_categories:
            path = _resolve_category_path(str(cat_id), memo)
            if path:
                categories[str(cat_id)] = path

        for product in store.values():
            raw_categories = product.get("categories")
            if isinstance(raw_categories, dict):
                raw_categories = [raw_categories]
            if not isinstance(raw_categories, list):
                continue
            for entry in raw_categories:
                if isinstance(entry, dict):
                    raw_id = entry.get("id") or entry.get("category_id") or entry.get("code")
                    raw_name = (
                        entry.get("path")
                        or entry.get("full_name")
                        or entry.get("name")
                        or entry.get("label")
                    )
                    ident = str(raw_id or raw_name or "").strip()
                    if not ident:
                        continue
                    name = str(raw_name or ident).strip()
                    categories[ident] = name
                elif isinstance(entry, str) and entry.strip():
                    ident = entry.strip()
                    categories[ident] = ident

    if not categories:
        return {"discogs": "Discogs Marketplace"}

    ordered = sorted(categories.items(), key=lambda item: item[1].lower())
    return {key: value for key, value in ordered}


def _next_category_id(existing: Dict[str, Any]) -> str:
    numeric_ids: List[int] = []
    for key in existing.keys():
        try:
            numeric_ids.append(int(str(key)))
        except (TypeError, ValueError):
            continue
    if numeric_ids:
        return str(max(numeric_ids) + 1)
    return "1"


def add_category(name: str, parent_id: Optional[str] = None) -> str:
    """Add a new catalog category.

    Args:
        name: Category name.
        parent_id: Optional parent category identifier.

    Returns:
        Category identifier.
    """
    title = (name or "").strip()
    if not title:
        raise ValueError("Category name is required")
    parent = (parent_id or "").strip()
    with _CATALOG_LOCK:
        with get_session() as session:
            repo = CatalogStateRepository(session)
            categories = _load_categories(repo)
            new_id = _next_category_id(categories)
            categories[new_id] = {"name": title, "parent_id": parent}
            _save_categories(repo, categories)
            return new_id


__all__ = [
    "upsert_from_payload",
    "delete_from_payload",
    "list_products",
    "get_products",
    "get_prices",
    "get_quantities",
    "list_categories",
    "add_category",
]
