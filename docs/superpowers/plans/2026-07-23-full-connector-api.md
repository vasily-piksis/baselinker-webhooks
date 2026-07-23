# Full Connector API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore every legacy BaseLinker connector endpoint in the stateless webhook service.

**Architecture:** Port the connector, schemas, handlers, and routes from `base-discogs`, replacing every persisted state operation with the existing in-memory cache. External Discogs/BaseLinker APIs remain the source of truth.

**Tech Stack:** FastAPI, Pydantic, httpx/requests, in-memory TTL cache, Docker Compose.

## Global Constraints

- No Airflow, Postgres, Redis, SQLAlchemy, Alembic, queue, or volume.
- Preserve `/exchange`, `/bl/products/*`, `/bl/orders/*`, product webhook, and health contracts.
- Keep BaseLinker pass validation and response shapes.
- Do not commit or push unless explicitly requested.

---

### Task 1: Port stateless connector core

**Files:**
- Create: `exchange/bl_connector.py`, `exchange/schemas.py`, `exchange/translation.py`, `exchange/status_map.py`, `exchange/app/handlers/connector_handler.py`
- Modify: `exchange/clients/discogs_client.py`, `exchange/utils/recent_result_cache.py`
- Test: `tests/test_connector.py`

- [ ] **Step 1: Write failing connector tests**

```python
def test_products_list_uses_connector_snapshot(mocker):
    connector = BaseLinkerConnector()
    mocker.patch.object(connector, "_products_from_inventory_export", return_value={"products": {"1": {"sku": "A"}}})
    assert connector.products_list({"page": 1, "per_page": 1000})["1"]["sku"] == "A"

def test_orders_get_delegates_to_discogs(mocker):
    mocker.patch("exchange.bl_connector.discogs_client.get_order", return_value={"id": "42", "items": []})
    assert BaseLinkerConnector().orders_get({"order_id": "42"})["status"] == "OK"
```

- [ ] **Step 2: Verify red**

Run: `pytest tests/test_connector.py -q`

Expected: import failure because `exchange.bl_connector` is absent.

- [ ] **Step 3: Port core with in-memory cache only**

Copy connector transformations and Pydantic request schemas from the original project. Replace catalog-state and database calls with current Discogs/BaseLinker reads plus `recent_result_cache`; remove all imports of `database`, `dags`, and Airflow symbols.

- [ ] **Step 4: Verify green**

Run: `pytest tests/test_connector.py -q`

Expected: all connector tests pass.

### Task 2: Restore routes and Exchange dispatch

**Files:**
- Create: `exchange/app/routes/{baselinker.py,exchange.py}`
- Modify: `exchange/app/routes/__init__.py`, `exchange/app/main.py`, `exchange/state.py`
- Test: `tests/test_connector_routes.py`

- [ ] **Step 1: Write failing HTTP-contract tests**

```python
def test_exchange_lists_all_supported_methods(client):
    response = client.post("/exchange", json={"action": "SupportedMethods", "bl_pass": "pass-a"})
    assert "ProductsCategories" in response.json()
    assert "OrdersGet" in response.json()

def test_bl_products_list_is_authenticated(client, mocker):
    mocker.patch("exchange.app.routes.baselinker.connector.products_list", return_value={"pages": 1})
    assert client.post("/bl/products/list", json={"bl_pass": "pass-a"}).json()["pages"] == 1
```

- [ ] **Step 2: Verify red**

Run: `pytest tests/test_connector_routes.py -q`

Expected: `/exchange` and `/bl/products/list` return 404.

- [ ] **Step 3: Implement all legacy actions**

Register `/exchange` and `/bl` routers. Dispatch the complete supported-method set: discovery, file version, categories, product list/data/prices/quantity/update/add/delete, order add/update/get, statuses, delivery methods, and payment methods. Categories use an in-memory generated `Discogs Marketplace` fallback and no persistent creation.

- [ ] **Step 4: Verify green**

Run: `pytest tests/test_connector_routes.py tests/test_webhooks.py -q`

Expected: all route contracts pass.

### Task 3: Verify stateless deployment and publish when asked

**Files:**
- Modify: `README.md`, `tests/test_airflow_free.py`, `tests/test_container_contract.py`

- [ ] **Step 1: Extend import/dependency tests**

```python
def test_connector_has_no_external_store_imports():
    assert import_targets("exchange") & {"database", "redis", "airflow", "dags"} == set()
```

- [ ] **Step 2: Run full verification**

Run: `pytest -q && docker compose config --quiet && docker build -t baselinker-webhooks:full-connector .`

Expected: all tests pass and image builds without external-store packages.
