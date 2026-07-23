# BaseLinker Webhooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `/Users/vasily/www/baselinker-webhooks` as an independently runnable BaseLinker webhook application with the current webhook behavior and no Airflow coupling.

**Architecture:** The new project owns its FastAPI API, synchronous inventory processing, API clients, SQLAlchemy persistence, migrations, and Docker configuration. It uses environment variables as its only configuration source and connects to the existing Postgres, Redis, BaseLinker, Discogs, and Traefik infrastructure without mounting or importing `base-discogs`.

**Tech Stack:** Python 3.10, FastAPI, Uvicorn, SQLAlchemy/Alembic, httpx/requests, Redis (optional rate-limit cache), Docker Compose, Traefik.

## Global Constraints

- Create and modify files only in `/Users/vasily/www/baselinker-webhooks`; do not change `/Users/vasily/www/base-discogs`.
- Do not commit, stage, initialize Git, or publish anything unless the user explicitly asks.
- Do not import `airflow`, `dags`, or read `AIRFLOW_*` configuration; no Airflow service, image, or DAG file may be included.
- Process the five BaseLinker webhook actions synchronously; do not add a queue, worker, or external trigger.
- `.env` contains real local credentials, is ignored by Git, and is never printed in command output. `.env.example` contains key names and safe defaults only.
- Preserve BaseLinker response shapes: add returns `{"product_id": "..."}`, quantity/price updates return `{"counter": N}`, delete returns `{}`.
- Keep correlation IDs, request-size checks, secret redaction, idempotency, Postgres persistence, and health endpoints.

---

## Target file structure

```
baselinker-webhooks/
  app/
    api/{main.py,routes.py,health.py,webhooks.py}
    middleware/{auth.py,errors.py}
    services/{inventory.py,basecom.py,discogs_csv.py,state.py}
    clients/{baselinker.py,discogs.py}
    core/{config.py,errors.py,logging.py,retry.py,utils.py}
    persistence/{config.py,session.py,models/,repositories/,migrations/}
  tests/{conftest.py,test_webhooks.py,test_airflow_free.py,test_config.py}
  Dockerfile
  docker-compose.yml
  requirements.txt
  .env
  .env.example
  .gitignore
  README.md
```

### Task 1: Establish isolated project and configuration contract

**Files:**
- Create: `.gitignore`, `requirements.txt`, `.env.example`, `app/core/config.py`, `tests/conftest.py`, `tests/test_config.py`
- Modify: `.env`

**Interfaces:**
- Produces `Settings` from `app.core.config` with `bl_api_token`, `bl_allowed_passes`, `discogs_token`, `discogs_ua`, `app_database_url`, `bl_inventory_id`, `bl_shop_id`, `bl_warehouse_id`, `bl_price_group_id`, and optional operational settings.
- Consumes only `os.environ` and `.env`; it must not use an Airflow fallback.

- [ ] **Step 1: Write the failing configuration tests**

```python
def test_settings_reads_required_webhook_values(monkeypatch):
    monkeypatch.setenv("BL_API_TOKEN", "bl-token")
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a,pass-b")
    monkeypatch.setenv("DISCOGS_TOKEN", "discogs-token")
    monkeypatch.setenv("DISCOGS_UA", "webhooks-test/1.0")
    monkeypatch.setenv("APP_DATABASE_URL", "sqlite:///test.db")
    settings = Settings.from_env()
    assert settings.bl_allowed_passes == {"pass-a", "pass-b"}
    assert settings.app_database_url == "sqlite:///test.db"

def test_settings_has_no_airflow_fields(monkeypatch):
    monkeypatch.setenv("AIRFLOW_TRIGGER_URL", "https://must-not-be-used")
    assert not hasattr(Settings.from_env(), "airflow_trigger_url")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_config.py -q`

Expected: failure because `app.core.config.Settings` does not exist.

- [ ] **Step 3: Add the minimal configuration and dependency files**

Implement `Settings.from_env()` using `dataclasses.dataclass`, `dotenv.load_dotenv(override=False)`, and comma-splitting for `BL_ALLOWED_PASSES`; accept `BL_PASS` as one additional pass. Pin only the runtime packages copied from `exchange/requirements.txt` that the final import audit uses: FastAPI, Uvicorn, Pydantic, python-multipart, httpx, requests, SQLAlchemy, Alembic, psycopg2-binary, tenacity, redis, and python-dotenv. Do not add Apache Airflow.

Create `.env.example` with blank values for all credentials and safe defaults for paths, timeouts, rate limits, cache TTLs, and pool values. Populate local `.env` by copying the matching existing local values without printing them; omit `AIRFLOW_UID`, supplier credentials, and all `AIRFLOW_*` keys. Add `.env`, `.env.*`, `.venv/`, `__pycache__/`, and runtime-data paths to `.gitignore`.

- [ ] **Step 4: Run the configuration tests**

Run: `pytest tests/test_config.py -q`

Expected: `2 passed`.

### Task 2: Port persistence and shared, Airflow-free runtime primitives

**Files:**
- Create: `app/persistence/config.py`, `app/persistence/session.py`, `app/persistence/models/{base.py,event.py,idempotency.py,catalog_state.py,discogs_csv.py,basecom_export.py,__init__.py}`, `app/persistence/repositories/{event.py,idempotency.py,catalog_state.py,discogs_csv.py,basecom_export.py,__init__.py}`, `app/persistence/migrations/`, `app/core/{errors.py,logging.py,retry.py,utils.py}`, `tests/test_airflow_free.py`
- Source to port: `/Users/vasily/www/base-discogs/database/` and `/Users/vasily/www/base-discogs/exchange/{errors.py,logging_utils.py,retry.py,utils/}`

**Interfaces:**
- Produces `get_session()` context manager, `EventRepository`, `IdempotencyRepository`, `CatalogStateRepository`, `DiscogsCsvRepository`, `BasecomExportRepository`.
- Produces `BaseWebhookError`, `raise_error(code, message, http_status)`, `CorrelationIdMiddleware`, and `get_correlation_id()`.

- [ ] **Step 1: Write failing persistence and import-isolation tests**

```python
def test_event_repository_persists_a_webhook_event(db_session):
    event = EventRepository(db_session).create(
        action="productquantityupdate", payload={"product_id": "42"}, status="OK"
    )
    assert event.action == "productquantityupdate"

def test_runtime_modules_do_not_import_airflow():
    forbidden = {"airflow", "dags"}
    for module in runtime_module_names():
        source = inspect.getsource(importlib.import_module(module))
        assert not any(f"import {name}" in source or f"from {name}" in source for name in forbidden)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_airflow_free.py -q`

Expected: failure because persistence and runtime modules do not exist.

- [ ] **Step 3: Port only the webhook persistence subset**

Adapt imports from `database.*` and `exchange.*` to `app.persistence.*` and `app.core.*`. Retain the existing schema/table names and Alembic revisions so the service connects to the existing database without a migration. Do not port `OrderInbox`, `MasterCatalog`, validation scripts, or order repositories unless the inventory import audit proves one is referenced.

Make `app/persistence/config.py` read `APP_DATABASE_URL` and the existing `APP_DATABASE_POOL_*` settings. Use the same SQLite-safe session behavior as the current project tests.

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/test_airflow_free.py -q`

Expected: all tests pass and no Airflow package is installed.

### Task 3: Port clients and synchronous inventory processor without the trigger path

**Files:**
- Create: `app/clients/{__init__.py,baselinker.py,discogs.py}`, `app/services/{inventory.py,basecom.py,discogs_csv.py,state.py,recent_listing.py,order_mapping_grace.py,discogs_relist.py}`, `app/core/{mapping.py,statuses.py,delivery_methods.py,translation.py}`, `tests/test_inventory_service.py`
- Source to port: `/Users/vasily/www/base-discogs/exchange/{clients/,processors/inventory_processor.py,processors/basecom_processor.py,processors/discogs_csv_processor.py,processors/recent_discogs_listing.py,processors/order_mapping_grace.py,processors/discogs_relist.py,state.py,settings.py,utils/}`

**Interfaces:**
- Produces `process_inventory_event(action: str, payload: dict[str, Any], *, persist: bool, idempotency_key: str | None, force: bool) -> dict[str, Any]`.
- Produces `upsert_from_payload(action, payload, rows)` and `delete_from_payload(payload)`.
- Consumes credentials exclusively from `Settings`.

- [ ] **Step 1: Write the failing processor tests**

```python
@patch("app.services.inventory.BaseLinkerClient")
@patch("app.services.inventory.edit_listing")
def test_quantity_update_calls_discogs_and_returns_ok(mock_edit, mock_bl):
    mock_bl.return_value.get_inventory_products_data.return_value = {"products": {"42": product()}}
    result = process_inventory_event("ProductQuantityUpdate", {"product_id": "42", "quantity": "3"}, persist=False, idempotency_key=None, force=False)
    assert result["status"] == "OK"
    mock_edit.assert_called_once()

def test_basecom_processing_never_calls_airflow(monkeypatch):
    monkeypatch.setattr("app.services.basecom.requests.post", lambda *a, **k: pytest.fail("external trigger"))
    assert "trigger_airflow_run" not in inspect.getsource(build_basecom_rows)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_inventory_service.py -q`

Expected: failure because `app.services.inventory` is absent.

- [ ] **Step 3: Port and decouple the inventory flow**

Copy behavior, not the package layout: replace imports with the `app.*` target paths; keep BaseLinker/Discogs API calls, idempotency cache, event records, CSV/basecom artifact records, listing cache, relisting, and catalog state updates used by the five product actions.

In `app/services/basecom.py`, retain `build_basecom_rows()` and local artifact generation but delete `trigger_airflow_run()`, its HTTP call, every Airflow setting, and the invocation from `process_inventory_event()`. Delete compatibility aliases that existed only for DAG imports. In clients, delete `_running_in_airflow`, `_get_airflow_variable`, and `_get_airflow_connection`; resolve tokens strictly through `Settings` environment values.

- [ ] **Step 4: Run the processor tests**

Run: `pytest tests/test_inventory_service.py -q`

Expected: all tests pass; the patched external trigger is never invoked.

### Task 4: Build the minimal FastAPI webhook surface

**Files:**
- Create: `app/api/main.py`, `app/api/webhooks.py`, `app/api/health.py`, `app/api/routes.py`, `app/middleware/{auth.py,errors.py}`, `tests/test_webhooks.py`
- Source to port: `/Users/vasily/www/base-discogs/exchange/app/{main.py,routes/webhooks.py,routes/health.py,middleware/auth.py,middleware/exception_handlers.py,handlers/inventory_handler.py}`

**Interfaces:**
- Produces `app.api.main:app`.
- Exposes only `/product/add`, `/product/quantity`, `/product/quantity/update`, `/product/price/update`, `/product/delete`, `/health`, `/healthz`, `/readyz`.

- [ ] **Step 1: Write failing HTTP-contract tests**

```python
def test_price_webhook_returns_baselinker_counter(client, mocker):
    mocker.patch("app.api.webhooks.process_inventory_event", return_value={"status": "OK", "detail": {"response": {"processed": 1, "failed": 0}, "rows": [{}]}})
    response = client.post("/product/price/update", json={"product_id": "42", "price": "19.99", "bl_pass": "pass-a"})
    assert response.status_code == 200
    assert response.json() == {"counter": 1}

def test_webhook_rejects_invalid_pass(client):
    response = client.post("/product/delete", json={"product_id": "42", "bl_pass": "wrong"})
    assert response.status_code == 401

def test_api_has_no_non_webhook_business_routes(client):
    assert client.get("/exchange").status_code == 404
    assert client.get("/baselinker").status_code == 404
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_webhooks.py -q`

Expected: failure because `app.api.main` is absent.

- [ ] **Step 3: Implement the API with exact response shaping**

Port `BodySizeLimitMiddleware`, request body parsing, secret extraction from the four supported headers and body fields, secret scrubbing, correlation middleware, and error handlers. Register only the health and webhook routers.

For `ProductAdd`, return the created listing ID as `product_id` or the request's product ID as fallback. For price/quantity update actions, return `counter` from processed plus failed rows. For delete, update catalog state on success and return `{}`. Do not include the old exchange, orders, connector, or BaseLinker administration routes.

- [ ] **Step 4: Run the HTTP-contract tests**

Run: `pytest tests/test_webhooks.py -q`

Expected: all tests pass.

### Task 5: Make the service deployable using its own Docker configuration and credentials

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `README.md`, `tests/test_container_contract.py`
- Modify: `.env`, `.env.example`, `requirements.txt`

**Interfaces:**
- Docker command: `uvicorn app.api.main:app --host 0.0.0.0 --port 8000`.
- Compose service: `baselinker-webhooks`, with `env_file: .env`, a named runtime-data volume, and external `traefik` network.

- [ ] **Step 1: Write the failing deployment-contract tests**

```python
def test_compose_is_self_contained():
    compose = yaml.safe_load(Path("docker-compose.yml").read_text())
    service = compose["services"]["baselinker-webhooks"]
    assert service["env_file"] == [".env"]
    assert "../" not in json.dumps(service)
    assert compose["networks"]["traefik"]["external"] is True

def test_dockerfile_runs_only_webhook_app():
    dockerfile = Path("Dockerfile").read_text()
    assert "app.api.main:app" in dockerfile
    assert "airflow" not in dockerfile.lower()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_container_contract.py -q`

Expected: failure because Docker and Compose files are absent.

- [ ] **Step 3: Add the deployment artifacts**

Use `python:3.10-slim`, install only `curl` as the OS health-check dependency, copy `requirements.txt`, install it, then copy the standalone project. Create `/app/data/exchange` and execute Uvicorn.

Compose must build with `context: .`, load `./.env`, mount only its named `webhook_data` volume at `/app/data/exchange`, use `restart: unless-stopped`, and health-check `http://localhost:8000/health`. Preserve the `bl-sync.musicseller.pro` Traefik host and HTTPS redirect labels, but rename router/service labels to `baselinker-webhooks` to avoid collision with the old service during cutover. Do not publish host port 8000 unless the deployment requires local debugging.

Document startup, required network (`docker network create traefik` only when it does not already exist), database migration command, health URL, the five accepted endpoints, and the fact that no Airflow component is required.

- [ ] **Step 4: Run deployment checks**

Run: `pytest tests/test_container_contract.py -q && docker compose config --quiet && docker build -t baselinker-webhooks:local .`

Expected: tests pass, Compose resolves without missing variables, and the image builds without Airflow packages.

### Task 6: Execute end-to-end verification and prepare the local handoff

**Files:**
- Modify: `README.md`
- Test: `tests/test_webhooks.py`, `tests/test_inventory_service.py`, `tests/test_airflow_free.py`, `tests/test_container_contract.py`

**Interfaces:**
- Verifies the runnable application at `http://localhost:8000` when Compose is started.

- [ ] **Step 1: Run the complete test suite**

Run: `pytest -q`

Expected: all tests pass without `airflow` installed.

- [ ] **Step 2: Validate the built container without secrets in output**

Run: `docker compose up -d --build && curl --fail --silent http://localhost:8000/health && docker compose ps`

Expected: health response contains `status: ok`; service status is running or healthy. Never run `docker compose config` without `--quiet` if it would print credential-expanded values.

- [ ] **Step 3: Exercise one protected webhook using a local shell variable**

Run: `set -a; source .env; set +a; curl --fail --silent --show-error -H "Content-Type: application/json" -H "X-BL-PASS: $BL_PASS" -d '{"product_id":"test-product","quantity":"1"}' http://localhost:8000/product/quantity/update`

Expected: JSON response has an integer `counter`; do not echo or log the secret.

- [ ] **Step 4: Update README verification record and hand off**

Record the commands and date, excluding credentials and webhook payload details that contain production data. Report the local project path, tests run, Docker image status, and the remaining deployment action: start the new Compose stack and switch the BaseLinker webhook URL only after the user authorizes cutover.
