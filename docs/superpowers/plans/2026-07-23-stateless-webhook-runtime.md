# Stateless Webhook Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Postgres and Redis from the standalone webhook project while preserving synchronous BaseLinker-compatible webhook processing.

**Architecture:** A bounded TTL cache in the Python process replaces persisted idempotency and Redis-backed rate limiting. The webhook processor no longer records events, catalog state, CSV records, or export records; it sends transformed data directly to BaseLinker and Discogs.

**Tech Stack:** Python 3.10, FastAPI, httpx/requests, tenacity, Docker Compose, Traefik.

## Global Constraints

- Do not use or configure Postgres, Redis, SQLAlchemy, Alembic, `psycopg2`, or `APP_DATABASE_*`.
- Do not add a stateful external service, queue, worker, or volume.
- Keep all webhook state in bounded in-memory TTL caches; cache loss on restart is intended.
- Do not commit or publish changes until the user explicitly asks.
- Retain BaseLinker response shapes and synchronous HTTP behavior.

---

### Task 1: Replace persistence and Redis with in-memory runtime state

**Files:**
- Create: `exchange/utils/ttl_cache.py`, `tests/test_in_memory_runtime.py`
- Modify: `exchange/utils/rate_limiter.py`, `exchange/utils/recent_result_cache.py`, `exchange/processors/inventory_processor.py`, `exchange/state.py`, `exchange/processors/{basecom_processor.py,discogs_csv_processor.py}`, `exchange/master.py`
- Delete: `database/`

**Interfaces:**
- Produces `TTLCache.get(key) -> object | None`, `TTLCache.set(key, value, ttl_seconds) -> None`, and `TTLCache.delete(key) -> None`.
- `process_inventory_event()` keeps in-memory idempotency for the configured TTL and never calls a repository or session.

- [ ] **Step 1: Write failing in-memory tests**

```python
def test_ttl_cache_returns_value_until_expiry(monkeypatch):
    cache = TTLCache(clock=lambda: 10.0)
    cache.set("webhook:42", {"counter": 1}, ttl_seconds=30)
    assert cache.get("webhook:42") == {"counter": 1}

def test_runtime_source_has_no_database_or_redis_imports():
    assert import_targets("exchange") & {"database", "redis"} == set()
```

- [ ] **Step 2: Verify the test is red**

Run: `pytest tests/test_in_memory_runtime.py -q`

Expected: failure because `TTLCache` does not exist and runtime modules import persistence.

- [ ] **Step 3: Implement the minimal stateless runtime**

Implement a lock-protected ordered TTL cache with a fixed maximum size. Refactor the rate limiter and recent-result cache to use it. Delete persistence calls and record IDs from inventory, state, CSV, Basecom, and master-catalog code. For CSV/Basecom behavior, retain only in-memory row construction and direct remote upload; do not write metadata or artifacts. Remove the entire `database/` tree.

- [ ] **Step 4: Verify green**

Run: `pytest tests/test_in_memory_runtime.py tests/test_webhooks.py -q`

Expected: all selected tests pass.

### Task 2: Remove storage dependencies and verify the deployment artifact

**Files:**
- Modify: `requirements.txt`, `.env`, `.env.example`, `Dockerfile`, `docker-compose.yml`, `README.md`, `tests/test_container_contract.py`, `tests/test_config.py`

**Interfaces:**
- Container starts with only HTTP/API dependencies and `EXCHANGE_DIR` is removed.
- Compose contains no named volumes and no Postgres/Redis connection setting.

- [ ] **Step 1: Write failing dependency-contract tests**

```python
def test_runtime_configuration_has_no_external_store_settings():
    example = Path(".env.example").read_text()
    assert "APP_DATABASE_URL" not in example
    assert "RATE_LIMITER_REDIS_URL" not in example

def test_requirements_exclude_database_and_redis_clients():
    requirements = Path("requirements.txt").read_text().lower()
    assert "sqlalchemy" not in requirements
    assert "alembic" not in requirements
    assert "psycopg2" not in requirements
    assert "redis" not in requirements
```

- [ ] **Step 2: Verify the test is red**

Run: `pytest tests/test_container_contract.py -q`

Expected: failure because current configuration still lists external storage dependencies.

- [ ] **Step 3: Remove external-store configuration**

Remove database/Redis requirements, variables, Docker data directory creation, Compose volume, and README references. Recreate `.env` only with needed BaseLinker/Discogs values, without printing credentials.

- [ ] **Step 4: Run full verification**

Run: `pytest -q && docker compose config --quiet && docker build -t baselinker-webhooks:local . && docker run --rm --env-file .env baselinker-webhooks:local python -c 'from exchange.app.main import app; assert app.title == "BaseLinker Webhooks"'`

Expected: all tests pass; the image builds and imports without Postgres or Redis packages.
