# Stateless BaseLinker webhook runtime

## Goal

Make `baselinker-webhooks` a single-process, self-contained webhook service with no Postgres, Redis, Airflow, DAG, queue, or external post-processing dependency.

## Runtime behavior

The service continues to expose the five BaseLinker product webhook endpoints and health checks. Each request is synchronously transformed and sent to the required BaseLinker and Discogs APIs.

Rate limiting and webhook idempotency use bounded, TTL-based in-memory caches. They protect one running container from API bursts and immediate BaseLinker retries. Their contents intentionally disappear on restart; no data is recovered or replayed after a restart.

## Removed behavior

- SQLAlchemy models, repositories, sessions, Alembic migrations, and all `APP_DATABASE_*` configuration.
- PostgreSQL driver and database URL configuration.
- Redis client, Redis URL configuration, and cross-process cache behavior.
- Persisted events, persisted idempotency results, catalog state, master catalog hydration, Discogs CSV records, and Basecom export records.
- Local artifact volumes and event reprocessing endpoints.

## Implementation constraints

The inventory flow must produce BaseLinker-compatible responses without database record IDs or event paths. Product add/delete must not attempt catalog-state persistence. Large Discogs batches may still use the existing in-memory CSV generation and direct upload request, but no CSV metadata is saved.

The Docker image retains only HTTP/API and in-memory-rate-limit dependencies. Compose contains one service, no volumes, no database/Redis environment variables, and the existing Traefik network/host routing.

## Verification

Tests prove that the webhook process imports and serves endpoints with no Postgres or Redis package installed, duplicate delivery is answered from an in-memory TTL cache while the process lives, rate limiting works in-memory, and the Docker image builds and starts without external data stores.
