# BaseLinker webhooks: standalone application design

## Goal

Create a self-contained `baselinker-webhooks` application that receives and processes BaseLinker product webhooks. It replaces the current FastAPI webhook runtime without requiring Airflow, DAGs, queues, or an external post-processing trigger.

## Scope

The application exposes the existing BaseLinker-compatible endpoints:

- `POST /product/add`
- `POST /product/quantity`
- `POST /product/quantity/update`
- `POST /product/price/update`
- `POST /product/delete`
- `GET /health`, `GET /healthz`, and `GET /readyz`

Every webhook and connector request is authenticated with the configured BaseLinker pass, processed synchronously, and answered in the response format expected by BaseLinker. It calls Discogs where the inventory and order flows require it; it does not persist requests locally.

## Architecture

`baselinker-webhooks` is a standalone Python package and Docker build context. It contains only the modules transitively needed by the webhook, Exchange protocol, and inventory flow: API routes and middleware, authentication and logging, inventory processors, BaseLinker and Discogs clients, connector translation, and their shared utilities.

The application must not import `airflow`, `dags`, or use Airflow Variables/Connections as a configuration fallback. The Basecom processor's Airflow trigger path is removed; no replacement trigger is introduced.

Docker Compose runs one FastAPI service, builds it from its own directory, loads `./.env`, stores local runtime artifacts in a named volume, and joins the existing external `traefik` network. The service exposes port 8000 internally and retains the existing Traefik host routing unless deployment configuration requires a different hostname.

## Configuration

`.env` is deliberately ignored by Git and is populated from the current local configuration for values the service needs. `.env.example` is committed with blank secret values and documented defaults.

Required credentials/configuration:

- `BL_API_TOKEN` and `BL_ALLOWED_PASSES` (or `BL_PASS`)
- `DISCOGS_TOKEN`, `DISCOGS_UA`, and `DISCOGS_BASE`
- `APP_DATABASE_URL`
- `BL_INVENTORY_ID`, `BL_SHOP_ID`, `BL_WAREHOUSE_ID`, and `BL_PRICE_GROUP_ID`
- `BASELINKER_EXPORT_ADD_URL` and `BASELINKER_EXPORT_UPDATE_URL` when the current inventory flow uses these endpoints

Optional operational settings keep their current defaults: HTTP timeouts/rate limits, process-local cache TTLs, and `APP_BUILD_SHA`.

No `AIRFLOW_*` variables, Airflow image, DAG directory, or Airflow Python dependency is part of this application.

## Error handling and observability

The service preserves request body limits, correlation IDs, secret redaction, structured error responses, idempotency, and health endpoints. `/readyz` verifies Discogs access. Docker health checks use `/health` and restart the service unless stopped manually.

## Verification

Tests are migrated or added for the webhook endpoints: authentication, BaseLinker response shapes, body limits, idempotency, and assurance that the standalone package imports without Airflow installed. Verification includes a Docker Compose configuration check, image build, and the focused Python test suite.

## Non-goals

- Airflow, DAG scheduling, queues, or any external trigger after a webhook.
- Migrating or changing existing database data (there is no database in this service).
- Changing the existing webhook business rules beyond removing Airflow coupling.
