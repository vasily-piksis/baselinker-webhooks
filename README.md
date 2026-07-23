# BaseLinker Webhooks

Standalone FastAPI service for the full BaseLinker connector and product
webhooks. It processes requests synchronously and has no Airflow, DAG, queue,
database, Redis, or post-webhook trigger.

## Configuration

Copy the required credentials into `.env`; start from `.env.example`. The
service requires only the BaseLinker, Discogs, and inventory settings listed
there. `.env` is excluded from both Git and Docker build context.

## Run

```sh
docker compose up -d --build
docker compose ps
```

The Compose stack joins the pre-existing external `traefik` network and serves
`bl-sync.musicseller.pro`. It has no database, Redis, migration, or persistent
volume; caches and request throttling live only in the running process.

## Endpoints

- `POST /product/add`
- `POST /product/quantity`
- `POST /product/quantity/update`
- `POST /product/price/update`
- `POST /product/delete`
- `GET` / `POST /exchange` — BaseLinker Exchange protocol, including discovery,
  products, prices, quantities, categories, orders, statuses, delivery and
  payment methods
- `POST /bl/products/list`, `/bl/products/data`, `/bl/products/add`,
  `/bl/products/quantity`, `/bl/products/quantity/update`, `/bl/products/delete`
- `POST /bl/orders/list`, `/bl/orders/get`, `/bl/orders/status`
- `GET /health`, `GET /healthz`, `GET /readyz`

The product catalog and order reads are fetched from Discogs. Short-lived
snapshots, rate limiting, duplicate protection, and created categories exist
only in the running process and are deliberately lost on restart.
