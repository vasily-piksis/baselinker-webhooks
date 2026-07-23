# BaseLinker Webhooks

Standalone FastAPI service for the BaseLinker product webhooks. It processes
requests synchronously and has no Airflow, DAG, queue, or post-webhook trigger.

## Configuration

Copy the required credentials into `.env`; start from `.env.example`. The
service requires BaseLinker, Discogs, Postgres, and inventory settings listed
there. `.env` is excluded from both Git and Docker build context.

## Run

```sh
docker compose up -d --build
docker compose ps
```

The Compose stack joins the pre-existing external `traefik` network and serves
`bl-sync.musicseller.pro`. It uses the existing Postgres configured by
`APP_DATABASE_URL`; no database service or migration is run automatically.

## Endpoints

- `POST /product/add`
- `POST /product/quantity`
- `POST /product/quantity/update`
- `POST /product/price/update`
- `POST /product/delete`
- `GET /health`, `GET /healthz`, `GET /readyz`
