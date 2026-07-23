# Full stateless BaseLinker connector API

## Goal

Restore the complete BaseLinker integration API in `baselinker-webhooks`, while preserving the stateless runtime: no Airflow, Postgres, Redis, queue, volume, or persistent catalog.

## Public API

Keep the existing product webhook routes and add the original connector surfaces:

- `POST /exchange` and `GET /exchange`
- `POST /bl/products/list`
- `POST /bl/products/data`
- `POST /bl/products/add`
- `POST /bl/products/quantity`
- `POST /bl/products/quantity/update`
- `POST /bl/products/delete`
- `POST /bl/orders/list`
- `POST /bl/orders/get`
- `POST /bl/orders/status`

`/exchange` advertises and handles all legacy methods: `SupportedMethods`, `GetModuleInformation`, `FileVersion`, `ProductsCategories`, `ProductsList`, `ProductsData`, `ProductsPrices`, `ProductsQuantity`, `ProductsPriceUpdate`, `ProductsQuantityUpdate`, `ProductAdd`, `CategoryAdd`, `ProductDelete`, `OrderAdd`, `OrderUpdate`, `OrdersGet`, `StatusesList`, `DeliveryMethodsList`, and `PaymentMethodsList`.

## Stateless data sources

Catalog and product-detail methods call the existing Discogs/BaseLinker clients and keep short-lived snapshots in the process cache. Categories are generated from the current Discogs-facing catalog response; no created category is retained across a restart. Prices and quantities come from the current external product data. Order list/get/status methods proxy and map Discogs orders synchronously.

The existing in-memory TTL caches protect API rate limits, export snapshots, connector responses, and duplicate webhook deliveries within one running container. Restarting the container intentionally clears all caches; the first subsequent request refreshes data from external APIs.

## Compatibility and security

All endpoints use the same BaseLinker pass validation, body-size limits, correlation IDs, secret redaction, and structured errors as product webhooks. Existing BaseLinker request and response shapes are preserved. Routes not part of the original integration remain absent.

## Verification

Tests cover each restored route, the complete `SupportedMethods` list, endpoint authentication, product/catalog response delegation, category response behavior, orders delegation, and absence of database/Redis imports. Docker Compose and image verification remain stateless.
