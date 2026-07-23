from __future__ import annotations

from fastapi.testclient import TestClient


def test_api_exposes_webhooks_and_full_connector_routes(monkeypatch):
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a")
    monkeypatch.setenv("BL_API_TOKEN", "bl-token")
    monkeypatch.setenv("DISCOGS_TOKEN", "discogs-token")
    from exchange.app.main import app
    from exchange.bl_connector import BaseLinkerConnector

    monkeypatch.setattr(BaseLinkerConnector, "products_list", lambda self, payload: {"pages": 1})

    client = TestClient(app)

    assert client.get("/exchange").status_code == 401
    assert client.get("/baselinker").status_code == 404
    assert client.post(
        "/exchange", json={"action": "SupportedMethods", "bl_pass": "pass-a"}
    ).status_code == 200
    assert client.post("/bl/products/list", json={"bl_pass": "pass-a"}).status_code == 200
    assert client.get("/health").status_code == 200


def test_price_webhook_returns_baselinker_counter(monkeypatch):
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a")
    monkeypatch.setenv("BL_API_TOKEN", "bl-token")
    monkeypatch.setenv("DISCOGS_TOKEN", "discogs-token")
    from exchange.app.main import app
    from exchange.app.handlers import inventory_handler

    monkeypatch.setattr(
        inventory_handler,
        "process_inventory_event",
        lambda *args, **kwargs: {
            "status": "OK",
            "detail": {"response": {"processed": 1, "failed": 0}, "rows": [{}]},
        },
    )
    response = TestClient(app).post(
        "/product/price/update",
        json={"product_id": "42", "price": "19.99", "bl_pass": "pass-a"},
    )

    assert response.status_code == 200
    assert response.json() == {"counter": 1}
