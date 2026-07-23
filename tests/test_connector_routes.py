from __future__ import annotations

from fastapi.testclient import TestClient


def test_exchange_lists_all_supported_methods(monkeypatch):
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a")
    from exchange.app.main import app

    response = TestClient(app).post(
        "/exchange", json={"action": "SupportedMethods", "bl_pass": "pass-a"}
    )

    assert response.status_code == 200
    assert "ProductsCategories" in response.json()
    assert "OrdersGet" in response.json()


def test_bl_products_list_is_authenticated(monkeypatch):
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a")
    from exchange.bl_connector import BaseLinkerConnector
    monkeypatch.setattr(BaseLinkerConnector, "products_list", lambda self, payload: {"pages": 1})
    from exchange.app.main import app

    response = TestClient(app).post("/bl/products/list", json={"bl_pass": "pass-a"})

    assert response.status_code == 200


def test_bl_order_routes_delegate_to_the_stateless_connector(monkeypatch):
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a")
    from exchange.app.routes import baselinker
    from exchange.app.main import app

    monkeypatch.setattr(
        baselinker.connector,
        "orders_list",
        lambda payload: {"status": "OK", "page": 1, "per_page": 50, "counter": 0, "orders": []},
    )
    monkeypatch.setattr(
        baselinker.connector,
        "orders_get",
        lambda payload: {"status": "OK", "order": {"order_id": payload["order_id"]}},
    )
    monkeypatch.setattr(
        baselinker.connector,
        "orders_status",
        lambda payload: {"status": "OK", "order_id": payload["order_id"], "updated_fields": [], "message_sent": False},
    )
    client = TestClient(app)

    assert client.post("/bl/orders/list", json={"bl_pass": "pass-a"}).status_code == 200
    assert client.post("/bl/orders/get", json={"bl_pass": "pass-a", "order_id": "42"}).json()["order"]["order_id"] == "42"
    assert client.post("/bl/orders/status", json={"bl_pass": "pass-a", "order_id": "42"}).json()["status"] == "OK"
