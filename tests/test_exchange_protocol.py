from fastapi.testclient import TestClient


def _client(monkeypatch):
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a")
    from exchange.app.main import app

    return TestClient(app)


def test_products_prices_and_quantities_are_derived_from_discogs_catalog(monkeypatch):
    from exchange.bl_connector import BaseLinkerConnector

    monkeypatch.setattr(
        BaseLinkerConnector,
        "products_list",
        lambda self, payload: {
            "101": {"price": 12.5, "quantity": 2},
            "102": {"price": 7, "quantity": 0},
            "pages": 1,
        },
    )
    client = _client(monkeypatch)

    prices = client.post("/exchange", json={"action": "ProductsPrices", "bl_pass": "pass-a"})
    quantities = client.post("/exchange", json={"action": "ProductsQuantity", "bl_pass": "pass-a"})

    assert prices.status_code == 200
    assert prices.json() == {"101": {"0": "12.50"}, "102": {"0": "7.00"}, "pages": 1}
    assert quantities.status_code == 200
    assert quantities.json() == {"101": {"0": "2"}, "102": {"0": "0"}, "pages": 1}


def test_order_update_is_sent_directly_to_discogs(monkeypatch):
    monkeypatch.setattr(
        "exchange.app.routes.exchange.sync_order_update_to_discogs",
        lambda payload: {"status": "OK", "processed": 1, "failed": []},
    )
    client = _client(monkeypatch)

    response = client.post(
        "/exchange",
        json={
            "action": "OrderUpdate",
            "bl_pass": "pass-a",
            "order_id": "discogs-1",
            "update_type": "status",
            "update_value": "Shipped",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"counter": 1}


def test_order_add_is_acknowledged_without_persistent_storage(monkeypatch):
    client = _client(monkeypatch)

    response = client.post(
        "/exchange",
        json={"action": "OrderAdd", "bl_pass": "pass-a", "order_id": "incoming-1"},
    )

    assert response.status_code == 200
    assert response.json() == {"order_id": "incoming-1"}
