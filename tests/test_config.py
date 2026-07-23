from __future__ import annotations


def test_settings_reads_required_webhook_values(monkeypatch):
    from app.core.config import Settings

    monkeypatch.setenv("BL_API_TOKEN", "bl-token")
    monkeypatch.setenv("BL_ALLOWED_PASSES", "pass-a,pass-b")
    monkeypatch.setenv("DISCOGS_TOKEN", "discogs-token")
    monkeypatch.setenv("DISCOGS_UA", "webhooks-test/1.0")

    settings = Settings.from_env()

    assert settings.bl_allowed_passes == {"pass-a", "pass-b"}


def test_settings_has_no_airflow_fields(monkeypatch):
    from app.core.config import Settings

    monkeypatch.setenv("AIRFLOW_TRIGGER_URL", "https://must-not-be-used")

    assert not hasattr(Settings.from_env(), "airflow_trigger_url")
