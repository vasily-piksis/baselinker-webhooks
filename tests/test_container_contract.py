from __future__ import annotations

from pathlib import Path


def test_compose_is_self_contained():
    compose = Path("docker-compose.yml").read_text()

    assert "baselinker-webhooks:" in compose
    assert "env_file:" in compose
    assert "- .env" in compose
    assert "../" not in compose
    assert "external: true" in compose


def test_dockerfile_runs_only_webhook_app():
    dockerfile = Path("Dockerfile").read_text().lower()

    assert "exchange.app:app" in dockerfile
    assert "airflow" not in dockerfile
