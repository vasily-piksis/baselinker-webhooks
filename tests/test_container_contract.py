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


def test_runtime_configuration_has_no_external_store_settings():
    example = Path(".env.example").read_text()

    assert "APP_DATABASE_URL" not in example
    assert "RATE_LIMITER_REDIS_URL" not in example


def test_requirements_exclude_database_and_redis_clients():
    requirements = Path("requirements.txt").read_text().lower()

    assert "sqlalchemy" not in requirements
    assert "alembic" not in requirements
    assert "psycopg2" not in requirements
    assert "redis" not in requirements
