from __future__ import annotations

import ast
from pathlib import Path


def test_ttl_cache_returns_value_until_expiry():
    from exchange.utils.ttl_cache import TTLCache

    now = [10.0]
    cache = TTLCache(clock=lambda: now[0])
    cache.set("webhook:42", {"counter": 1}, ttl_seconds=30)

    assert cache.get("webhook:42") == {"counter": 1}
    now[0] = 41.0
    assert cache.get("webhook:42") is None


def test_runtime_source_has_no_external_store_or_airflow_imports():
    forbidden = {"database", "redis", "airflow", "dags"}
    violations = []
    for path in Path("exchange").rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                targets = [node.module]
            violations.extend(
                f"{path}:{target}"
                for target in targets
                if target.split(".")[0] in forbidden
            )
    assert violations == []
