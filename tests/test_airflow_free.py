from __future__ import annotations

import ast
from pathlib import Path


def test_runtime_contains_no_airflow_or_dags_imports():
    forbidden = {"airflow", "dags"}
    runtime_files = [*Path("exchange").rglob("*.py"), *Path("database").rglob("*.py")]

    violations = []
    for path in runtime_files:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                violations.extend(
                    f"{path}:{alias.name}"
                    for alias in node.names
                    if alias.name.split(".")[0] in forbidden
                )
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.split(".")[0] in forbidden:
                    violations.append(f"{path}:{node.module}")

    assert violations == []
