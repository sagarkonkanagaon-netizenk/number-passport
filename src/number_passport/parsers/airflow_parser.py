"""Airflow DAGs: which DAG and task runs each model file, and on what schedule.

This is orchestration context for the passport ("gold.rwa_by_class is built by task build_gold in DAG
regulatory_reporting, daily at 02:00"). We read the DAG files statically and match string literals that
name a model file (``.sql`` or ``.py``) to the task that contains them.
"""

from __future__ import annotations

import ast
from pathlib import Path

from .sql_parser import ParseIssue, _rel


def _kw(call: ast.Call, name: str) -> str | None:
    for k in call.keywords:
        if k.arg == name and isinstance(k.value, ast.Constant) and isinstance(k.value.value, str):
            return k.value.value
    return None


def scan_airflow(paths: list[Path], root: Path) -> tuple[dict[str, list[dict[str, str]]], list[ParseIssue]]:
    """Return ``{file basename: [{dag_id, task_id, schedule, where}]}``."""
    out: dict[str, list[dict[str, str]]] = {}
    issues: list[ParseIssue] = []
    for base in paths:
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured Airflow path does not exist"))
            continue
        for f in sorted(base.rglob("*.py")) if base.is_dir() else [base]:
            rel = _rel(f, root)
            try:
                tree = ast.parse(f.read_text(encoding="utf-8"), filename=rel)
            except SyntaxError as e:
                issues.append(ParseIssue(rel, f"Python syntax error: {e}"))
                continue
            dag_id, schedule = None, None
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    fn = (
                        node.func.attr
                        if isinstance(node.func, ast.Attribute)
                        else getattr(node.func, "id", "")
                    )
                    if fn in {"DAG", "dag"}:
                        dag_id = _kw(node, "dag_id") or (
                            node.args[0].value
                            if node.args and isinstance(node.args[0], ast.Constant)
                            else dag_id
                        )
                        schedule = _kw(node, "schedule") or _kw(node, "schedule_interval") or schedule
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                task_id = _kw(node, "task_id")
                if not task_id:
                    continue
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                        v = sub.value.strip()
                        if v.endswith((".sql", ".py")) and "\n" not in v:
                            out.setdefault(Path(v).name, []).append(
                                {
                                    "dag_id": str(dag_id or "?"),
                                    "task_id": task_id,
                                    "schedule": str(schedule or "?"),
                                    "where": f"{rel}:{node.lineno}",
                                }
                            )
    return out, issues
