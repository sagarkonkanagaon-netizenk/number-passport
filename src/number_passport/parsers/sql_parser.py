"""Plain SQL files: ``CREATE TABLE ... AS``, ``CREATE VIEW ... AS``, ``INSERT INTO ... SELECT``."""

from __future__ import annotations

from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from ..model import Model, SourceRef
from . import referenced_tables, table_name


class ParseIssue(Exception):
    def __init__(self, where: str, message: str) -> None:
        super().__init__(f"{where}: {message}")
        self.where = where
        self.message = message


def _statement_line(text: str, stmt: exp.Expression, offset_lines: int) -> int:
    lines = [n.meta.get("line") for n in stmt.find_all(exp.Identifier) if n.meta.get("line")]
    return (min(lines) if lines else 1) + offset_lines


def models_from_sql(
    text: str, file: str, dialect: str, kind: str = "sql", line_offset: int = 0
) -> list[Model]:
    try:
        statements = sqlglot.parse(text, dialect=dialect)
    except ParseError as e:
        raise ParseIssue(file, f"SQL parse error: {e}") from e
    models: list[Model] = []
    for stmt in statements:
        if stmt is None:
            continue
        target: str | None = None
        query: exp.Expression | None = None
        if (
            isinstance(stmt, exp.Create)
            and stmt.kind in {"TABLE", "VIEW"}
            and isinstance(stmt.expression, exp.Query)
        ) or (isinstance(stmt, exp.Insert) and isinstance(stmt.expression, exp.Query)):
            tbl = stmt.this.this if isinstance(stmt.this, exp.Schema) else stmt.this
            if isinstance(tbl, exp.Table):
                target, query = table_name(tbl), stmt.expression
        if target is None or query is None:
            continue  # DDL, grants, comments: not a data-producing statement
        assert isinstance(query, exp.Query)
        models.append(
            Model(
                target=target,
                query=query,
                kind=kind,
                source=SourceRef(file, _statement_line(text, stmt, line_offset)),
                dialect=dialect,
                upstream=referenced_tables(query),
                line_offset=line_offset,
            )
        )
    return models


def scan_sql(paths: list[Path], root: Path, dialect: str) -> tuple[list[Model], list[ParseIssue]]:
    models: list[Model] = []
    issues: list[ParseIssue] = []
    for base in paths:
        files = sorted(base.rglob("*.sql")) if base.is_dir() else [base]
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured SQL path does not exist"))
            continue
        for f in files:
            rel = _rel(f, root)
            try:
                models.extend(models_from_sql(f.read_text(encoding="utf-8"), rel, dialect))
            except ParseIssue as e:
                issues.append(e)
    return models, issues


def _rel(p: Path, root: Path) -> str:
    try:
        return str(p.relative_to(root))
    except ValueError:
        return str(p)
