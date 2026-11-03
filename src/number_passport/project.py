"""Load every model in a code base into one lineage graph."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from .config import Config
from .model import Model, SourceRef
from .parsers import referenced_tables
from .parsers.airflow_parser import scan_airflow
from .parsers.dbt_parser import scan_dbt_manifest, scan_dbt_sources
from .parsers.pyspark_parser import scan_pyspark
from .parsers.sas_parser import scan_sas
from .parsers.sql_parser import ParseIssue, scan_sql

if TYPE_CHECKING:
    from .connectors import Connector


class LineageError(Exception):
    pass


@dataclass
class Project:
    models: dict[str, Model]
    issues: list[ParseIssue] = field(default_factory=list)
    duplicates: dict[str, list[str]] = field(default_factory=dict)
    warehouse_views: dict[str, str] = field(default_factory=dict)  # view -> sha256 of its definition

    def model_for(self, table: str) -> Model | None:
        return self.models.get(table.lower())

    def upstream(self, table: str) -> list[str]:
        m = self.model_for(table)
        return list(m.upstream) if m else []

    def downstream(self, table: str) -> list[str]:
        t = table.lower()
        return sorted(name for name, m in self.models.items() if t in m.upstream)

    def ancestors(self, table: str) -> list[str]:
        seen: list[str] = []
        stack = [table.lower()]
        while stack:
            for up in self.upstream(stack.pop()):
                if up not in seen:
                    seen.append(up)
                    stack.append(up)
        return seen

    def tables(self) -> list[str]:
        names = set(self.models)
        for m in self.models.values():
            names.update(m.upstream)
        return sorted(names)

    def cycles(self) -> list[list[str]]:
        found: list[list[str]] = []
        state: dict[str, int] = {}

        def visit(n: str, path: list[str]) -> None:
            state[n] = 1
            for up in self.upstream(n):
                if state.get(up) == 1:
                    found.append([*path[path.index(up) :], up] if up in path else [n, up])
                elif state.get(up) is None:
                    visit(up, [*path, up])
            state[n] = 2

        for n in self.models:
            if n not in state:
                visit(n, [n])
        return found


def load_project(cfg: Config, schema: dict[str, list[str]] | None = None) -> Project:
    found: list[Model] = []
    issues: list[ParseIssue] = []

    def take(res: tuple[Iterable[Model], Iterable[ParseIssue]]) -> None:
        found.extend(res[0])
        issues.extend(res[1])

    take(scan_sql(cfg.sql_paths, cfg.root, cfg.dialect))
    if cfg.dbt_manifest is not None:
        take(scan_dbt_manifest(cfg.dbt_manifest, cfg.root, cfg.dialect))
    else:
        take(scan_dbt_sources(cfg.dbt_models, cfg.root, cfg.dialect, cfg.dbt_schema_map))
    take(scan_pyspark(cfg.pyspark_paths, cfg.root, cfg.dialect, schema))
    take(scan_sas(cfg.sas_paths, cfg.root, cfg.sas_libnames, schema))
    orchestration, a_issues = scan_airflow(cfg.airflow_paths, cfg.root)
    issues.extend(a_issues)

    models: dict[str, Model] = {}
    duplicates: dict[str, list[str]] = {}
    for m in found:
        m.orchestration = orchestration.get(Path(m.source.file).name, [])
        if m.target in models:
            duplicates.setdefault(m.target, [str(models[m.target].source)]).append(str(m.source))
            continue
        models[m.target] = m
    return Project(models=models, issues=issues, duplicates=duplicates)


def view_model(name: str, definition: str, dialect: str) -> Model | None:
    """A view defined only in the warehouse, as a model (its definition comes from the catalog)."""
    try:
        node = sqlglot.parse_one(definition.strip().rstrip(";"), dialect=dialect)
    except (ParseError, TokenError):
        return None
    query = node.expression if isinstance(node, exp.Create) else node
    if not isinstance(query, exp.Query):
        return None
    return Model(
        target=name,
        query=query,
        kind="warehouse view",
        source=SourceRef(f"warehouse view {name}"),
        dialect=dialect,
        upstream=[u for u in referenced_tables(query) if u != name],
        notes=["definition read from the warehouse catalog (no pipeline file in this project builds it)"],
    )


def add_warehouse_views(project: Project, connector: Connector, extra: Iterable[str] = ()) -> None:
    """Add warehouse views that the code reads (or the metric names) but no pipeline file defines.

    Walks upward: a view over another view is followed too. Only catalog metadata is read.
    """
    tried: set[str] = set()
    frontier = [*project.tables(), *extra]
    for _ in range(50):
        frontier = [t for t in dict.fromkeys(frontier) if t not in tried and t not in project.models]
        if not frontier:
            return
        tried.update(frontier)
        found: list[str] = []
        for name, sql in connector.views(frontier).items():
            m = view_model(name, sql, connector.dialect)
            if m is None:
                project.issues.append(ParseIssue(f"warehouse view {name}", "could not parse its definition"))
                continue
            project.models[name] = m
            project.warehouse_views[name] = hashlib.sha256(sql.encode()).hexdigest()
            found.extend(m.upstream)
        frontier = found
