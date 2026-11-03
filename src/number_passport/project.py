"""Load every model in a code base into one lineage graph."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from .config import Config
from .model import Model
from .parsers.airflow_parser import scan_airflow
from .parsers.dbt_parser import scan_dbt_manifest, scan_dbt_sources
from .parsers.pyspark_parser import scan_pyspark
from .parsers.sql_parser import ParseIssue, scan_sql


class LineageError(Exception):
    pass


@dataclass
class Project:
    models: dict[str, Model]
    issues: list[ParseIssue] = field(default_factory=list)
    duplicates: dict[str, list[str]] = field(default_factory=dict)

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
