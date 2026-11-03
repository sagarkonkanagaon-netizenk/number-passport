"""Load and validate ``passport.yml``."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from .model import Metric

CONFIG_NAMES = ("passport.yml", "passport.yaml")
_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class ConfigError(ValueError):
    pass


def _expand_env(value: str) -> str:
    def repl(m: re.Match[str]) -> str:
        name, default = m.group(1), m.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ConfigError(f"environment variable {name} is not set (referenced in passport.yml)")

    return _ENV.sub(repl, value)


def _as_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [v]
    if isinstance(v, list) and all(isinstance(x, str) for x in v):
        return v
    raise ConfigError(f"expected a path or list of paths, got {v!r}")


@dataclass
class TableConfig:
    key: list[str] = field(default_factory=list)
    freshness_column: str | None = None
    max_age_hours: float | None = None


@dataclass
class Config:
    root: Path
    project: str
    dialect: str
    sql_paths: list[Path]
    dbt_models: list[Path]
    dbt_manifest: Path | None
    dbt_schema_map: dict[str, str]
    pyspark_paths: list[Path]
    airflow_paths: list[Path]
    connection_url: str | None
    privacy_mode: str
    safe_dimensions: set[str]
    tolerance_abs: Decimal
    tolerance_rel: Decimal
    tables: dict[str, TableConfig]
    metrics: dict[str, Metric]
    output_dir: Path
    as_of: str | None = None

    def table(self, name: str) -> TableConfig:
        return self.tables.get(name.lower(), TableConfig())


def find_config(start: Path) -> Path:
    start = start.resolve()
    if start.is_file():
        return start
    for d in (start, *start.parents):
        for n in CONFIG_NAMES:
            if (d / n).is_file():
                return d / n
    raise ConfigError(f"no passport.yml found in {start} or its parents (run `passport init`)")


def load_config(path: Path | str = ".") -> Config:
    cfg_path = find_config(Path(path))
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"{cfg_path}: invalid YAML: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"{cfg_path}: top level must be a mapping")
    return parse_config(raw, cfg_path.parent)


def parse_config(raw: dict[str, Any], root: Path) -> Config:
    known = {
        "project",
        "dialect",
        "code",
        "connection",
        "privacy",
        "tolerance",
        "tables",
        "metrics",
        "output_dir",
        "as_of",
    }
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(f"unknown top-level keys in passport.yml: {sorted(unknown)}")

    code = raw.get("code") or {}
    if not isinstance(code, dict):
        raise ConfigError("`code` must be a mapping")
    dbt = code.get("dbt") or {}
    if isinstance(dbt, str):
        dbt = {"models": dbt}

    def paths(v: Any) -> list[Path]:
        return [(root / p).resolve() for p in _as_list(v)]

    conn = raw.get("connection") or {}
    url = conn.get("url") if isinstance(conn, dict) else conn
    if url is not None:
        url = _expand_env(str(url))
        if url.startswith(("duckdb:///", "files:///")):
            scheme, rest = url.split(":///", 1)
            if not rest.startswith("/") and rest != ":memory:":
                url = f"{scheme}:///{(root / rest).resolve()}"

    privacy = raw.get("privacy") or {}
    mode = privacy.get("mode", "aggregates_only")
    if mode not in {"aggregates_only", "allow_dimensions"}:
        raise ConfigError("privacy.mode must be aggregates_only or allow_dimensions")

    tol = raw.get("tolerance") or {}
    tables: dict[str, TableConfig] = {}
    for name, t in (raw.get("tables") or {}).items():
        t = t or {}
        tables[name.lower()] = TableConfig(
            key=_as_list(t.get("key")),
            freshness_column=t.get("freshness_column"),
            max_age_hours=t.get("max_age_hours"),
        )

    metrics: dict[str, Metric] = {}
    for name, m in (raw.get("metrics") or {}).items():
        if not isinstance(m, dict) or "table" not in m or "column" not in m:
            raise ConfigError(f"metric {name!r} needs at least `table` and `column`")
        report = m.get("report")
        if report is not None:
            if not isinstance(report, dict):
                raise ConfigError(f"metric {name!r}: report must be a mapping")
            report = dict(report)
            if "file" in report:
                report["file"] = str((root / report["file"]).resolve())
        try:
            metrics[name] = Metric(
                name=name,
                table=m["table"].lower(),
                column=m["column"].lower(),
                agg=m.get("agg", "sum"),
                where=m.get("where"),
                label=m.get("label"),
                report=report,
            )
        except ValueError as e:
            raise ConfigError(str(e)) from e

    manifest = dbt.get("manifest")
    return Config(
        root=root,
        project=str(raw.get("project", root.name)),
        dialect=str(raw.get("dialect", "duckdb")),
        sql_paths=paths(code.get("sql")),
        dbt_models=paths(dbt.get("models")),
        dbt_manifest=(root / manifest).resolve() if manifest else None,
        dbt_schema_map={str(k): str(v) for k, v in (dbt.get("schema_map") or {}).items()},
        pyspark_paths=paths(code.get("pyspark")),
        airflow_paths=paths(code.get("airflow")),
        connection_url=url,
        privacy_mode=mode,
        safe_dimensions={d.lower() for d in _as_list(privacy.get("safe_dimensions"))},
        tolerance_abs=Decimal(str(tol.get("absolute", "0.01"))),
        tolerance_rel=Decimal(str(tol.get("relative", "0"))),
        tables=tables,
        metrics=metrics,
        output_dir=(root / raw.get("output_dir", "passports")).resolve(),
        as_of=raw.get("as_of"),
    )
