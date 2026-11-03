"""dbt models.

Two modes:

* **manifest** (preferred): read ``target/manifest.json`` produced by ``dbt compile`` and use each model's
  compiled SQL and relation name. Exact, because dbt already resolved every macro.
* **source**: read ``models/**/*.sql`` and resolve the common Jinja (``ref``, ``source``, ``config``,
  ``this``) ourselves. Anything else (custom macros, ``{% if %}`` blocks) is reported as opaque so the
  passport never pretends a model was reproduced faithfully when it was not.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..model import Model, SourceRef
from .sql_parser import ParseIssue, _rel, models_from_sql

_REF = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*['\"]([^'\"]+)['\"]\s*)?\)\s*\}\}")
_SOURCE = re.compile(r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_CONFIG = re.compile(r"\{\{\s*config\((.*?)\)\s*\}\}", re.S)
_CONFIG_SCHEMA = re.compile(r"schema\s*=\s*['\"]([^'\"]+)['\"]")
_CONFIG_ALIAS = re.compile(r"alias\s*=\s*['\"]([^'\"]+)['\"]")
_JINJA_EXPR = re.compile(r"\{\{.*?\}\}", re.S)
_JINJA_BLOCK = re.compile(r"\{%.*?%\}", re.S)
_JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.S)


def _blank(m: re.Match[str]) -> str:
    """Replace a match with spaces/newlines so line numbers stay correct."""
    return "".join("\n" if ch == "\n" else " " for ch in m.group(0))


def _model_identity(path: Path, base: Path, text: str, schema_map: dict[str, str]) -> tuple[str, str]:
    cfg = _CONFIG.search(text)
    schema = alias = None
    if cfg:
        if m := _CONFIG_SCHEMA.search(cfg.group(1)):
            schema = m.group(1)
        if m := _CONFIG_ALIAS.search(cfg.group(1)):
            alias = m.group(1)
    if schema is None:
        folder = path.parent.relative_to(base).parts[0] if path.parent != base else ""
        schema = schema_map.get(folder, folder or "main")
    return schema.lower(), (alias or path.stem).lower()


def scan_dbt_sources(
    paths: list[Path], root: Path, dialect: str, schema_map: dict[str, str]
) -> tuple[list[Model], list[ParseIssue]]:
    files: list[tuple[Path, Path]] = []
    issues: list[ParseIssue] = []
    for base in paths:
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured dbt models path does not exist"))
            continue
        files.extend((f, base) for f in sorted(base.rglob("*.sql")))

    texts = {f: f.read_text(encoding="utf-8") for f, _ in files}
    identity = {f: _model_identity(f, base, texts[f], schema_map) for f, base in files}
    by_name = {f.stem.lower(): ".".join(identity[f]) for f, _ in files}

    models: list[Model] = []
    for f, _ in files:
        text = texts[f]
        rel = _rel(f, root)
        opaque: list[str] = []

        def ref(m: re.Match[str], rel: str = rel) -> str:
            name = (m.group(2) or m.group(1)).lower()
            if name not in by_name:
                raise ParseIssue(rel, f"ref('{name}') does not match any model under the dbt models path")
            return by_name[name]

        try:
            sql = _JINJA_COMMENT.sub(_blank, text)
            sql = _CONFIG.sub(_blank, sql)
            sql = _REF.sub(ref, sql)
            sql = _SOURCE.sub(lambda m: f"{m.group(1)}.{m.group(2)}".lower(), sql)
            for m in _JINJA_BLOCK.finditer(sql):
                opaque.append(f"Jinja block {m.group(0).strip()[:60]!r} ignored")
            sql = _JINJA_BLOCK.sub(_blank, sql)
            for m in _JINJA_EXPR.finditer(sql):
                opaque.append(f"Jinja expression {m.group(0).strip()[:60]!r} replaced with NULL")
            sql = _JINJA_EXPR.sub("NULL", sql)
            schema, name = identity[f]
            wrapped = f"CREATE TABLE {schema}.{name} AS\n{sql.strip().rstrip(';')}"
            # the wrapper adds one line at the top; shift reported lines back by one
            for model in models_from_sql(wrapped, rel, dialect, kind="dbt", line_offset=-1):
                model.opaque.extend(opaque)
                models.append(model)
        except ParseIssue as e:
            issues.append(e)
    return models, issues


def scan_dbt_manifest(manifest: Path, root: Path, dialect: str) -> tuple[list[Model], list[ParseIssue]]:
    if not manifest.is_file():
        return [], [ParseIssue(str(manifest), "dbt manifest not found (run `dbt compile`)")]
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [], [ParseIssue(str(manifest), f"invalid JSON: {e}")]
    models: list[Model] = []
    issues: list[ParseIssue] = []
    for node in (data.get("nodes") or {}).values():
        if node.get("resource_type") != "model":
            continue
        compiled = node.get("compiled_code") or node.get("compiled_sql")
        path = node.get("original_file_path", node.get("unique_id", "?"))
        if not compiled:
            issues.append(ParseIssue(path, "model has no compiled SQL in the manifest (run `dbt compile`)"))
            continue
        schema = (node.get("schema") or "main").lower()
        name = (node.get("alias") or node.get("name")).lower()
        try:
            for model in models_from_sql(
                f"CREATE TABLE {schema}.{name} AS\n{compiled.strip().rstrip(';')}",
                path,
                dialect,
                kind="dbt",
                line_offset=-1,
            ):
                # compiled SQL line numbers do not match the Jinja source, so point at the file only
                model.source = SourceRef(path)
                models.append(model)
        except ParseIssue as e:
            issues.append(e)
    return models, issues
