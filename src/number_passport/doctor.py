"""``passport doctor``: check every connection point before anyone relies on a passport."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlglot import exp

from .config import Config, ConfigError, load_config
from .connectors import Connector, ConnectorError, connect
from .guard import Guard, GuardViolation, check_query
from .project import load_project
from .trace import TraceError, read_report_value


@dataclass
class Check:
    name: str
    status: str  # pass | warn | fail | skip
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def run_doctor(path: str | Path = ".", connector: Connector | None = None) -> list[Check]:
    checks: list[Check] = []

    def add(name: str, status: str, detail: str) -> None:
        checks.append(Check(name, status, detail))

    # 1. configuration
    try:
        cfg: Config = load_config(path)
    except ConfigError as e:
        add("config", "fail", str(e))
        return checks
    add(
        "config",
        "pass",
        f"{cfg.root / 'passport.yml'}: project {cfg.project!r}, {len(cfg.metrics)} metric(s)",
    )

    # 2. code paths
    configured = (
        [("sql", p) for p in cfg.sql_paths]
        + [("dbt", p) for p in cfg.dbt_models]
        + [("pyspark", p) for p in cfg.pyspark_paths]
        + [("airflow", p) for p in cfg.airflow_paths]
    )
    if cfg.dbt_manifest:
        configured.append(("dbt manifest", cfg.dbt_manifest))
    if not configured:
        add("code paths", "fail", "no code paths configured under `code:`")
    for kind, p in configured:
        add(
            f"code path ({kind})",
            "pass" if p.exists() else "fail",
            f"{p}" + ("" if p.exists() else " does not exist"),
        )

    # 3. database
    own = connector is None
    conn = connector
    if cfg.connection_url is None and conn is None:
        add("connection", "warn", "no connection configured: passports will be lineage-only")
    else:
        try:
            if conn is None:
                assert cfg.connection_url is not None
                conn = connect(cfg.connection_url)
            t0 = time.perf_counter()
            conn.ping()
            add(
                "connection",
                "pass",
                f"{conn.url_display} ({conn.dialect}), ping {(time.perf_counter() - t0) * 1000:.0f} ms",
            )
        except ConnectorError as e:
            add("connection", "fail", str(e))
            conn = None
    if conn is not None:
        try:
            wrote = conn.probe_write()
        except ConnectorError as e:
            add("read-only", "warn", f"could not probe write access: {e}")
        else:
            add(
                "read-only",
                "fail" if wrote else "pass",
                "the connection accepted a write; use a read-only role"
                if wrote
                else "writes are rejected by the database",
            )

    # 4. code parsing
    schema: dict[str, list[str]] = {}
    project = load_project(cfg)
    if conn is not None:
        schema = conn.schema(project.tables())
        project = load_project(cfg, schema)
    kinds: dict[str, int] = {}
    for m in project.models.values():
        kinds[m.kind] = kinds.get(m.kind, 0) + 1
    if project.models:
        add("models", "pass", ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())) + " model(s) parsed")
    else:
        add("models", "fail", "no models found in the configured code paths")
    for issue in project.issues:
        add("parse", "fail", str(issue))
    for t, where in project.duplicates.items():
        add(
            "duplicate producers",
            "warn",
            f"{t} is written by {len(where)} pieces of code: {', '.join(where)}",
        )
    for cyc in project.cycles():
        add("lineage cycle", "fail", " -> ".join(cyc))
    for m in project.models.values():
        for o in m.opaque:
            add(f"translation {m.target}", "warn", o)
    scheduled = sum(1 for m in project.models.values() if m.orchestration)
    if cfg.airflow_paths:
        add(
            "orchestration",
            "pass" if scheduled else "warn",
            f"{scheduled} of {len(project.models)} model(s) matched to an Airflow task",
        )

    # 5. schema coverage
    if conn is not None:
        missing = [t for t in project.tables() if t not in schema]
        add(
            "tables",
            "pass" if not missing else "fail",
            f"all {len(project.tables())} tables found"
            if not missing
            else f"missing in database: {', '.join(missing)}",
        )

    # 6. guard self-test
    dialect = conn.dialect if conn is not None else cfg.dialect
    bad = [
        "DROP TABLE x",
        "SELECT * FROM t",
        "SELECT a FROM t",
        "DELETE FROM t",
        "COPY t TO 'x.csv'",
        "SELECT SUM(a) AS v FROM t; DROP TABLE t",
        "SELECT a, SUM(b) AS v FROM t GROUP BY a",
    ]
    leaked = []
    for q in bad:
        try:
            check_query(q, "duckdb", "aggregates_only", set())
            leaked.append(q)
        except GuardViolation:
            pass
    try:
        check_query("SELECT SUM(a) AS v, COUNT(*) AS n FROM t", "duckdb", "aggregates_only", set())
    except GuardViolation as e:
        leaked.append(f"rejected a valid aggregate query: {e}")
    add(
        "privacy guard",
        "fail" if leaked else "pass",
        f"let through: {leaked}"
        if leaked
        else f"blocked {len(bad)} unsafe queries, allowed aggregates (mode {cfg.privacy_mode})",
    )

    # 7. every model re-runs in the warehouse dialect
    if conn is not None:
        guard = Guard(conn.query, dialect, cfg.privacy_mode, cfg.safe_dimensions)
        for m in project.models.values():
            if m.opaque:
                add(f"replay {m.target}", "warn", "skipped: code not fully translatable")
                continue
            sql = exp.select(exp.Count(this=exp.Star()).as_("n")).from_(m.query.copy().subquery("q"))
            try:
                n = guard.scalar(sql.sql(dialect=dialect), f"doctor: replay {m.target}")
                add(f"replay {m.target}", "pass", f"{m.source} runs in {dialect} ({n} rows)")
            except (ConnectorError, GuardViolation) as e:
                add(f"replay {m.target}", "fail", f"{m.source}: {e}")

    # 8. metrics
    for name, met in cfg.metrics.items():
        problems = []
        if project.model_for(met.table) is None and met.table not in project.tables():
            problems.append(f"table {met.table} is not in the code")
        if conn is not None and met.table in schema and met.column not in schema[met.table]:
            problems.append(f"column {met.column} not in {met.table}")
        if met.report:
            try:
                _v, cell = read_report_value(met.report)
            except (TraceError, ValueError, KeyError) as e:
                problems.append(f"report: {e}")
        add(
            f"metric {name}",
            "fail" if problems else "pass",
            "; ".join(problems)
            if problems
            else f"{met.agg}({met.table}.{met.column})" + (f", report cell {cell}" if met.report else ""),
        )

    # 9. output and MCP
    try:
        cfg.output_dir.mkdir(parents=True, exist_ok=True)
        probe = cfg.output_dir / ".write-test"
        probe.write_text("ok")
        probe.unlink()
        add("output folder", "pass", str(cfg.output_dir))
    except OSError as e:
        add("output folder", "fail", f"{cfg.output_dir}: {e}")
    try:
        from .mcp_server import build_server

        srv = build_server(str(cfg.root))
        add("MCP server", "pass", f"{srv.name} builds; tools registered")
    except ImportError:
        add("MCP server", "warn", "MCP SDK not installed: pip install 'number-passport[mcp]'")

    if own and conn is not None:
        conn.close()
    return checks


def summarize(checks: list[Check]) -> str:
    return (
        "fail"
        if any(c.status == "fail" for c in checks)
        else ("warn" if any(c.status == "warn" for c in checks) else "pass")
    )
