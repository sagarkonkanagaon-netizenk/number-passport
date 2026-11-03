"""One API for the CLI and the MCP server, so both always behave the same."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Config, ConfigError, load_config
from .connectors import Connector, connect
from .evidence import verify_pack, write_pack
from .guard import Guard
from .model import Metric
from .project import load_project
from .reconcile import HopAnalyzer, Tolerance
from .trace import Passport, TraceError, Tracer


@contextmanager
def open_project(path: str | Path, offline: bool = False) -> Iterator[tuple[Config, Connector | None]]:
    cfg = load_config(path)
    conn = None if offline or cfg.connection_url is None else connect(cfg.connection_url)
    try:
        yield cfg, conn
    finally:
        if conn is not None:
            conn.close()


def resolve_metric(
    cfg: Config,
    metric: str | None = None,
    table: str | None = None,
    column: str | None = None,
    agg: str = "sum",
    where: str | None = None,
) -> Metric:
    if metric:
        if metric not in cfg.metrics:
            raise ConfigError(f"unknown metric {metric!r}; configured: {', '.join(cfg.metrics) or 'none'}")
        return cfg.metrics[metric]
    if not (table and column):
        raise ConfigError("give a configured metric name, or both table and column")
    return Metric(
        name=f"{agg}_{table}_{column}".replace(".", "_"),
        table=table.lower(),
        column=column.lower(),
        agg=agg,
        where=where,
    )


def pack_dir(cfg: Config, metric: Metric) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", metric.name)
    return cfg.output_dir / f"{safe}-{stamp}"


def trace(
    path: str | Path,
    metric: str | None = None,
    table: str | None = None,
    column: str | None = None,
    agg: str = "sum",
    where: str | None = None,
    out: str | Path | None = None,
    offline: bool = False,
) -> tuple[Passport, Path]:
    with open_project(path, offline) as (cfg, conn):
        m = resolve_metric(cfg, metric, table, column, agg, where)
        passport = Tracer(cfg, conn).trace(m)
        target = Path(out) if out else pack_dir(cfg, m)
        write_pack(passport, target)
        return passport, target


def summary(passport: Passport, pack: Path | None = None) -> dict[str, Any]:
    """Compact, model-friendly digest of a passport (the full detail lives in the pack)."""
    d = passport.to_dict()
    hops = []
    for h in ([d["report_hop"]] if d["report_hop"] else []) + d["hops"]:
        hops.append(
            {
                "hop": h["index"],
                "table": h["target"],
                "role": h["role"],
                "measure": h["measure"],
                "value": h["target_value"],
                "recomputed": h["recomputed_value"],
                "unexplained": h["residual"],
                "status": h["status"],
                "built_by": h["produced_by"],
                "steps": [
                    {
                        "what": s["description"],
                        "where": s["where"],
                        "rows_removed": s["rows_removed"],
                        "value_effect": s["value_delta"],
                        "native_value_dropped": s["native_value_removed"],
                    }
                    for s in h["steps"]
                ],
                "findings": [f"{f['severity'].upper()}: {f['message']}" for f in h["findings"]],
            }
        )
    return {
        "metric": d["metric"],
        "status": d["status"],
        "summary": d["summary"],
        "hops": hops,
        "queries_run": len(d["audit"]),
        "evidence_pack": str(pack) if pack else None,
        "html": str(pack / "passport.html") if pack else None,
    }


def lineage(path: str | Path, table: str) -> dict[str, Any]:
    cfg = load_config(path)
    project = load_project(cfg)
    t = table.lower()
    m = project.model_for(t)
    out: dict[str, Any] = {
        "table": t,
        "upstream": project.upstream(t),
        "downstream": project.downstream(t),
        "all_ancestors": project.ancestors(t),
        "built_by": None,
    }
    if m is not None:
        an = HopAnalyzer(m, Guard(lambda s: [], cfg.dialect), {}, Tolerance())
        out["built_by"] = {
            "file": str(m.source),
            "kind": m.kind,
            "orchestration": m.orchestration,
            "row_removing_steps": [
                {"kind": s.kind.value, "what": s.description, "where": an._where(s.line)} for s in an.steps
            ],
            "untranslatable": m.opaque,
            "notes": m.notes,
        }
    elif t not in project.tables():
        raise TraceError(f"{t} is not produced or read by any code in this project")
    return out


def metrics(path: str | Path) -> list[dict[str, Any]]:
    cfg = load_config(path)
    return [
        {
            "name": m.name,
            "label": m.label,
            "table": m.table,
            "column": m.column,
            "agg": m.agg,
            "where": m.where,
            "has_report_cell": bool(m.report),
        }
        for m in cfg.metrics.values()
    ]


def verify(pack: str | Path, path: str | Path | None = None, offline: bool = False) -> dict[str, Any]:
    pack = Path(pack)
    if path is None:
        res = verify_pack(pack, None, None)
        return res.to_dict()
    with open_project(path, offline) as (cfg, conn):
        return verify_pack(pack, cfg.root, conn).to_dict()
