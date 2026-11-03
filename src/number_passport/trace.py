"""Walk a metric from the report cell down to source tables, one hop at a time."""

from __future__ import annotations

import csv
import hashlib
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import __version__
from .config import Config
from .connectors import Connector, ConnectorError
from .guard import Guard, GuardViolation
from .model import Finding, Hop, Metric, Severity
from .project import Project, load_project
from .quality import check_table
from .reconcile import HopAnalyzer, NextHop, Tolerance, fmt, money, to_decimal

MAX_HOPS = 50


class TraceError(Exception):
    pass


@dataclass
class Passport:
    metric: Metric
    project: str
    generated_at: str
    as_of: str | None
    connection: str | None
    dialect: str | None
    mode: str  # full | code_only
    report_hop: Hop | None
    hops: list[Hop]
    edges: list[tuple[str, str]]
    code_files: dict[str, str]
    audit: list[dict[str, Any]] = field(default_factory=list)
    parse_issues: list[str] = field(default_factory=list)
    tool_version: str = __version__

    @property
    def all_findings(self) -> list[Finding]:
        out: list[Finding] = []
        for h in ([self.report_hop] if self.report_hop else []) + self.hops:
            out.extend(h.findings)
        return out

    @property
    def status(self) -> str:
        if self.mode == "code_only":
            return "lineage_only"
        hops = ([self.report_hop] if self.report_hop else []) + self.hops
        if any(f.severity == Severity.CRITICAL for f in self.all_findings) or any(
            h.status in {"gap", "error"} for h in hops
        ):
            return "gaps_found"
        if any(h.status == "not_reproducible" for h in hops):
            return "partially_verified"
        return "verified"

    def to_dict(self) -> dict[str, Any]:
        crit = sum(f.severity == Severity.CRITICAL for f in self.all_findings)
        warn = sum(f.severity == Severity.WARNING for f in self.all_findings)
        return {
            "passport_version": 1,
            "tool": {"name": "number-passport", "version": self.tool_version},
            "metric": {
                "name": self.metric.name,
                "label": self.metric.label,
                "table": self.metric.table,
                "column": self.metric.column,
                "agg": self.metric.agg,
                "where": self.metric.where,
            },
            "project": self.project,
            "generated_at": self.generated_at,
            "as_of": self.as_of,
            "connection": self.connection,
            "dialect": self.dialect,
            "mode": self.mode,
            "status": self.status,
            "summary": {"hops": len(self.hops), "critical": crit, "warnings": warn},
            "report_hop": self.report_hop.to_dict() if self.report_hop else None,
            "hops": [h.to_dict() for h in self.hops],
            "edges": [list(e) for e in self.edges],
            "code_files": self.code_files,
            "parse_issues": self.parse_issues,
            "audit": self.audit,
        }


def read_report_value(report: dict[str, Any]) -> tuple[Decimal, str]:
    """A figure as printed on a report: a literal, or a cell in a CSV file (row label x column header)."""
    if "value" in report:
        return to_decimal(str(report["value"]).replace(",", "")), str(
            report.get("source", "configured value")
        )
    path = Path(report["file"])
    if not path.is_file():
        raise TraceError(f"report file not found: {path}")
    if path.suffix.lower() != ".csv":
        raise TraceError(f"report file must be .csv (got {path.name}); export the report sheet to CSV")
    row_label, col = str(report["row"]), str(report["column"])
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or col not in reader.fieldnames:
            raise TraceError(f"{path.name}: column {col!r} not found (have {reader.fieldnames})")
        label_col = reader.fieldnames[0]
        for i, r in enumerate(reader, start=2):
            if (r.get(label_col) or "").strip() == row_label:
                raw = (r.get(col) or "").replace(",", "").replace("$", "").strip()
                return to_decimal(raw), f"{path.name} row {i} ({row_label!r}), column {col!r}"
    raise TraceError(f"{path.name}: no row labelled {row_label!r}")


def _hash_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


class Tracer:
    def __init__(self, cfg: Config, connector: Connector | None) -> None:
        self.cfg = cfg
        self.conn = connector
        self.guard: Guard | None = None
        schema: dict[str, list[str]] = {}
        if connector is not None:
            self.guard = Guard(connector.query, connector.dialect, cfg.privacy_mode, cfg.safe_dimensions)
        self.project: Project = load_project(cfg)
        if connector is not None:
            schema = connector.schema(self.project.tables())
            # reload PySpark jobs now that column lists are known (exact withColumn/drop translation)
            self.project = load_project(cfg, schema)
        self.schema = schema
        self.tol = Tolerance(cfg.tolerance_abs, cfg.tolerance_rel)

    def _as_of(self) -> datetime | None:
        if not self.cfg.as_of:
            return None
        d = datetime.fromisoformat(str(self.cfg.as_of))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)

    def trace(self, metric: Metric) -> Passport:
        p = self.project
        if p.model_for(metric.table) is None and metric.table not in p.tables():
            raise TraceError(f"table {metric.table} is not produced or read by any code in this project")
        code_only = self.guard is None
        hops: list[Hop] = []
        edges: list[tuple[str, str]] = []
        files: set[Path] = set()
        queue: deque[tuple[NextHop, str | None, int]] = deque(
            [(NextHop(metric.table, metric.column), metric.where, 0)]
        )
        seen: set[tuple[str, str]] = set()
        quality_done: set[str] = set()

        while queue:
            nh, where, depth = queue.popleft()
            if (nh.table, nh.column) in seen:
                continue
            seen.add((nh.table, nh.column))
            if len(hops) >= MAX_HOPS:
                hops[-1].findings.append(
                    Finding(Severity.WARNING, "trace_truncated", f"stopped after {MAX_HOPS} hops", nh.table)
                )
                break
            model = p.model_for(nh.table)
            index = len(hops) + 1
            existing = next((h for h in hops if h.target == nh.table and h.model_kind == "source"), None)
            if model is None and existing is not None:
                # another column of a source table we already show: list it there instead of a new hop
                ref = f"{nh.table}.{nh.column}"
                if ref not in existing.factors:
                    existing.factors.append(ref)
                continue
            if model is None:
                hops.append(self._source_hop(nh, index, code_only, metric.agg))
            else:
                files.add((self.cfg.root / model.source.file).resolve())
                an = HopAnalyzer(model, self.guard or Guard(_no_db, self.cfg.dialect), self.schema, self.tol)
                result = (
                    an.describe(nh.column, metric.agg, index)
                    if code_only
                    else an.analyze(nh.column, metric.agg, where, index)
                )
                result.hop.role = nh.role
                hops.append(result.hop)
                for up in model.upstream:
                    if (up, model.target) not in edges:
                        edges.append((up, model.target))
                for nxt in result.next_hops:
                    if depth + 1 < MAX_HOPS:
                        queue.append((nxt, None, depth + 1))
                if not result.next_hops and model.upstream:
                    result.hop.findings.append(
                        Finding(
                            Severity.WARNING,
                            "trace_stops",
                            f"no single source column carries {nh.column} upstream (it is built from "
                            "joins or constants); upstream tables are listed as lineage only",
                            str(model.source),
                        )
                    )
            if not code_only and nh.table not in quality_done:
                quality_done.add(nh.table)
                self._quality(hops[-1], nh)

        report_hop = None
        if metric.report and not code_only and hops and hops[0].target_value is not None:
            report_hop = self._report_hop(metric, hops[0])
        for f in list(files):
            if not f.is_file():
                files.discard(f)
        if metric.report and "file" in metric.report and Path(metric.report["file"]).is_file():
            files.add(Path(metric.report["file"]))
        code_files = {}
        for f in sorted(files):
            try:
                key = str(f.relative_to(self.cfg.root))
            except ValueError:
                key = str(f)
            code_files[key] = _hash_file(f)
        return Passport(
            metric=metric,
            project=self.cfg.project,
            generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            as_of=self.cfg.as_of,
            connection=self.conn.url_display if self.conn else None,
            dialect=self.conn.dialect if self.conn else None,
            mode="code_only" if code_only else "full",
            report_hop=report_hop,
            hops=hops,
            edges=edges,
            code_files=code_files,
            audit=[e.to_dict() for e in self.guard.log] if self.guard else [],
            parse_issues=[str(i) for i in p.issues],
        )

    def _source_hop(self, nh: NextHop, index: int, code_only: bool, agg: str) -> Hop:
        measure = f"{agg.upper()}({nh.column})" if nh.role == "primary" else nh.column
        hop = Hop(
            index=index,
            target=nh.table,
            measure=measure,
            target_value=None,
            produced_by="source system (no code in this project builds it)",
            model_kind="source",
            status="source" if not code_only else "code_only",
            role=nh.role,
        )
        if not code_only and self.guard is not None and nh.role == "primary":
            fn = "COUNT(*)" if agg == "count" else f"SUM({nh.column})"
            try:
                v = self.guard.scalar(
                    f"SELECT {fn} AS v FROM {nh.table}", f"hop {index}: source total {nh.table}.{nh.column}"
                )
                hop.target_value = fmt(to_decimal(v))
            except (ConnectorError, GuardViolation, ValueError) as e:
                hop.status = "error"
                hop.findings.append(Finding(Severity.CRITICAL, "source_unreadable", str(e), nh.table))
        return hop

    def _quality(self, hop: Hop, nh: NextHop) -> None:
        assert self.guard is not None and self.conn is not None
        cols = self.schema.get(nh.table) or []
        res, findings = check_table(
            self.guard,
            nh.table,
            nh.column,
            self.cfg.table(nh.table),
            self.project.model_for(nh.table),
            cols,
            self._as_of(),
        )
        hop.quality.append(res)
        hop.findings.extend(findings)

    def _report_hop(self, metric: Metric, top: Hop) -> Hop:
        assert metric.report is not None
        hop = Hop(
            index=0,
            target=f"report: {metric.label or metric.name}",
            measure=metric.name,
            target_value=None,
            produced_by=None,
            model_kind="report",
        )
        try:
            reported, where = read_report_value(metric.report)
        except (TraceError, ValueError) as e:
            hop.status = "error"
            hop.findings.append(Finding(Severity.CRITICAL, "report_unreadable", str(e)))
            return hop
        hop.produced_by = where
        hop.target_value = fmt(reported)
        hop.recomputed_value = top.target_value
        gold = to_decimal(top.target_value)
        hop.residual = fmt(reported - gold)
        if self.tol.ok(reported, gold):
            hop.status = "reconciled"
        else:
            hop.status = "gap"
            hop.findings.append(
                Finding(
                    Severity.CRITICAL,
                    "report_mismatch",
                    f"the report shows {money(reported)} but {top.target} totals {money(gold)} "
                    f"(difference {money(reported - gold)}): a manual adjustment or a report built "
                    "from another source; it must be documented or corrected",
                    where,
                )
            )
        return hop


def _no_db(sql: str) -> list[tuple[Any, ...]]:
    raise ConnectorError("no database connection configured (code-only mode)")
