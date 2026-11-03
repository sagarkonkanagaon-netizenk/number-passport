"""Data quality evidence for each table on the trace (BCBS 239 principles 3-5: accuracy, completeness,
timeliness). Aggregate queries only."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlglot import exp

from .config import TableConfig
from .connectors import ConnectorError
from .guard import Guard, GuardViolation
from .model import Finding, Model, QualityResult, Severity
from .reconcile import to_decimal


def inferred_key(model: Model | None) -> list[str]:
    """If the producing code dedups with QUALIFY ROW_NUMBER() OVER (PARTITION BY k ...), k is the grain."""
    if model is None:
        return []
    outer = model.query if isinstance(model.query, exp.Select) else None
    for sel in [outer] if outer is not None else []:
        q = sel.args.get("qualify")
        if q is None:
            continue
        for w in q.find_all(exp.Window):
            parts = w.args.get("partition_by") or []
            if parts and all(isinstance(p, exp.Column) for p in parts):
                return [p.name.lower() for p in parts]
    return []


def _parse_ts(v: object) -> datetime | None:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            d = datetime.fromisoformat(v)
        except ValueError:
            return None
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def check_table(
    guard: Guard,
    table: str,
    measure: str | None,
    cfg: TableConfig,
    model: Model | None,
    columns: list[str],
    as_of: datetime | None = None,
) -> tuple[QualityResult, list[Finding]]:
    findings: list[Finding] = []
    res = QualityResult(table=table, row_count=None)
    d = guard.dialect

    def q(sql: str) -> str:
        return exp.maybe_parse(sql, dialect="duckdb").sql(dialect=d)

    try:
        res.row_count = int(
            to_decimal(guard.scalar(q(f"SELECT COUNT(*) AS n FROM {table}"), f"quality: row count {table}"))
        )
    except (ConnectorError, GuardViolation) as e:
        findings.append(Finding(Severity.WARNING, "quality_unavailable", f"{table}: {e}", table))
        return res, findings
    res.checks.append(
        {"check": "row_count", "value": res.row_count, "status": "pass" if res.row_count else "fail"}
    )
    if res.row_count == 0:
        findings.append(Finding(Severity.CRITICAL, "empty_table", f"{table} has no rows", table))

    if measure and measure in columns:
        n = int(
            to_decimal(
                guard.scalar(
                    q(f"SELECT COUNT(*) - COUNT({measure}) AS n FROM {table}"),
                    f"quality: nulls in {table}.{measure}",
                )
            )
        )
        res.measure_null_count = n
        res.checks.append(
            {"check": f"nulls in {measure}", "value": n, "status": "pass" if n == 0 else "warn"}
        )
        if n:
            findings.append(
                Finding(
                    Severity.WARNING,
                    "null_measure",
                    f"{n} row(s) in {table} have NULL {measure}; SUM ignores them silently",
                    table,
                )
            )

    key = cfg.key or inferred_key(model)
    key = [k for k in key if k in columns]
    if key:
        cols = ", ".join(key)
        n = int(
            to_decimal(
                guard.scalar(
                    q(
                        f"SELECT COUNT(*) AS n FROM (SELECT {cols} FROM {table} "
                        f"GROUP BY {cols} HAVING COUNT(*) > 1) AS d"
                    ),
                    f"quality: duplicate keys {table}({cols})",
                )
            )
        )
        res.duplicate_keys, res.key = n, key
        res.checks.append({"check": f"unique ({cols})", "value": n, "status": "pass" if n == 0 else "warn"})
        if n:
            findings.append(
                Finding(
                    Severity.WARNING,
                    "duplicate_keys",
                    f"{n} value(s) of ({cols}) appear more than once in {table}",
                    table,
                )
            )

    if cfg.freshness_column and cfg.freshness_column in columns:
        latest = guard.scalar(
            q(f"SELECT MAX({cfg.freshness_column}) AS v FROM {table}"),
            f"quality: freshness {table}.{cfg.freshness_column}",
        )
        res.freshness_column = cfg.freshness_column
        res.latest_load = str(latest) if latest is not None else None
        status = "pass"
        ts = _parse_ts(latest)
        if cfg.max_age_hours is not None and ts is not None:
            now = as_of or datetime.now(timezone.utc)
            age_h = (now - ts).total_seconds() / 3600
            if age_h > cfg.max_age_hours:
                status = "fail"
                findings.append(
                    Finding(
                        Severity.WARNING,
                        "stale_data",
                        f"{table} last loaded {res.latest_load} ({age_h:.1f}h before the "
                        f"as-of time), above the {cfg.max_age_hours}h limit",
                        table,
                    )
                )
        res.checks.append(
            {"check": f"latest {cfg.freshness_column}", "value": res.latest_load, "status": status}
        )
    return res, findings
