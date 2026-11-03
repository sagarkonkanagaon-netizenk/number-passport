"""The privacy and safety guard every database query passes through.

Rules (enforced, not advisory):

1. Exactly one statement, and it must be a query (``SELECT`` / ``WITH``). No DDL, DML, ``COPY``,
   ``ATTACH``, ``PRAGMA``, ``SET``, procedure calls or anything sqlglot cannot classify.
2. In ``aggregates_only`` mode every output column of the outermost query must be an aggregate (or
   arithmetic over aggregates / literals), so the result can never contain a customer row.
3. ``GROUP BY`` on the outermost query is only allowed on columns listed in
   ``privacy.safe_dimensions`` and only in ``allow_dimensions`` mode.
4. Every query, its purpose and its result are appended to the audit log that ships inside the
   evidence pack, so an auditor can see exactly what left the database.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

_FORBIDDEN = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Command,
    exp.Merge,
    exp.Set,
    exp.Pragma,
    exp.Use,
    exp.Copy,
    exp.Attach,
    exp.Detach,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Into,
)
_BLOCKED_FUNCS = (
    "read_",
    "scan",
    "copy",
    "system",
    "pg_read",
    "pg_ls",
    "lo_",
    "dblink",
    "sleep",
    "load_",
    "glob",
    "query_table",
    "sniff",
    "http",
    "s3",
    "url",
    "file",
    "export",
    "import",
    "attach",
)
_AGG_FUNCS = (exp.Sum, exp.Count, exp.Min, exp.Max, exp.Avg)


class GuardViolation(Exception):
    pass


def _is_aggregate_expr(e: exp.Expression) -> bool:
    if isinstance(e, exp.Alias):
        return _is_aggregate_expr(e.this)
    if isinstance(e, _AGG_FUNCS):
        return True
    if isinstance(e, (exp.Literal, exp.Null, exp.Boolean)):
        return True
    if isinstance(e, (exp.Paren, exp.Cast, exp.TryCast)):
        return _is_aggregate_expr(e.this)
    if isinstance(e, (exp.Coalesce,)):
        return all(_is_aggregate_expr(a) for a in [e.this, *e.expressions])
    if isinstance(e, exp.Binary):
        return _is_aggregate_expr(e.left) and _is_aggregate_expr(e.right)
    if isinstance(e, exp.Neg):
        return _is_aggregate_expr(e.this)
    return False


def check_query(
    sql: str, dialect: str, mode: str = "aggregates_only", safe_dimensions: set[str] | None = None
) -> exp.Query:
    try:
        statements = [s for s in sqlglot.parse(sql, dialect=dialect) if s is not None]
    except ParseError as e:
        raise GuardViolation(f"query does not parse in dialect {dialect}: {e}") from e
    if len(statements) != 1:
        raise GuardViolation(f"exactly one statement allowed, got {len(statements)}")
    q = statements[0]
    if not isinstance(q, exp.Query):
        raise GuardViolation(f"only queries are allowed, got {type(q).__name__}")
    for node in q.walk():
        if isinstance(node, _FORBIDDEN):
            raise GuardViolation(f"forbidden construct {type(node).__name__} in query")
        if isinstance(node, exp.Func):
            fname = (str(node.this) if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
            if any(b in fname for b in _BLOCKED_FUNCS):
                raise GuardViolation(f"function {fname} is not allowed")
        if isinstance(node, exp.Table) and not isinstance(node.this, exp.Identifier):
            raise GuardViolation("table functions are not allowed; only named tables may be read")
    if mode == "aggregates_only" or mode == "allow_dimensions":
        outer = q if isinstance(q, exp.Select) else None
        if outer is None:
            raise GuardViolation("set operations (UNION etc.) are not allowed at the top level")
        group = outer.args.get("group")
        if group:
            if mode != "allow_dimensions":
                raise GuardViolation("GROUP BY on the outer query is not allowed in aggregates_only mode")
            safe = safe_dimensions or set()
            for g in group.expressions:
                name = g.sql().lower().replace('"', "")
                if not any(name == d or d.endswith("." + name) for d in safe):
                    raise GuardViolation(f"GROUP BY {name} is not a configured safe dimension")
        group_names = {g.sql().lower() for g in group.expressions} if group else set()
        for proj in outer.expressions:
            inner = proj.this if isinstance(proj, exp.Alias) else proj
            if inner.sql().lower() in group_names:
                continue
            if not _is_aggregate_expr(proj):
                raise GuardViolation(
                    f"output column `{proj.sql()}` is not an aggregate; only totals and counts may leave the "
                    "database"
                )
    return q


@dataclass
class AuditEntry:
    id: int
    purpose: str
    sql: str
    dialect: str
    result: list[list[Any]]
    ms: float
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "purpose": self.purpose,
            "sql": self.sql,
            "dialect": self.dialect,
            "result": [[_jsonable(v) for v in row] for row in self.result],
            "ms": round(self.ms, 2),
            "sha256": self.sha256,
        }


def _jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (int, str, bool)) or v is None:
        return v
    if isinstance(v, float):
        return repr(v)
    return str(v)


def result_hash(rows: Sequence[Sequence[Any]]) -> str:
    canon = "|".join(",".join(str(_jsonable(v)) for v in r) for r in rows)
    return hashlib.sha256(canon.encode()).hexdigest()


@dataclass
class Guard:
    executor: Callable[[str], list[tuple[Any, ...]]]
    dialect: str
    mode: str = "aggregates_only"
    safe_dimensions: set[str] = field(default_factory=set)
    max_rows: int = 50
    log: list[AuditEntry] = field(default_factory=list)

    def run(self, sql: str, purpose: str) -> list[tuple[Any, ...]]:
        check_query(sql, self.dialect, self.mode, self.safe_dimensions)
        t0 = time.perf_counter()
        rows = self.executor(sql)
        ms = (time.perf_counter() - t0) * 1000
        if len(rows) > self.max_rows:
            raise GuardViolation(f"query returned {len(rows)} rows (limit {self.max_rows})")
        self.log.append(
            AuditEntry(
                len(self.log) + 1, purpose, sql, self.dialect, [list(r) for r in rows], ms, result_hash(rows)
            )
        )
        return rows

    def scalar(self, sql: str, purpose: str) -> Any:
        rows = self.run(sql, purpose)
        if len(rows) != 1 or len(rows[0]) < 1:
            raise GuardViolation(f"expected one row, got {len(rows)}")
        return rows[0][0]

    def row(self, sql: str, purpose: str) -> tuple[Any, ...]:
        rows = self.run(sql, purpose)
        if len(rows) != 1:
            raise GuardViolation(f"expected one row, got {len(rows)}")
        return rows[0]
