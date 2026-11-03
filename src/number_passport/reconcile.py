"""Per-hop reconciliation.

For one model (the code that builds table ``T``) and one traced measure ``SUM(T.col)`` we answer:

1. **Does the table match its code?** ``SUM(col)`` on the materialised table versus ``SUM(col)`` over the
   model's query re-run right now. A difference means a stale build, a manual edit, or non-determinism.
2. **Where did the value go?** Every row-removing step in the code (each ``WHERE`` conjunct, each inner
   join, each ``QUALIFY`` / ``DISTINCT`` / ``HAVING``) is switched off, then switched back on one at a
   time in pipeline order. The change in the measure at each switch is that step's contribution, so the
   contributions add up exactly to ``relaxed - recomputed``: no unexplained remainder by construction.
3. **How many rows did each step remove**, and for inner joins whose lost rows cannot be expressed in
   target units (no FX rate, no risk weight) the lost value in the driver table's own units.
4. **Join fan-out**: for every join to a physical table, whether the join keys are unique on that side.

Only aggregate queries are issued, through :class:`~number_passport.guard.Guard`.
"""

from __future__ import annotations

import difflib
import itertools
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any

from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage

from .connectors import ConnectorError
from .guard import Guard, GuardViolation
from .model import Finding, Hop, Model, Severity, StepKind, StepResult
from .parsers import arg, set_arg, table_name

_MARK = "np_step"


def to_decimal(v: Any) -> Decimal:
    if v is None:
        return Decimal(0)
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except InvalidOperation as e:
        raise ValueError(f"not a number: {v!r}") from e


def fmt(d: Decimal | None) -> str | None:
    """Money-style rendering. Comparisons always use the unrounded value."""
    if d is None:
        return None
    return format(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN), "f")


def money(d: Decimal | None) -> str:
    """Human rendering for messages: 1,085,669,797.07"""
    return "n/a" if d is None else f"{d.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN):,}"


@dataclass
class _Step:
    id: int
    kind: StepKind
    select_id: int
    description: str
    line: int | None


@dataclass
class Tolerance:
    absolute: Decimal = Decimal("0.01")
    relative: Decimal = Decimal(0)

    def ok(self, a: Decimal, b: Decimal) -> bool:
        return abs(a - b) <= self.absolute + self.relative * abs(b)


@dataclass
class NextHop:
    table: str
    column: str
    role: str = "primary"  # primary: carries the measure (read via FROM); factor: joined in (rates, weights)


@dataclass
class HopAnalysis:
    hop: Hop
    next_hops: list[NextHop] = field(default_factory=list)


def _line_of(node: exp.Expression) -> int | None:
    lines = [n.meta["line"] for n in node.walk() if isinstance(n, exp.Expression) and n.meta.get("line")]
    return min(lines) if lines else None


class HopAnalyzer:
    def __init__(self, model: Model, guard: Guard, schema: dict[str, list[str]], tol: Tolerance) -> None:
        self.model = model
        self.guard = guard
        self.schema = schema
        self.tol = tol
        self.dialect = guard.dialect
        self.query: exp.Query = model.query.copy()
        self.steps: list[_Step] = []
        self.selects: dict[int, exp.Select] = {}
        self._index()

    # ------------------------------------------------------------------ discovery
    def _where(self, line: int | None) -> str:
        fl = self.model.file_line(line)
        return f"{self.model.source.file}:{fl}" if fl else self.model.source.file

    def _index(self) -> None:
        counter = itertools.count(1)
        # post-order: inner selects (CTE bodies, subqueries) come before the selects that read them
        selects = [s for s in self.query.find_all(exp.Select)]
        selects.sort(key=lambda s: -s.depth)
        cte_order = {id(c.this): i for i, c in enumerate(self.query.find_all(exp.CTE))}
        selects.sort(key=lambda s: (cte_order.get(id(s), 10_000), -s.depth))
        for sid, sel in enumerate(selects, 1):
            sel.meta[_MARK + "_select"] = sid
            self.selects[sid] = sel
            for j in sel.args.get("joins") or []:
                kind = (j.args.get("kind") or "").upper()
                side = (j.args.get("side") or "").upper()
                if (
                    side
                    or kind in {"CROSS", "OUTER", "SEMI", "ANTI"}
                    or (not j.args.get("on") and not j.args.get("using"))
                ):
                    continue
                n = next(counter)
                j.meta[_MARK] = n
                self.steps.append(
                    _Step(n, StepKind.INNER_JOIN, sid, f"inner join {j.this.sql(self.dialect)}", _line_of(j))
                )
            where = sel.args.get("where")
            if where is not None:
                for cond in where.this.flatten() if isinstance(where.this, exp.And) else [where.this]:
                    n = next(counter)
                    cond.meta[_MARK] = n
                    self.steps.append(
                        _Step(n, StepKind.FILTER, sid, f"filter {cond.sql(self.dialect)}", _line_of(cond))
                    )
            if sel.args.get("qualify") is not None:
                n = next(counter)
                sel.args["qualify"].meta[_MARK] = n
                self.steps.append(
                    _Step(
                        n,
                        StepKind.DEDUP,
                        sid,
                        f"dedup {sel.args['qualify'].this.sql(self.dialect)}",
                        _line_of(sel.args["qualify"]),
                    )
                )
            if sel.args.get("distinct") is not None:
                n = next(counter)
                sel.args["distinct"].meta[_MARK] = n
                self.steps.append(_Step(n, StepKind.DISTINCT, sid, "SELECT DISTINCT", _line_of(sel)))
            having = sel.args.get("having")
            if having is not None:
                for cond in having.this.flatten() if isinstance(having.this, exp.And) else [having.this]:
                    n = next(counter)
                    cond.meta[_MARK] = n
                    self.steps.append(
                        _Step(n, StepKind.HAVING, sid, f"having {cond.sql(self.dialect)}", _line_of(cond))
                    )

    # ------------------------------------------------------------------ rewriting
    def _variant(self, disabled: set[int]) -> exp.Query:
        q = self.query.copy()
        for node in list(q.walk()):
            if not isinstance(node, exp.Expression):
                continue
            sid = node.meta.get(_MARK)
            if sid is None or sid not in disabled:
                continue
            if isinstance(node, exp.Join):
                node.set("side", "LEFT")
                node.set("kind", None)
            elif isinstance(node, exp.Qualify):
                node.parent.set("qualify", None)
            elif isinstance(node, exp.Distinct):
                node.parent.set("distinct", None)
            else:
                node.replace(exp.true())
        return q

    def _measure_sql(self, q: exp.Query, column: str, agg: str, outer_where: str | None) -> str:
        sub = q.subquery("np_q")
        col = exp.column(column, table="np_q")
        fn: exp.Expression = exp.Count(this=exp.Star()) if agg == "count" else exp.Sum(this=col)
        sel = exp.select(fn.as_("v")).from_(sub)
        if outer_where:
            sel = sel.where(outer_where, dialect=self.model.dialect)
        return sel.sql(dialect=self.dialect)

    def _select_in(self, q: exp.Query, select_id: int) -> exp.Select:
        for s in q.find_all(exp.Select):
            if s.meta.get(_MARK + "_select") == select_id:
                return s
        raise KeyError(select_id)

    def _row_count_sql(self, disabled: set[int], step: _Step) -> str:
        q = self._variant(disabled)
        s = self._select_in(q, step.select_id).copy()
        with_ = arg(q, "with")
        if step.kind in {StepKind.DISTINCT, StepKind.HAVING}:
            body: exp.Query = s
        else:
            s.set("expressions", [exp.Literal.number(1).as_("np_one")])
            for key in ("group", "having", "order", "limit", "offset", "distinct"):
                s.set(key, None)
            body = s
        outer = exp.select(exp.Count(this=exp.Star()).as_("n")).from_(body.subquery("np_rows"))
        if with_ is not None and arg(s, "with") is None:
            set_arg(outer, "with", with_.copy())
        return outer.sql(dialect=self.dialect)

    # ------------------------------------------------------------------ lineage
    def upstream_columns(self, column: str) -> tuple[list[NextHop], list[str]]:
        """Upstream columns feeding ``column``: primary ones (read via FROM) first, then factors (via JOIN).

        Returns the next hops (both roles) and the factor references as ``table.column`` strings.
        """
        sch: dict[str, Any] = {}
        for t, cols in self.schema.items():
            parts = t.split(".")
            node = sch
            for p in parts[:-1]:
                node = node.setdefault(p, {})
            node[parts[-1]] = dict.fromkeys(cols, "UNKNOWN")
        root = sqlglot_lineage(
            column, self.model.query.copy(), schema=sch or None, dialect=self.model.dialect
        )
        primary: list[NextHop] = []
        factors: list[str] = []
        for node in root.walk():
            if not isinstance(node.source, exp.Table) or node.downstream:
                continue
            tname = table_name(node.source)
            cname = node.name.split(".")[-1].lower()
            if isinstance(node.source.parent, exp.From):
                if not any(h.table == tname and h.column == cname for h in primary):
                    primary.append(NextHop(tname, cname))
            else:
                ref = f"{tname}.{cname}"
                if ref not in factors:
                    factors.append(ref)
        # Several columns of the same FROM table can feed one measure (value * (1 - haircut)). Only one
        # carries the value: pick the name closest to the traced column; the rest are factors.
        chosen: list[NextHop] = []
        for tname in dict.fromkeys(h.table for h in primary):
            cands = [h for h in primary if h.table == tname]
            best = max(cands, key=lambda h: difflib.SequenceMatcher(None, h.column, column.lower()).ratio())
            chosen.append(best)
            for h in cands:
                if h is not best and f"{h.table}.{h.column}" not in factors:
                    factors.append(f"{h.table}.{h.column}")
        hops = chosen + [NextHop(r.rsplit(".", 1)[0], r.rsplit(".", 1)[1], "factor") for r in factors]
        return hops, factors

    # ------------------------------------------------------------------ fan-out
    def fanout_findings(self) -> list[Finding]:
        out: list[Finding] = []
        for sel in self.query.find_all(exp.Select):
            for j in sel.args.get("joins") or []:
                if not isinstance(j.this, exp.Table):
                    continue
                right = table_name(j.this)
                alias = j.this.alias_or_name.lower()
                keys: list[str] = []
                on = j.args.get("on")
                if on is not None:
                    for eq in on.find_all(exp.EQ):
                        for side in (eq.left, eq.right):
                            if isinstance(side, exp.Column) and side.table.lower() == alias:
                                keys.append(side.name.lower())
                for u in j.args.get("using") or []:
                    keys.append(u.name.lower())
                keys = sorted(set(keys))
                if not keys:
                    continue
                cols = ", ".join(keys)
                sql = (
                    f"SELECT COUNT(*) AS n FROM (SELECT {cols} FROM {right} GROUP BY {cols} "
                    f"HAVING COUNT(*) > 1) AS np_dupes"
                )
                try:
                    q = exp.maybe_parse(sql, dialect="duckdb").sql(dialect=self.dialect)
                    n = int(to_decimal(self.guard.scalar(q, f"join key uniqueness {right}({cols})")))
                except (ConnectorError, GuardViolation) as e:
                    out.append(
                        Finding(
                            Severity.WARNING,
                            "fanout_unchecked",
                            f"could not check join keys on {right}: {e}",
                            self._where(_line_of(j)),
                        )
                    )
                    continue
                if n:
                    out.append(
                        Finding(
                            Severity.CRITICAL,
                            "join_fanout",
                            f"join to {right} on ({cols}) is not unique: {n} key value(s) appear "
                            "more than once, so matching rows are multiplied and the measure "
                            "can be double counted",
                            self._where(_line_of(j)),
                        )
                    )
        return out

    # ------------------------------------------------------------------ code-only
    def describe(self, column: str, agg: str = "sum", index: int = 1) -> HopAnalysis:
        """Lineage and the list of row-removing steps, without touching any database."""
        m = self.model
        hop = Hop(
            index=index,
            target=m.target,
            measure=f"{agg.upper()}({column})",
            target_value=None,
            produced_by=str(m.source),
            model_kind=m.kind,
            source_tables=list(m.upstream),
            orchestration=list(m.orchestration),
            status="code_only",
        )
        for note in m.notes:
            hop.findings.append(Finding(Severity.WARNING, "translation_note", note, m.source.file))
        for o in m.opaque:
            hop.findings.append(Finding(Severity.WARNING, "opaque_code", o, m.source.file))
        try:
            primary, factors = self.upstream_columns(column)
        except Exception as e:
            primary, factors = [], []
            hop.findings.append(
                Finding(
                    Severity.WARNING,
                    "lineage_incomplete",
                    f"column-level lineage for {column} could not be resolved: {e}",
                    str(m.source),
                )
            )
        hop.factors = factors
        hop.steps = [
            StepResult(
                kind=s.kind,
                description=s.description,
                where=self._where(s.line),
                value_delta=None,
                rows_removed=None,
            )
            for s in self.steps
        ]
        return HopAnalysis(hop, primary)

    # ------------------------------------------------------------------ main
    def analyze(
        self, column: str, agg: str = "sum", outer_where: str | None = None, index: int = 1
    ) -> HopAnalysis:
        m = self.model
        hop = Hop(
            index=index,
            target=m.target,
            measure=f"{agg.upper()}({column})",
            target_value=None,
            produced_by=str(m.source),
            model_kind=m.kind,
            source_tables=list(m.upstream),
            orchestration=list(m.orchestration),
        )
        for note in m.notes:
            hop.findings.append(Finding(Severity.WARNING, "translation_note", note, m.source.file))
        for o in m.opaque:
            hop.findings.append(Finding(Severity.WARNING, "opaque_code", o, m.source.file))

        try:
            primary, factors = self.upstream_columns(column)
        except Exception as e:  # sqlglot raises several types for unresolvable lineage
            primary, factors = [], []
            hop.findings.append(
                Finding(
                    Severity.WARNING,
                    "lineage_incomplete",
                    f"column-level lineage for {column} could not be resolved: {e}",
                    str(m.source),
                )
            )
        hop.factors = factors

        target_sql = exp.select(
            (exp.Count(this=exp.Star()) if agg == "count" else exp.Sum(this=exp.column(column))).as_("v")
        ).from_(m.target)
        if outer_where:
            target_sql = target_sql.where(outer_where, dialect=m.dialect)
        try:
            actual = to_decimal(
                self.guard.scalar(
                    target_sql.sql(dialect=self.dialect), f"hop {index}: {agg}({column}) on {m.target}"
                )
            )
            hop.target_value = fmt(actual)
        except (ConnectorError, GuardViolation) as e:
            hop.status = "error"
            hop.findings.append(Finding(Severity.CRITICAL, "target_unreadable", str(e), m.target))
            return HopAnalysis(hop, primary)

        if m.opaque:
            hop.status = "not_reproducible"
            hop.findings.append(
                Finding(
                    Severity.WARNING,
                    "not_reproducible",
                    "parts of this code could not be translated faithfully, so its numbers are not "
                    "re-derived; lineage above is still valid",
                    str(m.source),
                )
            )
            return HopAnalysis(hop, primary)

        def value(disabled: set[int], purpose: str) -> Decimal:
            return to_decimal(
                self.guard.scalar(
                    self._measure_sql(self._variant(disabled), column, agg, outer_where), purpose
                )
            )

        all_ids = {s.id for s in self.steps}
        try:
            recomputed = value(set(), f"hop {index}: re-run {m.source} and total {column}")
            relaxed = value(all_ids, f"hop {index}: re-run with every row-removing step switched off")
        except (ConnectorError, GuardViolation) as e:
            hop.status = "not_reproducible"
            hop.findings.append(
                Finding(Severity.CRITICAL, "rerun_failed", f"re-running the model failed: {e}", str(m.source))
            )
            return HopAnalysis(hop, primary)
        hop.recomputed_value = fmt(recomputed)
        hop.relaxed_value = fmt(relaxed)

        disabled = set(all_ids)
        prev = relaxed
        for st in self.steps:
            before_disabled = set(disabled)
            disabled.discard(st.id)
            purpose = f"hop {index}: switch on {st.description[:60]}"
            try:
                cur = value(disabled, purpose)
            except (ConnectorError, GuardViolation) as e:
                hop.findings.append(
                    Finding(Severity.WARNING, "step_failed", f"{st.description}: {e}", self._where(st.line))
                )
                continue
            rows: int | None = None
            try:
                before = int(
                    to_decimal(
                        self.guard.scalar(
                            self._row_count_sql(before_disabled, st), f"{purpose} (rows before)"
                        )
                    )
                )
                after = int(
                    to_decimal(
                        self.guard.scalar(self._row_count_sql(disabled, st), f"{purpose} (rows after)")
                    )
                )
                rows = before - after
            except (ConnectorError, GuardViolation, KeyError):
                rows = None
            res = StepResult(
                kind=st.kind,
                description=st.description,
                where=self._where(st.line),
                value_delta=fmt(cur - prev),
                rows_removed=rows,
            )
            if st.kind == StepKind.INNER_JOIN and rows and abs(cur - prev) <= self.tol.absolute:
                res.note = (
                    f"{rows} row(s) had no match and were dropped; their value could not be expressed "
                    f"in {column} units, so it is shown in the driving table's units where possible"
                )
                self._native_join_loss(st, res, before_disabled, primary)
            hop.steps.append(res)
            prev = cur

        if not self.tol.ok(prev, recomputed):  # internal consistency: all steps on == recomputed
            hop.findings.append(
                Finding(
                    Severity.WARNING,
                    "attribution_drift",
                    f"step attribution ended at {money(prev)} but the model gives "
                    f"{money(recomputed)}; steps interact non-additively",
                    str(m.source),
                )
            )

        residual = actual - recomputed
        hop.residual = fmt(residual)
        if self.tol.ok(actual, recomputed):
            hop.status = "reconciled"
        else:
            hop.status = "gap"
            hop.findings.append(
                Finding(
                    Severity.CRITICAL,
                    "table_code_mismatch",
                    f"{m.target} holds {money(actual)} but its code produces {money(recomputed)} today "
                    f"(difference {money(residual)}): stale build, manual change, or late-arriving "
                    "source data",
                    str(m.source),
                )
            )
        hop.findings.extend(self.fanout_findings())
        return HopAnalysis(hop, primary)

    def _native_join_loss(
        self, st: _Step, res: StepResult, disabled: set[int], primary: list[NextHop]
    ) -> None:
        """Value of rows dropped by an inner join, in the driving table's units."""
        q = self._variant(disabled)
        sel = self._select_in(q, st.select_id)
        join = next((j for j in sel.args.get("joins") or [] if j.meta.get(_MARK) == st.id), None)
        frm = arg(sel, "from")
        if join is None or frm is None:
            return
        right_alias = join.this.alias_or_name.lower()
        driver_alias = frm.this.alias_or_name.lower()
        right_key = None
        on = join.args.get("on")
        if on is not None:
            for c in on.find_all(exp.Column):
                if c.table.lower() == right_alias:
                    right_key = c
                    break
        if right_key is None:
            return
        wanted = {h.column for h in primary if h.role == "primary"}
        measure = None
        for c in sel.find_all(exp.Column):
            if c.table.lower() == driver_alias and c.name.lower() in wanted:
                measure = c
                break
        if measure is None:
            return
        s = sel.copy()
        s.set("expressions", [exp.Sum(this=measure.copy()).as_("v")])
        for key in ("group", "having", "order", "limit", "qualify", "distinct"):
            s.set(key, None)
        s = s.where(exp.Is(this=right_key.copy(), expression=exp.Null()))
        with_ = arg(q, "with")
        if with_ is not None and arg(s, "with") is None:
            set_arg(s, "with", with_.copy())
        try:
            v = to_decimal(
                self.guard.scalar(
                    s.sql(dialect=self.dialect), f"value of rows dropped by {st.description[:60]}"
                )
            )
        except (ConnectorError, GuardViolation):
            return
        res.native_value_removed = fmt(v)
        res.native_measure = f"SUM({measure.sql(self.dialect)})"
