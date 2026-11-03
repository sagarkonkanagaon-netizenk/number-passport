"""Informatica PowerCenter mappings (repository XML exports), translated into SQL.

Export a folder or a workflow from the Repository Manager (``File > Export Objects``) and point
``code.informatica`` at the ``.xml`` files. Each target instance in each mapping becomes a model whose SQL
has one CTE per transformation, so a row-removing Filter is reported at its line in the XML.

Translated: Source Qualifier (SQL override, source filter, user-defined join, select distinct), Filter,
Expression (including variable ports), Aggregator, Joiner (normal, master/detail/full outer), connected
Lookup (as a LEFT JOIN), Router groups, Union, Sorter (distinct), Update Strategy
(``DD_REJECT`` as a filter) and Transaction Control. Expressions are converted from the Informatica
language (``IIF``, ``DECODE``, ``ISNULL``, ``SYSDATE``, ``||``).

Anything else (Java, Normalizer, Stored Procedure, unconnected lookups, ``$$`` parameters, session-level
SQL or table overrides) keeps lineage and marks the hop not reproducible.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from ..model import Model, SourceRef
from . import referenced_tables
from .sas_parser import _args
from .sql_parser import ParseIssue, _rel


class _Opaque(Exception):
    pass


def _q(name: str) -> str:
    n = re.sub(r"\W+", "_", name.lower()).strip("_") or "x"
    return n if re.match(r"[a-z_]", n) else f"_{n}"


# ---------------------------------------------------------------------------------------------- expressions
def _replace_calls(text: str, name: str, build: object) -> str:
    rx = re.compile(rf"(?<![\w.]){name}\s*\(", re.I)
    while True:
        hits = list(rx.finditer(text))
        if not hits:
            return text
        m = hits[-1]  # innermost-last first, so nested calls are already rewritten
        args, close = _args(text, m.end() - 1)
        if close < 0:
            return text
        text = text[: m.start()] + build([a.strip() for a in args]) + text[close + 1 :]  # type: ignore[operator]


def _iif(a: list[str]) -> str:
    if len(a) < 2:
        raise _Opaque("IIF needs two or three arguments")
    other = a[2] if len(a) > 2 else "NULL"
    return f"(CASE WHEN {a[0]} THEN {a[1]} ELSE {other} END)"


def _decode(a: list[str]) -> str:
    if len(a) < 3:
        raise _Opaque("DECODE needs at least three arguments")
    subject, rest = a[0], a[1:]
    default = rest.pop() if len(rest) % 2 == 1 else "NULL"
    pairs = list(zip(rest[::2], rest[1::2], strict=True))
    if subject.upper() == "TRUE":
        whens = " ".join(f"WHEN {c} THEN {r}" for c, r in pairs)
    else:
        whens = " ".join(f"WHEN {subject} = {v} THEN {r}" for v, r in pairs)
    return f"(CASE {whens} ELSE {default} END)"


def infa_expr(text: str) -> str:
    """Informatica transformation language to SQL."""
    t = re.sub(r"(--|//)[^\n]*", " ", text)
    if "$$" in t or re.search(r":LKP\.", t, re.I) or re.search(r":SP\.", t, re.I):
        raise _Opaque(f"expression uses a mapping parameter or an unconnected lookup: {t.strip()[:60]}")
    t = re.sub(r"\^=|!=", "<>", t)
    t = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", t, flags=re.I)
    t = re.sub(r"\bSESSSTARTTIME\b", "CURRENT_TIMESTAMP", t, flags=re.I)
    t = _replace_calls(t, "ISNULL", lambda a: f"({a[0]} IS NULL)")
    t = _replace_calls(t, "IS_SPACES", lambda a: f"(TRIM({a[0]}) = '')")
    t = _replace_calls(t, "IIF", _iif)
    t = _replace_calls(t, "DECODE", _decode)
    t = re.sub(r"\bDD_INSERT\b", "0", t, flags=re.I)
    t = re.sub(r"\bDD_UPDATE\b", "1", t, flags=re.I)
    t = re.sub(r"\bDD_DELETE\b", "2", t, flags=re.I)
    t = re.sub(r"\bDD_REJECT\b", "3", t, flags=re.I)
    return " ".join(t.split())


# ---------------------------------------------------------------------------------------------- model
@dataclass
class _Field:
    name: str
    porttype: str
    expression: str | None
    group: str | None = None
    ref_field: str | None = None
    groupby: bool = False


@dataclass
class _Tx:
    name: str
    type: str
    fields: list[_Field]
    attrs: dict[str, str]
    groups: list[tuple[str, str, str]]  # (name, type, expression)
    line: int


@dataclass
class _Rel:
    """A transformation's output: a CTE name and its columns."""

    cte: str
    cols: list[str]
    # (instance, port) -> column of this relation holding that port's value. Passive transformations
    # (Expression, Lookup) carry their input's ports forward, as Informatica does row by row.
    carry: dict[tuple[str, str], str] = field(default_factory=dict)


@dataclass
class _Mapping:
    name: str
    line: int
    txs: dict[str, _Tx] = field(default_factory=dict)  # instance name -> transformation
    kinds: dict[str, str] = field(default_factory=dict)  # instance name -> SOURCE | TARGET | TRANSFORMATION
    sources: dict[str, str] = field(default_factory=dict)  # source instance -> table
    targets: dict[str, str] = field(default_factory=dict)  # target instance -> table
    assoc: dict[str, list[str]] = field(default_factory=dict)  # source qualifier -> source instances
    edges: list[tuple[str, str, str, str]] = field(
        default_factory=list
    )  # from_inst, from_field, to_inst, to_field


def _line_of(raw: str, pattern: str, start: int = 0) -> int:
    m = re.search(pattern, raw[start:])
    return raw.count("\n", 0, start + m.start()) + 1 if m else 1


def _fields(el: ET.Element, tag: str) -> list[_Field]:
    out = []
    for f in el.findall(tag):
        out.append(
            _Field(
                name=f.get("NAME", "").lower(),
                porttype=(f.get("PORTTYPE") or "").upper(),
                expression=f.get("EXPRESSION"),
                group=(f.get("GROUP") or None),
                ref_field=(f.get("REF_FIELD") or "").lower() or None,
                groupby=(f.get("GROUPBY") or "").upper() == "YES",
            )
        )
    return out


def _tx(el: ET.Element, line: int) -> _Tx:
    attrs = {a.get("NAME", "").lower(): a.get("VALUE", "") for a in el.findall("TABLEATTRIBUTE")}
    groups = [
        (g.get("NAME", ""), (g.get("TYPE") or "").upper(), g.get("EXPRESSION") or "")
        for g in el.findall("GROUP")
    ]
    return _Tx(el.get("NAME", ""), el.get("TYPE", ""), _fields(el, "TRANSFORMFIELD"), attrs, groups, line)


class _Builder:
    def __init__(self, m: _Mapping, schema_map: dict[str, str]) -> None:
        self.m = m
        self.schema_map = schema_map
        self.ctes: list[tuple[str, str, int]] = []  # name, body, xml line
        self.done: dict[tuple[str, str | None], _Rel] = {}
        self.inherited: dict[str, dict[tuple[str, str], str]] = {}
        self.notes: list[str] = []
        self.opaque: list[str] = []

    def table(self, owner: str | None, name: str) -> str:
        own = (owner or "").lower()
        own = self.schema_map.get(own, own)
        return f"{own}.{name.lower()}" if own else name.lower()

    def inputs(self, inst: str) -> list[tuple[str, str, str]]:
        """(from_instance, from_field, to_field) edges into ``inst``."""
        return [(a, f, tf) for a, f, b, tf in self.m.edges if b == inst]

    def cte(self, name: str, body: str, line: int) -> str:
        base = _q(name)
        n, i = base, 1
        existing = {c[0] for c in self.ctes}
        while n in existing:
            i += 1
            n = f"{base}_{i}"
        self.ctes.append((n, body, line))
        return n

    def rel(self, inst: str, group: str | None = None) -> _Rel:
        key = (inst, group)
        if key not in self.done:
            r = self._build(inst, group)
            if not r.carry:
                r.carry = {(inst, c): c for c in r.cols}
            self.done[key] = r
        return self.done[key]

    def _wire(
        self, inst: str, fields: list[str] | None = None, inherit: bool = False
    ) -> tuple[str, list[str]]:
        """``SELECT <upstream col> AS port, ...`` over the relation that holds every port wired into ``inst``.

        Ports may come from several transformations when the later ones are passive (an Expression fed by
        a Router and by a Lookup on the same rows); the most downstream relation carries them all.
        """
        ins = [(a, f, tf) for a, f, tf in self.inputs(inst) if fields is None or tf.lower() in fields]
        if not ins:
            raise _Opaque(f"{inst} has no connected inputs")
        need = [(a, f.lower()) for a, f, _ in ins]
        chosen: _Rel | None = None
        for up in reversed(list(dict.fromkeys(a for a, _, _ in ins))):
            r = self.rel(up, self._router_group(up, [e for e in ins if e[0] == up]))
            if all(k in r.carry for k in need):
                chosen = r
                break
        if chosen is None:
            ups = ", ".join(dict.fromkeys(a for a, _, _ in ins))
            raise _Opaque(f"{inst} reads ports from transformations that do not share rows ({ups})")
        sel, cols = [], []
        inherited: dict[tuple[str, str], str] = {}
        for (a, f), (_, _, tf) in zip(need, ins, strict=True):
            src = chosen.carry[(a, f)]
            t = tf.lower()
            sel.append(f"{src} AS {t}" if src != t else t)
            cols.append(t)
            inherited[(a, f)] = t
        for key, col in chosen.carry.items() if inherit else []:
            if key in inherited:
                continue
            alias = "_c_" + _q(f"{key[0]}_{key[1]}")
            if alias in cols or alias in inherited.values():
                continue
            sel.append(f"{col} AS {alias}")
            inherited[key] = alias
        self.inherited[inst] = inherited
        return f"SELECT {', '.join(sel)} FROM {chosen.cte}", cols

    def _router_group(self, up: str, ins: list[tuple[str, str, str]]) -> str | None:
        tx = self.m.txs.get(up)
        if tx is None or tx.type.lower() != "router":
            return None
        by_name = {f.name: f for f in tx.fields}
        groups = {by_name[f.lower()].group for _, f, _ in ins if f.lower() in by_name}
        groups.discard(None)
        if len(groups) != 1:
            raise _Opaque(f"{up}: downstream reads ports from several router groups")
        return groups.pop()

    # ---- per transformation type
    def _build(self, inst: str, group: str | None) -> _Rel:
        kind = self.m.kinds.get(inst, "")
        if kind == "SOURCE":
            return _Rel(self.m.sources[inst], [])
        tx = self.m.txs.get(inst)
        if tx is None:
            raise _Opaque(f"transformation {inst} is not in the export (a shortcut to another folder?)")
        t = tx.type.lower()
        out_ports = [f.name for f in tx.fields if "OUTPUT" in f.porttype]
        if t == "source qualifier":
            return self._sq(inst, tx)
        if t == "filter":
            body, cols = self._wire(inst)
            cond = infa_expr(tx.attrs.get("filter condition") or "TRUE")
            return _Rel(self.cte(inst, f"SELECT * FROM ({body}) AS _i WHERE {cond}", tx.line), cols)
        if t == "expression":
            body, cols = self._wire(inst, inherit=True)
            exprs = self._port_exprs(tx)
            sel = [f"{exprs.get(p, p)} AS {p}" if exprs.get(p, p) != p else p for p in out_ports]
            carry = {(inst, p): p for p in out_ports}
            for key, col in self.inherited.get(inst, {}).items():
                if col in out_ports and exprs.get(col, col) == col:
                    carry.setdefault(key, col)  # an input/output port passed through unchanged
                    continue
                alias = col if col.startswith("_c_") else "_c_" + _q(f"{key[0]}_{key[1]}")
                if alias not in out_ports and f" AS {alias}" not in " ".join(sel) and alias not in sel:
                    sel.append(col if alias == col else f"{col} AS {alias}")
                carry.setdefault(key, alias)
            sql = f"SELECT {', '.join(sel)} FROM ({body}) AS _i"
            return _Rel(self.cte(inst, sql, tx.line), out_ports, carry)
        if t == "aggregator":
            body, _ = self._wire(inst)
            exprs = self._port_exprs(tx)
            keys = [f.name for f in tx.fields if f.groupby]
            sel = []
            for p in out_ports:
                e = exprs.get(p, p)
                if p in keys or e != p:
                    sel.append(f"{e} AS {p}" if e != p else p)
                else:
                    self.notes.append(
                        f"{inst}: port {p} is neither grouped nor aggregated; Informatica returns "
                        "the last row's value, the passport uses ANY_VALUE"
                    )
                    sel.append(f"ANY_VALUE({p}) AS {p}")
            grp = f" GROUP BY {', '.join(keys)}" if keys else ""
            return _Rel(
                self.cte(inst, f"SELECT {', '.join(sel)} FROM ({body}) AS _i{grp}", tx.line), out_ports
            )
        if t == "joiner":
            return self._joiner(inst, tx)
        if t in {"lookup procedure", "lookup"}:
            return self._lookup(inst, tx)
        if t == "router":
            return self._router(inst, tx, group)
        if t == "union transformation" or t == "union":
            return self._union(inst, tx)
        if t == "sorter":
            body, cols = self._wire(inst)
            distinct = "DISTINCT " if (tx.attrs.get("distinct") or "").upper() == "YES" else ""
            return _Rel(self.cte(inst, f"SELECT {distinct}* FROM ({body}) AS _i", tx.line), cols)
        if t == "update strategy":
            body, cols = self._wire(inst)
            e = infa_expr(tx.attrs.get("update strategy expression") or "0")
            if e.strip() in {"0"}:
                return _Rel(self.cte(inst, f"SELECT * FROM ({body}) AS _i", tx.line), cols)
            if re.search(r"(?<![\w.])[12](?![\w.])", e):
                self.opaque.append(f"{inst}: Update Strategy updates or deletes rows already in the target")
            return _Rel(self.cte(inst, f"SELECT * FROM ({body}) AS _i WHERE ({e}) <> 3", tx.line), cols)
        if t in {"transaction control", "expression macro"}:
            body, cols = self._wire(inst)
            return _Rel(self.cte(inst, f"SELECT * FROM ({body}) AS _i", tx.line), cols)
        raise _Opaque(f"{inst}: {tx.type} transformation is not translated")

    def _port_exprs(self, tx: _Tx) -> dict[str, str]:
        """Output port -> SQL, with variable ports substituted in."""
        variables = {f.name: f.expression or "NULL" for f in tx.fields if "VARIABLE" in f.porttype}
        for v, e in variables.items():
            if re.search(rf"\b{re.escape(v)}\b", e, re.I):
                raise _Opaque(f"{tx.name}: variable port {v} keeps state across rows")
        out: dict[str, str] = {}
        for f in tx.fields:
            if "OUTPUT" not in f.porttype or not f.expression:
                continue
            e = f.expression
            for _ in range(10):
                new = e
                for v, ve in variables.items():
                    new = re.sub(rf"\b{re.escape(v)}\b", f"({ve})", new, flags=re.I)
                if new == e:
                    break
                e = new
            sql = infa_expr(e)
            if sql.lower() != f.name:
                out[f.name] = sql
        return out

    def _sq(self, inst: str, tx: _Tx) -> _Rel:
        ports = [f.name for f in tx.fields]
        override = (tx.attrs.get("sql query") or "").strip()
        if override:
            if "$$" in override:
                raise _Opaque(f"{inst}: SQL override uses mapping parameters")
            try:
                q = sqlglot.parse_one(override, dialect=self._dialect())
            except (ParseError, TokenError) as e:
                raise _Opaque(f"{inst}: SQL override did not parse ({str(e).splitlines()[0]})") from e
            if not isinstance(q, exp.Select):
                raise _Opaque(f"{inst}: SQL override is not a single SELECT")
            if len(q.expressions) != len(ports):
                self.notes.append(
                    f"{inst}: SQL override selects {len(q.expressions)} columns for {len(ports)} ports"
                )
            q.set(
                "expressions",
                [
                    (e.this if isinstance(e, exp.Alias) else e).as_(p)
                    for e, p in zip(q.expressions, ports, strict=False)
                ],
            )
            return _Rel(self.cte(inst, q.sql(dialect="duckdb"), tx.line), ports[: len(q.expressions)])
        srcs = self.m.assoc.get(inst) or list(dict.fromkeys(a for a, _, _ in self.inputs(inst)))
        if not srcs:
            raise _Opaque(f"{inst}: no source is connected")
        ins = self.inputs(inst)
        sel = []
        multi = len(srcs) > 1
        for a, f, tf in ins:
            col = f"{_q(a)}.{f.lower()}" if multi else f.lower()
            sel.append(f"{col} AS {tf.lower()}" if col != tf.lower() else col)
        tables = ", ".join(f"{self.m.sources[s]} AS {_q(s)}" if multi else self.m.sources[s] for s in srcs)
        sql = f"SELECT {'DISTINCT ' if (tx.attrs.get('select distinct') or '').upper() == 'YES' else ''}"
        sql += f"{', '.join(sel) or '*'} FROM {tables}"
        conds = []
        if multi:
            join = (tx.attrs.get("user defined join") or "").strip()
            if not join:
                raise _Opaque(f"{inst}: several sources without a user-defined join (default key join)")
            conds.append(f"({infa_expr(self._qualify(join, srcs))})")
        filt = (tx.attrs.get("source filter") or "").strip()
        if filt:
            conds.append(infa_expr(self._qualify(filt, srcs)) if multi else infa_expr(filt))
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        return _Rel(self.cte(inst, sql, tx.line), [tf.lower() for _, _, tf in ins])

    def _qualify(self, text: str, srcs: list[str]) -> str:
        for s in srcs:
            tname = self.m.sources[s].split(".")[-1]
            text = re.sub(rf"\b{re.escape(tname)}\.", f"{_q(s)}.", text, flags=re.I)
        return text

    def _dialect(self) -> str:
        return "oracle"

    def _joiner(self, inst: str, tx: _Tx) -> _Rel:
        master = {f.name for f in tx.fields if "MASTER" in f.porttype}
        ins = self.inputs(inst)
        sides: dict[str, list[tuple[str, str, str]]] = {"m": [], "d": []}
        for a, f, tf in ins:
            sides["m" if tf.lower() in master else "d"].append((a, f, tf))
        rels = {}
        for side, edges in sides.items():
            ups = list(dict.fromkeys(a for a, _, _ in edges))
            if len(ups) != 1:
                raise _Opaque(
                    f"{inst}: the {'master' if side == 'm' else 'detail'} side has {len(ups)} inputs"
                )
            group = self._router_group(ups[0], edges)
            r = self.rel(ups[0], group)
            sel = []
            for _, f, tf in edges:
                src = r.carry.get((ups[0], f.lower()), f.lower())
                sel.append(f"{src} AS {tf.lower()}" if src != tf.lower() else tf.lower())
            rels[side] = f"(SELECT {', '.join(sel)} FROM {r.cte})"
        jt = (tx.attrs.get("join type") or "Normal Join").lower()
        cond = infa_expr(tx.attrs.get("join condition") or "")
        if not cond:
            raise _Opaque(f"{inst}: joiner without a join condition")
        if jt.startswith("normal"):
            sql = f"SELECT * FROM {rels['d']} AS d INNER JOIN {rels['m']} AS m ON {cond}"
        elif jt.startswith("master outer"):
            sql = f"SELECT * FROM {rels['d']} AS d LEFT JOIN {rels['m']} AS m ON {cond}"
        elif jt.startswith("detail outer"):
            sql = f"SELECT * FROM {rels['m']} AS m LEFT JOIN {rels['d']} AS d ON {cond}"
        else:
            sql = f"SELECT * FROM {rels['d']} AS d FULL OUTER JOIN {rels['m']} AS m ON {cond}"
        cols = [f.name for f in tx.fields if "OUTPUT" in f.porttype]
        return _Rel(self.cte(inst, f"SELECT {', '.join(cols)} FROM ({sql}) AS _j", tx.line), cols)

    def _lookup(self, inst: str, tx: _Tx) -> _Rel:
        table = (tx.attrs.get("lookup table name") or "").strip()
        override = (tx.attrs.get("lookup sql override") or "").strip()
        cond = (tx.attrs.get("lookup condition") or "").strip()
        if not cond or not (table or override):
            raise _Opaque(f"{inst}: lookup without a table or a condition")
        body, in_cols = self._wire(inst, inherit=True)
        lkp_cols = [f.name for f in tx.fields if "LOOKUP" in f.porttype and f.name not in in_cols]
        src = f"({override})" if override else self.table(None, table)
        policy = (tx.attrs.get("lookup policy on multiple match") or "").lower()
        self.notes.append(
            f"{inst}: connected lookup on {table or 'an SQL override'} read as a LEFT JOIN"
            + (f" ({policy} on multiple matches)" if policy else "")
        )
        sel = [f"_i.{c}" for c in in_cols] + [f"_l.{c}" for c in lkp_cols if "OUTPUT" in self._ptype(tx, c)]
        c2 = cond
        for c in lkp_cols:
            c2 = re.sub(rf"(?<![\w.]){re.escape(c)}\b", f"_l.{c}", c2, flags=re.I)
        for c in in_cols:
            c2 = re.sub(rf"(?<![\w.]){re.escape(c)}\b", f"_i.{c}", c2, flags=re.I)
        out = in_cols + [c for c in lkp_cols if "OUTPUT" in self._ptype(tx, c)]
        carry = {(inst, p): p for p in out}
        for key, col in self.inherited.get(inst, {}).items():
            if col not in in_cols:
                sel.append(f"_i.{col}")
            carry.setdefault(key, col)
        sql = f"SELECT {', '.join(sel)} FROM ({body}) AS _i LEFT JOIN {src} AS _l ON {infa_expr(c2)}"
        return _Rel(self.cte(inst, sql, tx.line), out, carry)

    @staticmethod
    def _ptype(tx: _Tx, name: str) -> str:
        return next((f.porttype for f in tx.fields if f.name == name), "")

    def _router(self, inst: str, tx: _Tx, group: str | None) -> _Rel:
        input_fields = [f.name for f in tx.fields if f.porttype.startswith("INPUT")]
        body, cols = self._wire(inst, input_fields)
        conds = [(n, e) for n, ty, e in tx.groups if "OUTPUT" in ty and "DEFAULT" not in ty]
        if group is None:
            return _Rel(self.cte(inst, body, tx.line), cols)
        g = next((x for x in tx.groups if x[0] == group), None)
        if g is None:
            raise _Opaque(f"{inst}: router group {group} not found")
        if "DEFAULT" in g[1]:
            any_true = " OR ".join(f"COALESCE({infa_expr(e)}, FALSE)" for _, e in conds) or "FALSE"
            where = f"NOT ({any_true})"
        else:
            where = infa_expr(g[2])
        carry = {(inst, f.name): f.ref_field for f in tx.fields if f.group == group and f.ref_field}
        sql = f"SELECT * FROM ({body}) AS _i WHERE {where}"
        return _Rel(self.cte(f"{inst}_{group}", sql, tx.line), cols, carry)

    def _union(self, inst: str, tx: _Tx) -> _Rel:
        outs = [f.name for f in tx.fields if f.porttype == "OUTPUT"]
        groups = list(dict.fromkeys(f.group for f in tx.fields if f.porttype == "INPUT" and f.group))
        parts = []
        for g in groups:
            gf = [f.name for f in tx.fields if f.porttype == "INPUT" and f.group == g]
            edges = [(a, f, tf) for a, f, tf in self.inputs(inst) if tf.lower() in gf]
            ups = list(dict.fromkeys(a for a, _, _ in edges))
            if len(ups) != 1:
                raise _Opaque(f"{inst}: union group {g} has {len(ups)} inputs")
            grp = self._router_group(ups[0], edges)
            r = self.rel(ups[0], grp)
            feed = {tf.lower(): r.carry.get((ups[0], f.lower()), f.lower()) for _, f, tf in edges}
            sel = [
                f"{feed[c]} AS {o}" if c in feed else f"NULL AS {o}" for c, o in zip(gf, outs, strict=False)
            ]
            parts.append(f"SELECT {', '.join(sel)} FROM {r.cte}")
        if not parts:
            raise _Opaque(f"{inst}: union without input groups")
        return _Rel(self.cte(inst, " UNION ALL ".join(parts), tx.line), outs)

    # ---- targets
    def target(self, inst: str) -> tuple[str, dict[int, int]]:
        body, _ = self._wire(inst)
        lines, lmap = [], {}
        for i, (name, b, xml_line) in enumerate(self.ctes):
            lines.append(("WITH " if i == 0 else ", ") + f"{name} AS ({' '.join(b.split())})")
            lmap[len(lines)] = xml_line
        lines.append(body)
        lmap[len(lines)] = self.m.txs[inst].line if inst in self.m.txs else self.m.line
        return "\n".join(lines), lmap


def _parse_xml(raw: str) -> ET.Element:
    """Parse an export. Entity declarations are refused (PowerCenter exports never contain them), so
    entity-expansion attacks cannot run; the external DTD reference is dropped and never fetched."""
    if re.search(r"<!ENTITY", raw, re.I):
        raise ET.ParseError("entity declarations are not allowed in an Informatica export")
    text = re.sub(r"<!DOCTYPE[^>\[]*(\[[^\]]*\])?\s*>", "", raw, count=1)
    return ET.fromstring(text)  # noqa: S314 - no DTD and no entities, see above


def models_from_informatica(
    raw: str, file: str, schema_map: dict[str, str] | None = None
) -> tuple[list[Model], list[ParseIssue]]:
    issues: list[ParseIssue] = []
    try:
        root = _parse_xml(raw)
    except ET.ParseError as e:
        return [], [ParseIssue(file, f"XML parse error: {e}")]
    smap = {k.lower(): v.lower() for k, v in (schema_map or {}).items()}
    models: list[Model] = []
    for folder in root.iter("FOLDER"):
        sources = {s.get("NAME", ""): s for s in folder.findall("SOURCE")}
        targets = {t.get("NAME", ""): t for t in folder.findall("TARGET")}
        folder_txs = {t.get("NAME", ""): t for t in folder.findall("TRANSFORMATION")}
        for mel in folder.findall("MAPPING"):
            mname = mel.get("NAME", "")
            mstart = raw.find(f'NAME="{mname}"')
            m = _Mapping(mname, _line_of(raw, rf'<MAPPING\b[^>]*NAME="{re.escape(mname)}"'))
            local = {t.get("NAME", ""): t for t in mel.findall("TRANSFORMATION")}
            b = _Builder(m, smap)
            for inst in mel.findall("INSTANCE"):
                iname, tname = inst.get("NAME", ""), inst.get("TRANSFORMATION_NAME", "")
                itype = (inst.get("TYPE") or "").upper()
                m.kinds[iname] = itype
                if itype == "SOURCE":
                    s = sources.get(tname)
                    owner = s.get("OWNERNAME") if s is not None else None
                    dbd = s.get("DBDNAME") if s is not None else None
                    own = owner or dbd
                    if own and own.lower() not in smap and dbd and dbd.lower() in smap:
                        own = dbd
                    m.sources[iname] = b.table(own, tname)
                elif itype == "TARGET":
                    t = targets.get(tname)
                    owner = (t.get("OWNERNAME") if t is not None else None) or inst.get("DBDNAME")
                    m.targets[iname] = b.table(owner, tname)
                else:
                    el = local.get(tname)
                    if el is None:
                        el = folder_txs.get(tname)
                    if el is not None:
                        line = _line_of(
                            raw, rf'<TRANSFORMATION\b[^>]*NAME="{re.escape(tname)}"', max(mstart, 0)
                        )
                        if tname not in local:
                            line = _line_of(raw, rf'<TRANSFORMATION\b[^>]*NAME="{re.escape(tname)}"')
                        tx = _tx(el, line)
                        tx.name = iname
                        m.txs[iname] = tx
                    for a in inst.findall("ASSOCIATED_SOURCE_INSTANCE"):
                        m.assoc.setdefault(iname, []).append(a.get("NAME", ""))
            for c in mel.findall("CONNECTOR"):
                m.edges.append(
                    (
                        c.get("FROMINSTANCE", ""),
                        c.get("FROMFIELD", ""),
                        c.get("TOINSTANCE", ""),
                        c.get("TOFIELD", ""),
                    )
                )
            for tinst, table in m.targets.items():
                b = _Builder(m, smap)
                where = SourceRef(
                    file, _line_of(raw, rf'<INSTANCE\b[^>]*NAME="{re.escape(tinst)}"', max(mstart, 0))
                )
                note = f"Informatica mapping {mname}"
                try:
                    sql, lmap = b.target(tinst)
                    q = sqlglot.parse_one(sql, dialect="duckdb")
                    assert isinstance(q, exp.Query)
                    models.append(
                        Model(
                            target=table,
                            query=q,
                            kind="informatica",
                            source=where,
                            dialect="duckdb",
                            upstream=[u for u in referenced_tables(q) if u != table],
                            opaque=b.opaque,
                            notes=[note, *dict.fromkeys(b.notes)],
                            line_map=lmap,
                        )
                    )
                except (_Opaque, ParseError, TokenError) as e:
                    placeholder = sqlglot.parse_one("SELECT NULL AS untranslated", dialect="duckdb")
                    assert isinstance(placeholder, exp.Query)
                    models.append(
                        Model(
                            target=table,
                            query=placeholder,
                            kind="informatica",
                            source=where,
                            dialect="duckdb",
                            upstream=sorted(
                                {
                                    *m.sources.values(),
                                    *(
                                        b.table(None, tx.attrs["lookup table name"])
                                        for tx in m.txs.values()
                                        if tx.attrs.get("lookup table name")
                                    ),
                                }
                            ),
                            opaque=[f"mapping {mname}: {str(e).splitlines()[0]}"],
                            notes=[note],
                            line_map={},
                        )
                    )
    return models, issues


def scan_informatica(
    paths: list[Path], root: Path, schema_map: dict[str, str] | None = None
) -> tuple[list[Model], list[ParseIssue]]:
    models: list[Model] = []
    issues: list[ParseIssue] = []
    for base in paths:
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured Informatica path does not exist"))
            continue
        files = sorted(base.rglob("*.xml")) if base.is_dir() else [base]
        for f in files:
            found, problems = models_from_informatica(
                f.read_text(encoding="utf-8", errors="replace"), _rel(f, root), schema_map
            )
            models.extend(found)
            issues.extend(problems)
    return models, issues
