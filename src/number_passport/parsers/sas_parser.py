"""SAS programs, translated into SQL the same way PySpark jobs are.

The program is never run. Supported:

* ``PROC SQL`` (``CREATE TABLE/VIEW ... AS``, ``INSERT INTO``), with SAS operators (``eq``, ``ne``, ``^=``),
  double-quoted strings, date literals (``'31DEC2025'd``) and ``CALCULATED``;
* ``DATA`` steps: ``SET`` (one or more datasets), ``MERGE ... BY`` with ``IN=`` flags, ``WHERE``,
  subsetting ``IF``, ``IF ... THEN DELETE``, assignments, ``IF/ELSE IF/ELSE`` assignments, ``KEEP``/``DROP``/
  ``RENAME`` statements and the same dataset options;
* ``PROC SORT`` (``OUT=``, ``NODUPKEY``), ``PROC MEANS``/``PROC SUMMARY`` (``CLASS``/``BY``, ``VAR``,
  ``OUTPUT OUT= SUM= N= MEAN= MIN= MAX=``) and ``PROC APPEND``;
* ``%LET`` macro variables (``&name`` and ``&name.``).

WORK datasets (one-level names) are temporary in SAS, so they are inlined into the step that reads them;
only permanent datasets (``lib.name``) become tables in the lineage. ``libnames`` in ``passport.yml`` map a
SAS libref to the warehouse schema it points at. Anything else (``RETAIN``, ``DO`` loops, arrays, ``LAG``,
``FIRST.``/``LAST.``, macros with logic) is reported: lineage is kept, the hop is marked not reproducible.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from ..model import Model, SourceRef
from . import referenced_tables
from .pyspark_parser import Frame, _ident, _inline, _render
from .sql_parser import ParseIssue, _Builder, _output_names, _rel, _Splitter, _Stmt

_MONTHS = {m: i for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}  # noqa: SIM905
_STRING = re.compile(r"\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'")
_DATE_LIT = re.compile(r"'(\d{1,2})([A-Za-z]{3})(\d{2}|\d{4})'d", re.I)
_DATETIME_LIT = re.compile(r"'(\d{1,2})([A-Za-z]{3})(\d{4}):(\d{2}):(\d{2}):(\d{2})'dt", re.I)
_OPS = [
    (re.compile(r"\^=|~=|¬="), "<>"),
    (re.compile(r"\bne\b", re.I), "<>"),
    (re.compile(r"\beq\b", re.I), "="),
    (re.compile(r"\bge\b", re.I), ">="),
    (re.compile(r"\ble\b", re.I), "<="),
    (re.compile(r"\bgt\b", re.I), ">"),
    (re.compile(r"\blt\b", re.I), "<"),
    (re.compile(r"(?<![<>!=])\^(?!=)|~(?!=)"), " NOT "),
    (re.compile(r"\bcalculated\s+", re.I), ""),
    (re.compile(r"\bupcase\s*\(", re.I), "UPPER("),
    (re.compile(r"\blowcase\s*\(", re.I), "LOWER("),
    (re.compile(r"\bstrip\s*\(", re.I), "TRIM("),
]
_MISSING_CMP = re.compile(r"(<>|=)\s*\.(?![\w.])")
_DATASET = re.compile(r"([A-Za-z_&][\w&.]*)\s*(\((?:[^()]|\((?:[^()]|\([^()]*\))*\))*\))?")


class _Untranslatable(Exception):
    pass


@dataclass
class _St:
    text: str  # without the trailing ';'
    line: int  # file line of the first character
    start: int  # offset in the comment-free source
    end: int

    @property
    def word(self) -> str:
        m = re.match(r"\s*(%?[A-Za-z_]\w*)", self.text)
        return m.group(1).lower() if m else ""

    def rest(self) -> str:
        return re.sub(r"^\s*%?[A-Za-z_]\w*", "", self.text, count=1).strip()


# ---------------------------------------------------------------------------------------------- text
def _strip_comments(src: str) -> str:
    """Blank ``/* */`` comments (newlines kept) without touching quoted text."""
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c in "'\"":
            j = i + 1
            while j < n and not (src[j] == c and not (j + 1 < n and src[j + 1] == c)):
                j += 2 if src[j] == c else 1
            i = j + 1
        elif src.startswith("/*", i):
            j = src.find("*/", i + 2)
            j = n if j < 0 else j + 2
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        else:
            i += 1
    return "".join(out)


def _split(src: str) -> list[_St]:
    out: list[_St] = []
    i, n, start = 0, len(src), 0
    while i < n:
        c = src[i]
        if c in "'\"":
            j = i + 1
            while j < n:
                if src[j] == c and j + 1 < n and src[j + 1] == c:
                    j += 2
                    continue
                if src[j] == c:
                    break
                j += 1
            i = j + 1
            continue
        if c == ";":
            raw = src[start:i]
            stripped = raw.strip()
            if stripped and not stripped.startswith("*"):  # `* comment;` statements
                lead = len(raw) - len(raw.lstrip())
                out.append(_St(stripped, src.count("\n", 0, start + lead) + 1, start + lead, i))
            start = i + 1
        i += 1
    return out


def _date(m: re.Match[str]) -> str:
    d, mon, y = int(m.group(1)), _MONTHS.get(m.group(2).upper()), m.group(3)
    if mon is None:
        return m.group(0)
    year = int(y) if len(y) == 4 else (2000 + int(y) if int(y) < 50 else 1900 + int(y))
    return f"DATE '{year:04d}-{mon:02d}-{d:02d}'"


def _datetime(m: re.Match[str]) -> str:
    mon = _MONTHS.get(m.group(2).upper())
    if mon is None:
        return m.group(0)
    return (
        f"TIMESTAMP '{int(m.group(3)):04d}-{mon:02d}-{int(m.group(1)):02d} "
        f"{m.group(4)}:{m.group(5)}:{m.group(6)}'"
    )


def sas_expr(text: str) -> str:
    """SAS expression syntax to SQL (strings and newlines are preserved)."""
    text = _DATETIME_LIT.sub(_datetime, text)
    text = _DATE_LIT.sub(_date, text)
    parts: list[str] = []
    pos = 0
    for m in _STRING.finditer(text):
        parts.append(_code(text[pos : m.start()]))
        s = m.group(0)
        if s.startswith('"'):
            s = "'" + s[1:-1].replace('""', '"').replace("'", "''") + "'"
        parts.append(s)
        pos = m.end()
    parts.append(_code(text[pos:]))
    return _row_functions("".join(parts))


def _code(seg: str) -> str:
    for rx, rep in _OPS:
        seg = rx.sub(rep, seg)
    seg = _MISSING_CMP.sub(lambda m: " IS NULL" if m.group(1) == "=" else " IS NOT NULL", seg)
    seg = re.sub(r"\bmissing\s*\(\s*([\w.]+)\s*\)", r"(\1 IS NULL)", seg, flags=re.I)
    return seg


def _args(text: str, open_at: int) -> tuple[list[str], int]:
    """Top-level comma separated arguments of the call whose '(' is at ``open_at``; and the ')' offset."""
    depth, j, start = 0, open_at, open_at + 1
    args: list[str] = []
    while j < len(text):
        c = text[j]
        if c == "'":
            j = text.find("'", j + 1)
            if j < 0:
                break
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                args.append(text[start:j])
                return args, j
        elif c == "," and depth == 1:
            args.append(text[start:j])
            start = j + 1
        j += 1
    return args, -1


def _row_functions(text: str) -> str:
    """SAS row-wise ``sum(a, b)`` (missing counts as 0) and ``round(x, 0.01)`` (rounding unit)."""
    out = text
    for name in ("sum", "round"):
        pos = 0
        rx = re.compile(rf"\b{name}\s*\(", re.I)
        while m := rx.search(out, pos):
            args, close = _args(out, m.end() - 1)
            if close < 0:
                break
            rep = None
            if name == "sum" and len(args) > 1:
                rep = "(" + " + ".join(f"COALESCE({a.strip()}, 0)" for a in args) + ")"
            elif name == "round" and len(args) == 2 and re.fullmatch(r"\s*(1|0?\.0*1)\s*", args[1]):
                unit = args[1].strip()
                digits = 0 if unit == "1" else len(unit.split(".")[1])
                rep = f"ROUND({args[0].strip()}, {digits})"
            if rep is None:
                pos = m.end()
                continue
            out = out[: m.start()] + rep + out[close + 1 :]
            pos = m.start() + len(rep)
    return out


# ---------------------------------------------------------------------------------------------- program
@dataclass
class _Options:
    keep: list[str] | None = None
    drop: list[str] = field(default_factory=list)
    rename: dict[str, str] = field(default_factory=dict)
    where: str | None = None
    in_flag: str | None = None


def _parse_options(text: str | None) -> _Options:
    o = _Options()
    if not text:
        return o
    body = text.strip()[1:-1]
    for m in re.finditer(
        r"(\w+)\s*=\s*(\((?:[^()]|\([^()]*\))*\)|[^\s()]+(?:\s+(?!\w+\s*=)[^\s()=]+)*)", body
    ):
        key, val = m.group(1).lower(), m.group(2).strip()
        inner = val[1:-1] if val.startswith("(") else val
        if key == "keep":
            o.keep = [v.lower() for v in inner.split()]
        elif key == "drop":
            o.drop = [v.lower() for v in inner.split()]
        elif key == "rename":
            for a, b in re.findall(r"(\w+)\s*=\s*(\w+)", inner):
                o.rename[a.lower()] = b.lower()
        elif key == "where":
            o.where = sas_expr(inner)
        elif key == "in":
            o.in_flag = inner.lower()
    return o


def _datasets(text: str) -> list[tuple[str, _Options]]:
    out = []
    for m in _DATASET.finditer(text):
        name = m.group(1)
        if "=" in text[max(0, m.start() - 1) : m.start()]:
            continue
        out.append((name.lower(), _parse_options(m.group(2))))
    return out


class _Program:
    def __init__(
        self,
        file: str,
        libnames: dict[str, str],
        schema: dict[str, list[str]],
        issues: list[ParseIssue],
    ) -> None:
        self.file = file
        self.libnames = {k.lower(): v.lower() for k, v in libnames.items()}
        self.schema = schema
        self.issues = issues
        self.macros: dict[str, str] = {}
        self.work: dict[str, Frame] = {}
        self.models: dict[str, Model] = {}
        self.sql = _Builder(file, "duckdb", "sas", 0, is_temp=self._is_work)
        self._pending_opaque: dict[str, list[str]] = {}  # WORK dataset -> problems inherited by readers

    # ---- names
    def name(self, raw: str) -> str:
        """Warehouse name of a SAS dataset: ``work.x``/``x`` stay one-level, librefs map to schemas."""
        n = raw.lower().strip()
        if n.startswith("work."):
            return n[5:]
        if "." in n:
            lib, ds = n.split(".", 1)
            return f"{self.libnames.get(lib, lib)}.{ds}"
        return n

    @staticmethod
    def _is_work(key: str) -> bool:
        return "." not in key

    def issue(self, line: int, msg: str) -> None:
        self.issues.append(ParseIssue(f"{self.file}:{line}", msg))

    def expand(self, st: _St) -> _St:
        def rep(m: re.Match[str]) -> str:
            v = self.macros.get(m.group(1).lower())
            return v if v is not None else m.group(0)

        text = re.sub(r"&(\w+)\.?", rep, st.text)
        return _St(text, st.line, st.start, st.end)

    # ---- reading datasets into frames
    def read(self, raw: str, opts: _Options, line: int, opaque: list[str]) -> Frame:
        n = self.name(raw)
        if "&" in n:
            raise _Untranslatable(f"line {line}: dataset name {raw} uses an unresolved macro variable")
        if n in self.work:
            f = Frame(list(self.work[n].layers), self.work[n].cols)
        elif n in self.sql.temps:
            q = self.sql.temps[n]
            f = Frame([(q.sql(dialect="duckdb"), line)], _output_names(q))
        elif self._is_work(n):
            raise _Untranslatable(f"line {line}: WORK dataset `{n}` is not created earlier in this program")
        else:
            cols = self.schema.get(n)
            if cols is None and n in self.models:  # built earlier in this program
                cols = _output_names(self.models[n].query)
            if cols is None and n in self.sql.targets and self.sql.targets[n].query is not None:
                q = self.sql.targets[n].query
                cols = _output_names(q) if q is not None else None
            f = Frame([(f"SELECT * FROM {n}", line)], cols)
        return self.apply_options(f, opts, line, opaque)

    def apply_options(self, f: Frame, o: _Options, line: int, opaque: list[str]) -> Frame:
        if o.where:
            f = f.add(f"SELECT * FROM {{prev}} WHERE {o.where}", line)
        return self.project(f, o.keep, o.drop, o.rename, line, opaque)

    def project(
        self,
        f: Frame,
        keep: list[str] | None,
        drop: list[str],
        rename: dict[str, str],
        line: int,
        opaque: list[str],
    ) -> Frame:
        if keep is None and not drop and not rename:
            return f
        if f.cols is None:
            if keep is not None and not rename:
                return f.add(f"SELECT {', '.join(_ident(c) for c in keep)} FROM {{prev}}", line, keep)
            opaque.append(
                f"line {line}: DROP/RENAME on a dataset with unknown columns (connect a database so the "
                "schema is known)"
            )
            sel = ["*"] + [f"{_ident(a)} AS {_ident(b)}" for a, b in rename.items()]
            return f.add(f"SELECT {', '.join(sel)} FROM {{prev}}", line, None)
        cols = [c for c in f.cols if (keep is None or c in keep) and c not in drop]
        sel = [f"{_ident(c)} AS {_ident(rename[c])}" if c in rename else _ident(c) for c in cols]
        return f.add(f"SELECT {', '.join(sel)} FROM {{prev}}", line, [rename.get(c, c) for c in cols])

    # ---- outputs
    def emit(
        self, target: str, f: Frame, line: int, opaque: list[str], notes: list[str], reads: list[str]
    ) -> None:
        if self._is_work(target):
            self.work[target] = f
            if f.layers:
                sql, lmap = _render(f)
                try:
                    q = sqlglot.parse_one(sql, dialect="duckdb")
                except (ParseError, TokenError):
                    q = None
                if isinstance(q, exp.Query):
                    for node in q.walk():  # generated-SQL lines -> SAS file lines, for PROC SQL readers
                        if isinstance(node, exp.Expression) and node.meta.get("line"):
                            node.meta["line"] = lmap.get(node.meta["line"], line)
                    self.sql.temps[target] = q
            if opaque:
                self._pending_opaque.setdefault(target, []).extend(opaque)
            return
        sql, line_map = _render(f) if f.layers else ("SELECT NULL AS untranslated", {})
        try:
            query = sqlglot.parse_one(sql, dialect="duckdb")
        except (ParseError, TokenError) as e:
            opaque = [*opaque, f"line {line}: generated SQL did not parse ({str(e).splitlines()[0]})"]
            query = sqlglot.parse_one("SELECT NULL AS untranslated", dialect="duckdb")
            line_map = {}
        assert isinstance(query, exp.Query)
        inherited = [o for r in reads for o in self._pending_opaque.get(r, [])]
        upstream = referenced_tables(query) if not opaque else sorted({*referenced_tables(query), *reads})
        self.models[target] = Model(
            target=target,
            query=query,
            kind="sas",
            source=SourceRef(self.file, line),
            dialect="duckdb",
            upstream=[u for u in upstream if u != target and not self._is_work(u)],
            opaque=list(dict.fromkeys([*inherited, *opaque])),
            notes=list(dict.fromkeys(notes)),
            line_map=line_map,
        )

    # ---- driver
    def run(self, src: str) -> list[Model]:
        clean = _strip_comments(src)
        sts = _split(clean)
        i = 0
        while i < len(sts):
            st = self.expand(sts[i])
            w = st.word
            if w == "%let":
                m = re.match(r"\s*%let\s+(\w+)\s*=\s*(.*)$", st.text, re.I | re.S)
                if m:
                    self.macros[m.group(1).lower()] = m.group(2).strip()
                i += 1
            elif w == "%macro":
                j = i + 1
                while j < len(sts) and sts[j].word != "%mend":
                    j += 1
                self.issue(
                    st.line, f"macro {st.rest().split('(')[0]} is defined here; macro logic is not expanded"
                )
                i = j + 1
            elif w.startswith("%") and w not in {"%put", "%global", "%local"}:
                self.issue(
                    st.line, f"macro call {w} is not expanded; tables it writes are missing from lineage"
                )
                i += 1
            elif w == "libname":
                m = re.match(r"\s*libname\s+(\w+)\s+(?:\w+\s+)?.*?\bschema\s*=\s*[\"']?(\w+)", st.text, re.I)
                if m and m.group(1).lower() not in self.libnames:
                    self.libnames[m.group(1).lower()] = m.group(2).lower()
                i += 1
            elif w == "data":
                j = self._block_end(sts, i, {"run"})
                self._data(st, [self.expand(s) for s in sts[i + 1 : j]])
                i = j + 1
            elif w == "proc":
                kind = (st.rest().split() or [""])[0].lower()
                if kind == "sql":
                    j = self._block_end(sts, i, {"quit"})
                    self._proc_sql(clean, [self.expand(s) for s in sts[i + 1 : j]])
                else:
                    j = self._block_end(sts, i, {"run", "quit"})
                    body = [self.expand(s) for s in sts[i + 1 : j]]
                    if kind == "sort":
                        self._sort(st, body)
                    elif kind in {"means", "summary"}:
                        self._means(st, body)
                    elif kind == "append":
                        self._append(st)
                    elif re.search(r"\bout\s*=", " ".join([st.text, *(b.text for b in body)]), re.I):
                        self.issue(st.line, f"PROC {kind.upper()} writes a dataset; it is not translated")
                i = j + 1
            else:
                i += 1
        return list(self.models.values()) + self.sql.models()

    @staticmethod
    def _block_end(sts: list[_St], i: int, enders: set[str]) -> int:
        j = i + 1
        while j < len(sts) and sts[j].word not in enders and sts[j].word not in {"data", "proc"}:
            j += 1
        return j

    # ---- PROC SQL
    def _proc_sql(self, clean: str, body: list[_St]) -> None:
        splitter = _Splitter(self.file, "duckdb", self.issues)
        for st in body:
            if st.word in {"reset", "title", "footnote", "options"}:
                continue
            text = "\n" * (st.line - 1) + sas_expr(st.text)
            for s in splitter.statements(text):
                for t in s.node.find_all(exp.Table):
                    full = ".".join(p for p in (t.db, t.name) if p)
                    mapped = self.name(full)
                    if mapped != full.lower():
                        parts = mapped.split(".")
                        t.set("db", exp.to_identifier(parts[0]) if len(parts) > 1 else None)
                        t.set("this", exp.to_identifier(parts[-1]))
                self.sql.take(_Stmt(s.node, s.line, s.condition))
                # a PROC SQL table read later by a DATA step must be visible as a frame too
        for key, q in self.sql.temps.items():
            if key not in self.work:
                self.work[key] = Frame(
                    [(q.sql(dialect="duckdb"), body[0].line if body else 1)], _output_names(q)
                )

    # ---- DATA step
    def _data(self, head: _St, body: list[_St]) -> None:
        outs = _datasets(head.rest())
        outs = [(n, o) for n, o in outs if n not in {"_null_"}]
        if not outs:
            return
        opaque: list[str] = []
        notes: list[str] = []
        reads: list[str] = []
        if len(outs) > 1:
            opaque.append(f"line {head.line}: DATA step writes {len(outs)} datasets (OUTPUT statements)")
        frame: Frame | None = None
        keep: list[str] | None = None
        drop: list[str] = []
        rename: dict[str, str] = {}
        merge: list[tuple[str, _Options]] | None = None
        merge_line = head.line
        k = 0
        try:
            while k < len(body):
                st = body[k]
                w = st.word
                if w == "set":
                    dss = _datasets(st.rest())
                    reads += [self.name(n) for n, _ in dss]
                    frames = [self.read(n, o, st.line, opaque) for n, o in dss]
                    frame = frames[0] if len(frames) == 1 else self._stack(frames, st.line, opaque)
                elif w == "merge":
                    merge = _datasets(st.rest())
                    merge_line = st.line
                    reads += [self.name(n) for n, _ in merge]
                elif w == "by":
                    keys = [x.lower() for x in st.rest().split() if x.lower() != "descending"]
                    if merge is not None:
                        frame, consumed = self._merge(merge, keys, body[k + 1 :], merge_line, opaque, notes)
                        merge = None
                        if consumed is not None:
                            body = [*body[: k + 1], *[b for b in body[k + 1 :] if b is not consumed]]
                elif frame is None:
                    raise _Untranslatable(f"line {st.line}: DATA step without SET or MERGE (e.g. DATALINES)")
                elif w == "where":
                    frame = frame.add(f"SELECT * FROM {{prev}} WHERE {sas_expr(st.rest())}", st.line)
                elif w == "if":
                    frame, k = self._if(frame, body, k, opaque)
                    continue
                elif w == "keep":
                    keep = [c.lower() for c in st.rest().split()]
                elif w == "drop":
                    drop += [c.lower() for c in st.rest().split()]
                elif w == "rename":
                    rename.update(
                        {a.lower(): b.lower() for a, b in re.findall(r"(\w+)\s*=\s*(\w+)", st.rest())}
                    )
                elif w in {"length", "format", "informat", "label", "attrib", "output"}:
                    pass
                elif (m := re.match(r"\s*([A-Za-z_]\w*)\s*=(?!=)\s*(.+)$", st.text, re.S)) is not None:
                    frame = self._assign(frame, m.group(1).lower(), sas_expr(m.group(2)), st.line, opaque)
                else:
                    raise _Untranslatable(
                        f"line {st.line}: DATA step statement `{w.upper()}` is not translated"
                    )
                k += 1
        except _Untranslatable as e:
            opaque.append(str(e))
        target_raw, out_opts = outs[0]
        target = self.name(target_raw)
        if frame is None or (opaque and frame is not None and not frame.layers):
            frame = Frame([], None)
        if frame.layers:
            frame = self.project(frame, keep, drop, rename, head.line, opaque)
            frame = self.apply_options(frame, out_opts, head.line, opaque)
        self.emit(target, frame, head.line, opaque, notes, [r for r in reads if not self._is_work(r)])

    def _stack(self, frames: list[Frame], line: int, opaque: list[str]) -> Frame:
        """``SET a b;``: rows of a, then rows of b, columns matched by name."""
        if any(f.cols is None for f in frames):
            opaque.append(
                f"line {line}: SET with several datasets of unknown columns is stacked by position "
                "(connect a database so the schema is known)"
            )
            sql = " UNION ALL ".join(f"SELECT * FROM ({_inline(f)}) AS _u{i}" for i, f in enumerate(frames))
            return Frame([(sql, line)], None)
        cols: list[str] = []
        for f in frames:
            cols += [c for c in f.cols or [] if c not in cols]
        parts = []
        for i, f in enumerate(frames):
            sel = ", ".join(_ident(c) if c in (f.cols or []) else f"NULL AS {_ident(c)}" for c in cols)
            parts.append(f"SELECT {sel} FROM ({_inline(f)}) AS _u{i}")
        return Frame([(" UNION ALL ".join(parts), line)], cols)

    def _merge(
        self,
        dss: list[tuple[str, _Options]],
        keys: list[str],
        rest: list[_St],
        line: int,
        opaque: list[str],
        notes: list[str],
    ) -> tuple[Frame, _St | None]:
        if len(dss) != 2:
            raise _Untranslatable(f"line {line}: MERGE of {len(dss)} datasets is not translated")
        flags = [o.in_flag for _, o in dss]
        frames = [self.read(n, o, line, opaque) for n, o in dss]
        how, consumed = "FULL OUTER", None
        for st in rest:
            if st.word != "if" or re.search(r"\bthen\b", st.text, re.I):
                continue
            cond = re.sub(r"\s+", " ", st.rest().lower()).strip()
            a, b = flags
            table = {
                f"{a} and {b}": "INNER",
                f"{b} and {a}": "INNER",
                f"{a}": "LEFT",
                f"{b}": "RIGHT",
                f"{a} or {b}": "FULL OUTER",
            }
            if a and b and cond in table:
                how, consumed = table[cond], st
            elif a and cond == a:
                how, consumed = "LEFT", st
            elif b and cond == b:
                how, consumed = "RIGHT", st
            break
        notes.append(
            f"line {line}: SAS MERGE BY {', '.join(keys)} is read as a {how} JOIN; it matches SAS when "
            "the BY keys are unique in at least one dataset"
        )
        left, right = frames
        using = ", ".join(_ident(k) for k in keys)
        if left.cols is not None and right.cols is not None:
            key_sel = [f"COALESCE(_l.{_ident(k)}, _r.{_ident(k)}) AS {_ident(k)}" for k in keys]
            lonly = [f"_l.{_ident(c)}" for c in left.cols if c not in keys and c not in right.cols]
            rsel = [f"_r.{_ident(c)}" for c in right.cols if c not in keys]
            cols = keys + [c for c in left.cols if c not in keys and c not in right.cols]
            cols += [c for c in right.cols if c not in keys]
            on = " AND ".join(f"_l.{_ident(k)} = _r.{_ident(k)}" for k in keys)
            sql = (
                f"SELECT {', '.join(key_sel + lonly + rsel)} FROM ({_inline(left)}) AS _l "
                f"{how} JOIN ({_inline(right)}) AS _r ON {on}"
            )
            return Frame([(sql, line)], cols), consumed
        opaque.append(f"line {line}: MERGE of datasets with unknown columns (connect a database)")
        sql = f"SELECT * FROM ({_inline(left)}) AS _l {how} JOIN ({_inline(right)}) AS _r USING ({using})"
        return Frame([(sql, line)], None), consumed

    def _if(self, frame: Frame, body: list[_St], k: int, opaque: list[str]) -> tuple[Frame, int]:
        st = body[k]
        m = re.match(r"\s*if\s+(.*?)\s+then\s+(.*)$", st.text, re.I | re.S)
        if m is None:  # subsetting IF
            return frame.add(f"SELECT * FROM {{prev}} WHERE {sas_expr(st.rest())}", st.line), k + 1
        cond, action = sas_expr(m.group(1)), m.group(2).strip()
        if action.lower() == "delete":
            return frame.add(f"SELECT * FROM {{prev}} WHERE NOT COALESCE({cond}, FALSE)", st.line), k + 1
        am = re.match(r"([A-Za-z_]\w*)\s*=(?!=)\s*(.+)$", action, re.S)
        if am is None:
            raise _Untranslatable(
                f"line {st.line}: IF ... THEN {action.split()[0].upper()} is not translated"
            )
        var = am.group(1).lower()
        branches = [(cond, sas_expr(am.group(2)))]
        default: str | None = None
        k += 1
        while k < len(body) and body[k].word == "else":
            rest = body[k].rest()
            em = re.match(r"if\s+(.*?)\s+then\s+([A-Za-z_]\w*)\s*=(?!=)\s*(.+)$", rest, re.I | re.S)
            dm = re.match(r"([A-Za-z_]\w*)\s*=(?!=)\s*(.+)$", rest, re.S)
            if em and em.group(2).lower() == var:
                branches.append((sas_expr(em.group(1)), sas_expr(em.group(3))))
            elif dm and dm.group(1).lower() == var:
                default = sas_expr(dm.group(2))
                k += 1
                break
            else:
                raise _Untranslatable(f"line {body[k].line}: ELSE branch assigns a different variable")
            k += 1
        known = frame.cols is not None and var in frame.cols
        fallback = default if default is not None else (_ident(var) if known else "NULL")
        if default is None and frame.cols is None:
            opaque.append(
                f"line {st.line}: IF ... THEN {var} = ... on a dataset with unknown columns "
                "(connect a database so the schema is known)"
            )
        case = "CASE " + " ".join(f"WHEN {c} THEN {e}" for c, e in branches) + f" ELSE {fallback} END"
        return self._assign(frame, var, case, st.line, opaque), k

    def _assign(self, frame: Frame, var: str, expr_sql: str, line: int, opaque: list[str]) -> Frame:
        if frame.cols is not None:
            sel = [f"{expr_sql} AS {_ident(var)}" if c == var else _ident(c) for c in frame.cols]
            cols = list(frame.cols)
            if var not in cols:
                sel.append(f"{expr_sql} AS {_ident(var)}")
                cols.append(var)
            return frame.add(f"SELECT {', '.join(sel)} FROM {{prev}}", line, cols)
        opaque.append(
            f"line {line}: assignment to {var} on a dataset with unknown columns; if it overwrites an "
            "existing variable the translation is ambiguous (connect a database so the schema is known)"
        )
        return frame.add(f"SELECT *, {expr_sql} AS {_ident(var)} FROM {{prev}}", line, None)

    # ---- other procedures
    @staticmethod
    def _opt(text: str, key: str) -> str | None:
        m = re.search(rf"\b{key}\s*=\s*([\w.&]+(?:\s*\((?:[^()]|\([^()]*\))*\))?)", text, re.I)
        return m.group(1) if m else None

    def _sort(self, head: _St, body: list[_St]) -> None:
        opaque: list[str] = []
        notes: list[str] = []
        data = self._opt(head.text, "data")
        if data is None:
            return
        out = self._opt(head.text, "out") or data
        keys = [
            x.lower() for b in body if b.word == "by" for x in b.rest().split() if x.lower() != "descending"
        ]
        (dn, do), (on, oo) = _datasets(data)[0], _datasets(out)[0]
        try:
            f = self.read(dn, do, head.line, opaque)
        except _Untranslatable as e:
            opaque.append(str(e))
            f = Frame([], None)
        if f.layers and re.search(r"\bnodupkey\b", head.text, re.I) and keys:
            k = ", ".join(_ident(x) for x in keys)
            notes.append(
                f"line {head.line}: PROC SORT NODUPKEY keeps the first row per key in input order; "
                "the passport keeps the first by key order"
            )
            f = f.add(
                f"SELECT * FROM {{prev}} QUALIFY ROW_NUMBER() OVER (PARTITION BY {k} ORDER BY {k}) = 1",
                head.line,
            )
        elif f.layers and re.search(r"\bnodup(?:recs)?\b", head.text, re.I):
            notes.append(
                f"line {head.line}: PROC SORT NODUPRECS removes adjacent duplicates; read as DISTINCT"
            )
            f = f.add("SELECT DISTINCT * FROM {prev}", head.line)
        target = self.name(on)
        if target == self.name(dn) and not self._is_work(target):
            self.issue(head.line, f"PROC SORT rewrites {target} in place; the trace treats it as unchanged")
            return
        if f.layers:
            f = self.apply_options(f, oo, head.line, opaque)
        self.emit(target, f, head.line, opaque, notes, [self.name(dn)])

    def _means(self, head: _St, body: list[_St]) -> None:
        opaque: list[str] = []
        data = self._opt(head.text, "data")
        if data is None:
            return
        cls = [x.lower() for b in body if b.word in {"class", "by"} for x in b.rest().split()]
        var = [x.lower() for b in body if b.word == "var" for x in b.rest().split()]
        outputs = [b for b in body if b.word == "output"]
        if not outputs:
            return
        o = outputs[0]
        out = self._opt(o.text, "out")
        if out is None:
            return
        if cls and not re.search(r"\bnway\b", head.text, re.I) and not any(b.word == "by" for b in body):
            opaque.append(
                f"line {head.line}: PROC {head.rest().split()[0].upper()} without NWAY also writes subtotal "
                "rows (_TYPE_), which double count when summed"
            )
        aggs: list[str] = ["COUNT(*) AS _freq_"]
        names: list[str] = ["_freq_"]
        fn = {"sum": "SUM", "n": "COUNT", "mean": "AVG", "min": "MIN", "max": "MAX", "nmiss": None}
        spec = re.sub(r"\bout\s*=\s*[\w.]+(\s*\([^)]*\))?", "", o.rest(), flags=re.I)
        for m in re.finditer(r"(\w+)\s*(\(([^)]*)\))?\s*=\s*((?:(?!\w+\s*(?:\([^)]*\))?\s*=)[\w\s])*)", spec):
            stat, sel, outnames = m.group(1).lower(), m.group(3), m.group(4).split()
            if stat not in fn or fn[stat] is None:
                if stat not in {"autoname"}:
                    opaque.append(f"line {o.line}: statistic {stat.upper()}= is not translated")
                continue
            vs = [v.lower() for v in sel.split()] if sel else var
            outs = [x.lower() for x in outnames] or vs
            for v, n in zip(vs, outs, strict=False):
                aggs.append(f"{fn[stat]}({_ident(v)}) AS {_ident(n)}")
                names.append(n)
        try:
            f = self.read(*_datasets(data)[0], head.line, opaque)
        except _Untranslatable as e:
            opaque.append(str(e))
            f = Frame([], None)
        if f.layers:
            group = f" GROUP BY {', '.join(_ident(c) for c in cls)}" if cls else ""
            sel = ", ".join([*(_ident(c) for c in cls), *aggs])
            f = f.add(f"SELECT {sel} FROM {{prev}}{group}", head.line, [*cls, *names])
        self.emit(
            self.name(_datasets(out)[0][0]), f, head.line, opaque, [], [self.name(_datasets(data)[0][0])]
        )

    def _append(self, head: _St) -> None:
        base, data = self._opt(head.text, "base"), self._opt(head.text, "data")
        if base is None or data is None:
            return
        opaque: list[str] = []
        target = self.name(_datasets(base)[0][0])
        try:
            new = self.read(*_datasets(data)[0], head.line, opaque)
        except _Untranslatable as e:
            opaque.append(str(e))
            new = Frame([], None)
        if target in self.work and new.layers:
            prev = self.work[target]
            sql = f"SELECT * FROM ({_inline(prev)}) AS _b UNION ALL SELECT * FROM ({_inline(new)}) AS _n"
            self.emit(target, Frame([(sql, head.line)], prev.cols), head.line, opaque, [], [])
            return
        opaque.append(
            f"line {head.line}: PROC APPEND adds rows to {target}, so its total also depends on rows "
            "already there"
        )
        self.emit(target, new, head.line, opaque, [], [self.name(_datasets(data)[0][0])])


def models_from_sas(
    text: str,
    file: str,
    libnames: dict[str, str] | None = None,
    schema: dict[str, list[str]] | None = None,
) -> tuple[list[Model], list[ParseIssue]]:
    issues: list[ParseIssue] = []
    models = _Program(file, libnames or {}, schema or {}, issues).run(text)
    return models, issues


def scan_sas(
    paths: list[Path],
    root: Path,
    libnames: dict[str, str] | None = None,
    schema: dict[str, list[str]] | None = None,
) -> tuple[list[Model], list[ParseIssue]]:
    models: list[Model] = []
    issues: list[ParseIssue] = []
    for base in paths:
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured SAS path does not exist"))
            continue
        files = sorted(base.rglob("*.sas")) if base.is_dir() else [base]
        for f in files:
            found, problems = models_from_sas(
                f.read_text(encoding="utf-8", errors="replace"), _rel(f, root), libnames, schema
            )
            models.extend(found)
            issues.extend(problems)
    return models, issues


__all__ = ["models_from_sas", "sas_expr", "scan_sas"]
