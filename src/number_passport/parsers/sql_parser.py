"""SQL scripts and stored procedures.

Turns every statement that fills a table into a :class:`Model`:

* ``CREATE TABLE/VIEW ... AS``, ``INSERT INTO ... SELECT`` (with or without a column list),
  ``INSERT OVERWRITE`` and ``SELECT ... INTO`` (SQL Server);
* several ``INSERT``\\ s into one table in one script (combined with ``UNION ALL``);
* ``DELETE ... WHERE`` and ``UPDATE ... SET ... WHERE`` that run after a table is loaded (folded in as a
  filter and a ``CASE WHEN``, so their effect on the number is measured like any other step);
* temporary and staging tables (``#tmp``, ``CREATE TEMP TABLE``) inlined into the statement that reads them;
* ``MERGE`` (lineage through the ``USING`` source; marked not reproducible because an upsert depends on
  rows already in the table);
* stored procedures, functions and anonymous blocks in SQL Server, Postgres (PL/pgSQL), Snowflake,
  BigQuery, Oracle (PL/SQL), Databricks and MySQL: the statements inside the body are read the same way.

Statements that cannot be parsed are reported and skipped; the rest of the file still counts.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from ..model import Model, SourceRef
from . import arg, referenced_tables, set_arg, table_name


class ParseIssue(Exception):
    def __init__(self, where: str, message: str) -> None:
        super().__init__(f"{where}: {message}")
        self.where = where
        self.message = message


# ---------------------------------------------------------------------------------------------- text scanning
_STMT_WORDS = {
    "INSERT", "CREATE", "MERGE", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "DECLARE", "SET", "EXEC",
    "EXECUTE", "PRINT", "RETURN", "IF", "ELSE", "ELSIF", "ELSEIF", "WHILE", "BEGIN", "END", "GRANT",
    "REVOKE", "USE", "CALL", "COMMIT", "ROLLBACK", "RAISERROR", "THROW", "OPEN", "FETCH", "CLOSE",
    "DEALLOCATE", "ANALYZE", "VACUUM", "COPY", "LOOP", "FOR", "RAISE", "PERFORM", "EXCEPTION", "GET",
    "OPTIMIZE", "REFRESH", "CACHE", "UNCACHE", "COMMENT", "SELECT", "WITH", "VALUES", "GO",
}  # fmt: skip
_CONTROL = {"IF", "ELSE", "ELSIF", "ELSEIF", "WHILE", "BEGIN", "END", "LOOP", "FOR", "EXCEPTION", "WHEN"}
_LOADS = {"INSERT", "CREATE", "MERGE", "UPDATE", "DELETE", "TRUNCATE", "DECLARE", "EXEC", "EXECUTE", "SET"}
_DOLLAR = re.compile(r"\$([A-Za-z_]\w*)?\$")
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_INNER_DML = re.compile(
    r"\b(?:INSERT\s+(?:INTO|OVERWRITE)|MERGE\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:(?:GLOBAL\s+|LOCAL\s+)?TEMP(?:ORARY)?\s+)?(?:TABLE|VIEW)|DELETE\s+FROM|UPDATE\s+\S+\s+SET)\b",
    re.I,
)
_LOOKS_LOAD = re.compile(
    r"^\s*(?:INSERT|MERGE|CREATE\s+(?:OR\s+REPLACE\s+)?(?:\w+\s+)*?(?:TABLE|VIEW)|WITH|SELECT\b.*\bINTO\b)",
    re.I | re.S,
)
_DYNAMIC = re.compile(r"\b(?:EXEC(?:UTE)?\s*\(|EXEC(?:UTE)?\s+(?:IMMEDIATE|sp_executesql)\b)", re.I)
_PROC_HEAD = re.compile(
    r"\bCREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?(?:DEFINER\s*=\s*\S+\s+)?"
    r"(PROC(?:EDURE)?|FUNCTION)\s+([\w.\[\]\"`@#$]+)",
    re.I,
)
_ALTER_HEAD = re.compile(r"\bALTER\s+(PROC(?:EDURE)?)\s+([\w.\[\]\"`@#$]+)", re.I)
_DO_HEAD = re.compile(r"(?<![\w$])DO\s*(?=\$)", re.I)
_EXECUTE_BLOCK = re.compile(r"\bEXECUTE\s+IMMEDIATE\s*(?=\$)", re.I)
_FOREIGN_LANG = re.compile(
    r"\bLANGUAGE\s+'?(JAVASCRIPT|PYTHON|JAVA|SCALA|PLPYTHON\w*|PLV8|PLPERL\w*|C)\b", re.I
)


def _mask(text: str) -> str:
    """Same length as ``text``, with comments and the inside of string literals blanked (newlines kept).

    Dollar-quoted bodies are blanked too, but their delimiters are kept so callers can find them.
    """
    out = list(text)
    n = len(text)

    def blank(a: int, b: int) -> None:
        for j in range(a, min(b, n)):
            if out[j] != "\n":
                out[j] = " "

    i = 0
    while i < n:
        c = text[i]
        if c == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif c == "/" and text.startswith("/*", i):
            j = text.find("*/", i + 2)
            j = n if j < 0 else j + 2
            blank(i, j)
            i = j
        elif c == "'":
            j = i + 1
            while j < n:
                if text[j] == "'" and j + 1 < n and text[j + 1] == "'":
                    j += 2
                    continue
                if text[j] == "\\" and j + 1 < n:
                    j += 2
                    continue
                if text[j] == "'":
                    break
                j += 1
            blank(i + 1, j)
            i = j + 1
        elif c == "$" and (m := _DOLLAR.match(text, i)) and not (i > 0 and (text[i - 1].isalnum())):
            tag = m.group(0)
            close = text.find(tag, m.end())
            close = n if close < 0 else close
            blank(m.end(), close)
            i = close + len(tag)
        else:
            i += 1
    return "".join(out)


def _blank(text: str, a: int, b: int) -> str:
    return text[:a] + re.sub(r"[^\n]", " ", text[a:b]) + text[b:]


def _line_at(text: str, pos: int) -> int:
    """1-based line number of character ``pos``."""
    return text.count("\n", 0, pos) + 1


def _shift_lines(node: exp.Expression, by: int) -> None:
    if not by:
        return
    for n in node.walk():
        if isinstance(n, exp.Expression) and n.meta.get("line"):
            n.meta["line"] += by


def _first_word(masked: str) -> str:
    s = masked.lstrip()
    m = _WORD.match(s)
    return m.group(0).upper() if m else (s[:1] if s else "")


# ---------------------------------------------------------------------------------------------- procedures
@dataclass
class _Body:
    name: str
    text: str  # the body, same characters as in the file
    start: int  # offset of the body in the file text
    language: str | None = None  # set when the body is not SQL (JavaScript, Python...)


def _balanced_end(masked: str, begin: int) -> int:
    """Given the offset of a block ``BEGIN``, return the offset just after its matching ``END``."""
    depth = 0
    i = begin
    tokens = list(_WORD.finditer(masked, begin))
    k = 0
    while k < len(tokens):
        t = tokens[k]
        w = t.group(0).upper()
        nxt = tokens[k + 1].group(0).upper() if k + 1 < len(tokens) else ""
        if w == "BEGIN" and nxt not in {"TRAN", "TRANSACTION", "WORK", "DISTRIBUTED", "TRY", "CATCH"}:
            depth += 1
        elif w == "BEGIN" and nxt in {"TRY", "CATCH"}:
            depth += 1
            k += 1
        elif w == "CASE":
            depth += 1
        elif w == "END":
            if nxt in {"IF", "LOOP", "WHILE", "FOR", "REPEAT"}:
                k += 2
                continue
            if nxt in {"TRY", "CATCH", "CASE"}:
                k += 1
            depth -= 1
            if depth == 0:
                i = tokens[k].end()
                return i
        k += 1
    return len(masked)


def _extract_bodies(text: str, dialect: str) -> tuple[str, list[_Body]]:
    """Cut stored procedures, functions and anonymous blocks out of ``text``.

    Returns the text with those definitions blanked (line numbers unchanged) and each body.
    """
    masked = _mask(text)
    bodies: list[_Body] = []
    spans: list[tuple[int, int]] = []
    heads = [(m, m.group(2)) for m in _PROC_HEAD.finditer(masked)]
    heads += [(m, m.group(2)) for m in _ALTER_HEAD.finditer(masked)]
    heads += [(m, "anonymous block") for m in _DO_HEAD.finditer(masked)]
    heads += [(m, "EXECUTE IMMEDIATE block") for m in _EXECUTE_BLOCK.finditer(masked)]
    heads.sort(key=lambda h: h[0].start())
    pos = 0
    for m, raw_name in heads:
        if m.start() < pos:
            continue  # inside a body we already took
        name = raw_name.strip('[]"`').replace("].[", ".").replace('"."', ".")
        # header: up to the first dollar quote, block BEGIN, or AS/IS at paren depth 0
        depth = 0
        j = m.end()
        body_start = body_end = end = -1
        n = len(masked)
        lang = None
        while j < n:
            c = masked[j]
            if c == "(":
                depth += 1
            elif c == ")":
                depth = max(0, depth - 1)
            elif depth == 0 and c == "$" and (dq := _DOLLAR.match(masked, j)):
                close = masked.find(dq.group(0), dq.end())
                close = n if close < 0 else close
                body_start, body_end = dq.end(), close
                semi_at = masked.find(";", close + len(dq.group(0)))
                nxt = re.search(r"\b(CREATE|DO|ALTER)\b", masked[close + len(dq.group(0)) :])
                stop = close + len(dq.group(0)) + (nxt.start() if nxt else n)
                end = semi_at + 1 if 0 <= semi_at < stop else close + len(dq.group(0))
                break
            elif depth == 0 and c == ";":
                break  # a declaration only (e.g. CREATE FUNCTION ... EXTERNAL NAME ...;)
            elif depth == 0 and (w := _WORD.match(masked, j)) and (j == 0 or not masked[j - 1].isalnum()):
                word = w.group(0).upper()
                if word in {"BEGIN"}:
                    body_start = j
                    end = body_end = _balanced_end(masked, j)
                    term = re.match(r"\s*(?:\w+\s*)?;", masked[end:])
                    end += term.end() if term else 0
                    break
                if word in {"AS", "IS"}:
                    after = masked[w.end() :]
                    if re.match(r"\s*\$", after) or re.match(r"\s*'", after):
                        j = w.end()
                        if re.match(r"\s*'", after):  # Snowflake: AS '<body>'
                            q = masked.index("'", j)
                            close = masked.find("'", q + 1)
                            body_start, body_end = q + 1, close
                            end = close + 1
                            break
                        continue
                    if dialect in {"tsql"}:
                        body_start = w.end()
                        go = re.search(r"(?im)^[ \t]*GO[ \t]*\d*[ \t]*;?[ \t]*$", masked[body_start:])
                        nxt = _PROC_HEAD.search(masked, body_start)
                        stops = [n]
                        if go:
                            stops.append(body_start + go.start())
                        if nxt:
                            stops.append(nxt.start())
                        end = body_end = min(stops)
                        break
                    b = re.search(r"\bBEGIN\b", masked[w.end() :], re.I)
                    if b is None:
                        break
                    body_start = w.end()
                    end = body_end = _balanced_end(masked, w.end() + b.start())
                    term = re.match(r"\s*(?:\w+\s*)?;", masked[end:])
                    end += term.end() if term else 0
                    break
                if (lm := _FOREIGN_LANG.match(masked, j)) is not None:
                    lang = lm.group(1)
                j = w.end()
                continue
            j += 1
        if body_start < 0:
            continue
        if lang is None and (lm := _FOREIGN_LANG.search(masked[m.end() : body_start])):
            lang = lm.group(1)
        bodies.append(_Body(name, text[body_start:body_end], body_start, lang))
        spans.append((m.start(), end))
        pos = end
    out = text
    for s_from, s_to in spans:
        out = _blank(out, s_from, s_to)
    return out, bodies


# ---------------------------------------------------------------------------------------------- statements
@dataclass
class _Stmt:
    node: exp.Expression
    line: int  # file line (before line_offset)
    condition: str | None = None  # text of the IF/WHILE the statement sits under, if any


class _Splitter:
    """Statements of one script, even when the whole script does not parse (T-SQL without semicolons,
    procedural code, a statement sqlglot does not know)."""

    def __init__(self, file: str, dialect: str, issues: list[ParseIssue]) -> None:
        self.file = file
        self.dialect = dialect
        self.issues = issues

    def _parse(self, chunk: str) -> list[exp.Expression]:
        """Parse, treating sqlglot's catch-all ``Command`` that swallowed a data statement as a failure."""
        log = logging.getLogger("sqlglot")
        level = log.level
        log.setLevel(logging.ERROR)  # sqlglot warns on every fallback to Command; we report our own issues
        try:
            out = [s for s in sqlglot.parse(chunk, dialect=self.dialect) if s is not None]
        finally:
            log.setLevel(level)
        for s in out:
            if isinstance(s, exp.Command) and _INNER_DML.search(_mask(s.sql(dialect=self.dialect))):
                raise ParseError(f"unsupported statement: {s.sql(dialect=self.dialect)[:60]}")
        return out

    def statements(self, text: str) -> list[_Stmt]:
        """``text`` is a whole file, or a body padded with newlines, so sqlglot lines are file lines."""
        if not text.strip():
            return []
        try:
            parsed = self._parse(text)
        except (ParseError, TokenError):
            return self._fallback(text)
        out: list[_Stmt] = []
        for s in parsed:
            out.extend(self._flatten(s, None))
        return out

    def _flatten(self, s: exp.Expression, cond: str | None) -> list[_Stmt]:
        """Data statements in ``s``, looking inside IF/WHILE/BEGIN blocks."""
        if not isinstance(s, exp.Expression):
            return []
        if isinstance(s, (exp.IfBlock, exp.WhileBlock)):
            c = s.this.sql(dialect=self.dialect) if isinstance(s.this, exp.Expression) else "condition"
            c = ("IF " if isinstance(s, exp.IfBlock) else "WHILE ") + c
            out: list[_Stmt] = []
            for key in ("true", "false", "body", "expression"):
                part = s.args.get(key)
                if isinstance(part, exp.Expression):
                    out.extend(self._flatten(part, c if key != "false" else f"NOT ({c})"))
            return out
        if isinstance(s, exp.Block):
            out = []
            for x in s.expressions:
                out.extend(self._flatten(x, cond))
            return out
        if isinstance(s, (exp.Command, exp.Execute)) and _DYNAMIC.search(s.sql(dialect=self.dialect)):
            self._dynamic(_min_line(s))
            return []
        return [_Stmt(s, _min_line(s), cond)]

    def _dynamic(self, line: int) -> None:
        self.issues.append(
            ParseIssue(
                f"{self.file}:{line}",
                "dynamic SQL (EXEC of a string) cannot be read statically; tables it writes are missing "
                "from lineage",
            )
        )

    def _fallback(self, text: str) -> list[_Stmt]:
        masked = _mask(text)
        # candidate boundaries: after each ';', and at line starts that begin with a statement word
        cands: list[tuple[int, bool]] = []
        depth = 0
        line_start = True
        for i, c in enumerate(masked):
            if c == "(":
                depth += 1
            elif c == ")":
                depth = max(0, depth - 1)
            elif c == ";":
                depth = 0
                cands.append((i + 1, True))
            if c == "\n":
                line_start = True
                continue
            if line_start and not c.isspace():
                line_start = False
                if depth == 0 and _first_word(masked[i : i + 40]) in _STMT_WORDS:
                    cands.append((i, False))
        cands.append((len(text), True))

        out: list[_Stmt] = []
        start = 0
        pending_cond: str | None = None
        for b, hard in cands:
            if b <= start:
                continue
            chunk = text[start:b]
            if not chunk.strip() or not masked[start:b].strip(" \n\t;"):
                start = b
                continue
            word = _first_word(masked[start:b])
            nxt_word = _first_word(masked[b : b + 40]) if b < len(text) else ""
            if word == "GO":
                start = b
                continue
            try:
                parsed = self._parse(chunk)
                ok = True
            except (ParseError, TokenError):
                parsed, ok = [], False
            if not hard:
                if ok:
                    # INSERT INTO t / INSERT INTO t (a, b) parse alone; the query is on the next lines
                    if any(isinstance(p, exp.Insert) and p.expression is None for p in parsed):
                        continue
                    if nxt_word in {"SELECT", "VALUES", "WITH"} and masked[start:b].rstrip().upper().endswith(
                        ("UNION", "ALL", "EXCEPT", "INTERSECT", "AS", "DISTINCT")
                    ):
                        continue
                else:
                    tail = masked[start:b].rstrip().upper()
                    forced = (
                        nxt_word in _LOADS
                        and word not in {"MERGE", "WITH"}
                        and not tail.endswith(("THEN", " AS", "(", ","))
                    ) or (word in _CONTROL and nxt_word in _LOADS | {"SELECT", "WITH"})
                    if not forced:
                        continue
            line = _line_at(text, start + len(chunk) - len(chunk.lstrip()))
            if ok:
                stmts: list[_Stmt] = []
                for p in parsed:
                    _shift_lines(p, line - 1)
                    stmts.extend(self._flatten(p, None))
                is_control = word in _CONTROL or all(
                    isinstance(p, (exp.IfBlock, exp.WhileBlock, exp.Command)) for p in parsed
                )
                if is_control and not any(_is_load(s.node) for s in stmts):
                    if word in {"IF", "ELSE", "WHILE", "ELSIF", "ELSEIF", "WHEN", "FOR", "LOOP"}:
                        pending_cond = " ".join(chunk.split())[:80]
                    start = b
                    continue
                for s in stmts:
                    if s.condition is None and pending_cond and _is_load(s.node):
                        s.condition = pending_cond
                out.extend(stmts)
                if any(_is_load(s.node) for s in stmts):
                    pending_cond = None
            else:
                rec = self._recover(text, masked, start, b)
                if rec is not None:
                    cond_text = " ".join(text[start : start + rec[1]].split())[:80] or None
                    for s in rec[0]:
                        s.condition = s.condition or cond_text or pending_cond
                    out.extend(rec[0])
                    pending_cond = None
                elif _DYNAMIC.search(masked[start:b]):
                    self._dynamic(line)
                elif _LOOKS_LOAD.match(masked[start:b]):
                    first = " ".join(chunk.split())[:70]
                    self.issues.append(
                        ParseIssue(f"{self.file}:{line}", f"could not parse statement: {first}")
                    )
                elif word in {"IF", "ELSE", "WHILE", "ELSIF", "ELSEIF", "WHEN", "FOR", "LOOP"}:
                    pending_cond = " ".join(chunk.split())[:80]
            start = b
        return out

    def _recover(self, text: str, masked: str, a: int, b: int) -> tuple[list[_Stmt], int] | None:
        """A data statement behind procedural code on the same line (``IF x THEN INSERT ...``)."""
        m = _INNER_DML.search(masked, a, b)
        if m is None or m.start() == a:
            return None
        try:
            parsed = self._parse(text[m.start() : b])
        except (ParseError, TokenError):
            return None
        line = _line_at(text, m.start())
        out: list[_Stmt] = []
        for p in parsed:
            _shift_lines(p, line - 1)
            out.extend(self._flatten(p, None))
        return out, m.start() - a


def _min_line(node: exp.Expression) -> int:
    lines = [n.meta["line"] for n in node.walk() if isinstance(n, exp.Expression) and n.meta.get("line")]
    return min(lines) if lines else 1


def _is_load(node: exp.Expression) -> bool:
    return isinstance(
        node, (exp.Create, exp.Insert, exp.Merge, exp.Delete, exp.Update, exp.TruncateTable)
    ) or (isinstance(node, exp.Select) and node.args.get("into") is not None)


# ---------------------------------------------------------------------------------------------- models
@dataclass
class _Target:
    query: exp.Query | None
    line: int
    loads: list[int] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    opaque: list[str] = field(default_factory=list)


def _key(t: exp.Table) -> str:
    ident = t.this
    name = table_name(t)
    if isinstance(ident, exp.Identifier) and (ident.args.get("temporary") or ident.args.get("global_")):
        return "#" + name
    return name


def _is_temp_create(c: exp.Create) -> bool:
    props = c.args.get("properties")
    return isinstance(props, exp.Properties) and any(
        isinstance(p, exp.TemporaryProperty) for p in props.expressions
    )


class _Builder:
    def __init__(
        self,
        file: str,
        dialect: str,
        kind: str,
        line_offset: int,
        procedure: str | None = None,
    ) -> None:
        self.file = file
        self.dialect = dialect
        self.kind = kind
        self.line_offset = line_offset
        self.procedure = procedure
        self.targets: dict[str, _Target] = {}
        self.temps: dict[str, exp.Query] = {}

    # ---- helpers
    def _inline_temps(self, q: exp.Query) -> exp.Query:
        if not self.temps:
            return q
        cte_names = {c.alias_or_name.lower() for c in q.find_all(exp.CTE)}
        for t in list(q.find_all(exp.Table)):
            k = _key(t)
            if k in self.temps and table_name(t) not in cte_names:
                sub = self.temps[k].copy().subquery(t.alias or t.name)
                t.replace(sub)
        return q

    def _where(self, line: int) -> str:
        return f"line {line + self.line_offset}"

    def _set(self, key: str, q: exp.Query, line: int, replace: bool, how: str) -> None:
        tgt = self.targets.get(key)
        if tgt is None or replace or tgt.query is None:
            if tgt is not None and tgt.query is not None and replace:
                tgt.notes.append(
                    f"rebuilt at {self._where(line)} ({how}); earlier loads in this script are ignored"
                )
            new = _Target(q, line, [line])
            if tgt is not None:
                new.notes = tgt.notes
                new.opaque = tgt.opaque
            self.targets[key] = new
            return
        tgt.query = exp.union(tgt.query, q, distinct=False)
        tgt.loads.append(line)
        lines = ", ".join(self._where(x).removeprefix("line ") for x in tgt.loads)
        tgt.notes = [n for n in tgt.notes if not n.startswith("rows come from")]
        tgt.notes.append(
            f"rows come from {len(tgt.loads)} INSERT statements (lines {lines}), combined as UNION ALL"
        )

    def _cond_note(self, key: str, st: _Stmt, what: str) -> None:
        if st.condition and key in self.targets:
            self.targets[key].notes.append(
                f"{what} at {self._where(st.line)} only runs when `{st.condition}`; the trace assumes it ran"
            )

    # ---- statement handlers
    def take(self, st: _Stmt) -> None:
        s = st.node
        if isinstance(s, exp.Create):
            kind = (s.kind or "").upper()
            tbl = s.this.this if isinstance(s.this, exp.Schema) else s.this
            if (
                kind in {"TABLE", "VIEW"}
                and isinstance(tbl, exp.Table)
                and isinstance(s.expression, exp.Query)
            ):
                q = self._inline_temps(s.expression.copy())
                key = _key(tbl)
                if _is_temp_create(s) or key.startswith("#"):
                    self.temps[key] = q
                    return
                self._set(key, q, st.line, replace=True, how=f"CREATE {kind}")
                self._cond_note(key, st, f"CREATE {kind}")
            return
        if isinstance(s, exp.Insert):
            self._insert(s, st)
            return
        if isinstance(s, exp.Select) and s.args.get("into") is not None:
            into = s.args["into"]
            tbl = into.this
            if not isinstance(tbl, exp.Table):
                return
            if self.procedure is not None and self.dialect not in {"tsql"}:
                return  # PL/pgSQL, PL/SQL, Snowflake: SELECT ... INTO <variable>
            q = s.copy()
            q.set("into", None)
            q = self._inline_temps(q)
            key = _key(tbl)
            if key.startswith("#") or into.args.get("temporary"):
                self.temps[key] = q
                return
            self._set(key, q, st.line, replace=True, how="SELECT INTO")
            self._cond_note(key, st, "SELECT INTO")
            return
        if isinstance(s, exp.Merge):
            self._merge(s, st)
            return
        if isinstance(s, exp.Delete):
            self._delete(s, st)
            return
        if isinstance(s, exp.Update):
            self._update(s, st)
            return
        if isinstance(s, exp.TruncateTable):
            for t in s.expressions:
                if isinstance(t, exp.Table):
                    k = _key(t)
                    if k in self.targets:
                        self.targets[k].query = None
                        self.targets[k].loads = []
                    self.temps.pop(k, None)

    def _insert(self, s: exp.Insert, st: _Stmt) -> None:
        q0 = s.expression
        if not isinstance(q0, exp.Query):
            return  # INSERT ... VALUES: constants, no lineage
        tbl = s.this.this if isinstance(s.this, exp.Schema) else s.this
        if not isinstance(tbl, exp.Table):
            return
        q = q0.copy()
        w = arg(s, "with")
        if w is not None and arg(q, "with") is None:
            set_arg(q, "with", w.copy())  # T-SQL: WITH cte AS (...) INSERT INTO t SELECT ... FROM cte
        cols = (
            [c.name.lower() for c in s.this.expressions if isinstance(c, (exp.Identifier, exp.Column))]
            if isinstance(s.this, exp.Schema)
            else []
        )
        key = _key(tbl)
        notes: list[str] = []
        if cols and not _alias_columns(q, cols):
            notes.append(
                f"INSERT at {self._where(st.line)} lists columns {', '.join(cols)} but selects `*`; "
                "columns are matched by name"
            )
        q = self._inline_temps(q)
        if key.startswith("#") or key in self.temps:
            prev = self.temps.get(key)
            self.temps[key] = (
                q if prev is None or s.args.get("overwrite") else exp.union(prev, q, distinct=False)
            )
            return
        self._set(key, q, st.line, replace=bool(s.args.get("overwrite")), how="INSERT OVERWRITE")
        self.targets[key].notes.extend(notes)
        self._cond_note(key, st, "INSERT")

    def _merge(self, s: exp.Merge, st: _Stmt) -> None:
        tbl = s.this
        using = s.args.get("using")
        if not isinstance(tbl, exp.Table) or not isinstance(using, exp.Expression):
            return
        key = _key(tbl)
        talias = (tbl.alias or tbl.name).lower()
        values: dict[str, exp.Expression] = {}
        star = False
        skipped: list[str] = []
        whens = s.args.get("whens")
        for w in whens.expressions if isinstance(whens, exp.Expression) else []:
            then = w.args.get("then")
            if isinstance(then, exp.Insert):
                if isinstance(then.this, exp.Star):
                    star = True
                    continue
                cols = then.this.expressions if isinstance(then.this, exp.Tuple) else []
                vals = then.expression.expressions if isinstance(then.expression, exp.Tuple) else []
                for c, v in zip(cols, vals, strict=False):
                    values[c.name.lower()] = v
            elif isinstance(then, exp.Update):
                for eq in then.expressions:
                    if not isinstance(eq, exp.EQ):
                        continue
                    c = eq.this.name.lower()
                    v = eq.expression
                    if any(col.table.lower() == talias for col in v.find_all(exp.Column)):
                        skipped.append(c)  # t.total = t.total + s.amount: depends on the existing row
                        continue
                    values.setdefault(c, v)
        if not values and not star:
            return  # only DELETEs
        src = using.copy()
        q: exp.Query
        if star:
            q = exp.select(exp.Star()).from_(src)
        else:
            q = exp.select(*[v.copy().as_(c) for c, v in values.items()]).from_(src)
        q = self._inline_temps(q)
        tgt = self.targets.get(key)
        if tgt is not None and tgt.query is not None:
            tgt.opaque.append(
                f"MERGE at {self._where(st.line)} upserts into rows loaded earlier; the combined result is "
                "not recomputed"
            )
            return
        self._set(key, q, st.line, replace=True, how="MERGE")
        t = self.targets[key]
        t.opaque.append(
            f"MERGE at {self._where(st.line)} upserts into {key}: the result also depends on rows already in "
            "the table, so the number is traced through the USING source but not re-derived"
        )
        if skipped:
            t.notes.append(
                f"MERGE updates {', '.join(skipped)} from the existing row (e.g. running totals); "
                "those columns "
                "are not traced further"
            )
        self._cond_note(key, st, "MERGE")

    def _delete(self, s: exp.Delete, st: _Stmt) -> None:
        tbl = s.this
        if not isinstance(tbl, exp.Table):
            return
        key = _key(tbl)
        where = s.args.get("where")
        tgt = self.targets.get(key)
        if key in self.temps and tgt is None:
            if where is None:
                self.temps.pop(key)
            else:
                wrapped = _wrap(self.temps[key], tbl, where.this, negate=True)
                if wrapped is not None:
                    self.temps[key] = wrapped
            return
        if tgt is None or tgt.query is None:
            return  # deleting from a table this script does not build (or before it is loaded)
        if where is None and not s.args.get("using"):
            tgt.query = None
            tgt.loads = []
            return
        if s.args.get("using") or s.args.get("tables"):
            tgt.opaque.append(f"DELETE with a join at {self._where(st.line)} removes rows after the load")
            return
        wrapped = _wrap(tgt.query, tbl, where.this, negate=True)
        if wrapped is None:
            tgt.opaque.append(
                f"DELETE at {self._where(st.line)} removes rows after the load, and the load selects `*` so "
                "it cannot be folded in"
            )
            return
        tgt.query = wrapped
        tgt.notes.append(f"DELETE at {self._where(st.line)} is applied as a filter on the loaded rows")
        self._cond_note(key, st, "DELETE")

    def _update(self, s: exp.Update, st: _Stmt) -> None:
        tbl = s.this
        if not isinstance(tbl, exp.Table):
            return
        key = _key(tbl)
        tgt = self.targets.get(key)
        if tgt is None or tgt.query is None:
            return
        if arg(s, "from") is not None or s.args.get("joins"):
            tgt.opaque.append(
                f"UPDATE ... FROM at {self._where(st.line)} changes values after the load using another table"
            )
            return
        where = s.args.get("where")
        sets: dict[str, exp.Expression] = {}
        for eq in s.expressions:
            if isinstance(eq, exp.EQ):
                sets[eq.this.name.lower()] = eq.expression
        names = _output_names(tgt.query)
        if names is None or any(c not in names for c in sets):
            tgt.opaque.append(f"UPDATE at {self._where(st.line)} changes values after the load")
            return
        alias = (tbl.alias or tbl.name).lower()
        inner = tgt.query.subquery(alias)
        cond = where.this.copy() if where is not None else None
        proj: list[exp.Expression] = []
        for n in names:
            col = exp.column(n, table=alias)
            if n in sets:
                v = sets[n].copy()
                e = exp.Case(ifs=[exp.If(this=cond.copy(), true=v)], default=col) if cond is not None else v
                proj.append(e.as_(n))
            else:
                proj.append(col)
        tgt.query = exp.select(*proj).from_(inner)
        tgt.notes.append(
            f"UPDATE at {self._where(st.line)} changes {', '.join(sets)} after the load; it is folded in as "
            "CASE WHEN"
        )
        self._cond_note(key, st, "UPDATE")

    # ---- output
    def models(self) -> list[Model]:
        out: list[Model] = []
        for key, t in self.targets.items():
            if t.query is None or key.startswith("#"):
                continue
            notes = list(t.notes)
            if self.procedure:
                notes.insert(0, f"built inside {self.procedure}")
            out.append(
                Model(
                    target=key,
                    query=t.query,
                    kind=self.kind,
                    source=SourceRef(self.file, t.line + self.line_offset),
                    dialect=self.dialect,
                    upstream=[u for u in referenced_tables(t.query) if u != key],
                    opaque=list(t.opaque),
                    notes=notes,
                    line_offset=self.line_offset,
                )
            )
        return out


def _output_names(q: exp.Query) -> list[str] | None:
    sel = q
    while isinstance(sel, exp.SetOperation):
        sel = sel.this
    if not isinstance(sel, exp.Select):
        return None
    names: list[str] = []
    for e in sel.expressions:
        if isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star)):
            return None
        if not e.alias_or_name:
            return None
        names.append(e.alias_or_name.lower())
    return names


def _alias_columns(q: exp.Query, cols: list[str]) -> bool:
    """Name each projection after the INSERT's column list; False when it selects ``*``."""
    sels: list[exp.Select] = []
    node: exp.Expression = q
    while isinstance(node, exp.SetOperation):
        sels.append(node.expression) if isinstance(node.expression, exp.Select) else None
        node = node.this
    if isinstance(node, exp.Select):
        sels.append(node)
    if not sels:
        return False
    for sel in sels:
        exprs = sel.expressions
        if any(
            isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
            for e in exprs
        ):
            return False
        if len(exprs) != len(cols):
            return False
        sel.set(
            "expressions",
            [(e.this if isinstance(e, exp.Alias) else e).as_(c) for e, c in zip(exprs, cols, strict=True)],
        )
    return True


def _wrap(q: exp.Query, tbl: exp.Table, cond: exp.Expression, negate: bool) -> exp.Query | None:
    names = _output_names(q)
    if names is None:
        return None
    alias = (tbl.alias or tbl.name).lower()
    c = cond.copy()
    for col in c.find_all(exp.Column):
        if col.table and col.table.lower() in {tbl.name.lower(), alias}:
            col.set("table", exp.to_identifier(alias))
            col.set("db", None)
            col.set("catalog", None)
    keep = exp.not_(exp.func("COALESCE", c, exp.false())) if negate else c
    return exp.select(*[exp.column(n, table=alias) for n in names]).from_(q.subquery(alias)).where(keep)


# ---------------------------------------------------------------------------------------------- entry points
def parse_sql(
    text: str, file: str, dialect: str, kind: str = "sql", line_offset: int = 0
) -> tuple[list[Model], list[ParseIssue]]:
    """Models in one SQL file, and the statements that could not be read (the rest still count)."""
    issues: list[ParseIssue] = []
    if dialect == "tsql":
        text = re.sub(r"(?im)^([ \t]*)GO([ \t]*\d*[ \t]*;?[ \t]*)$", lambda m: " " * len(m.group(0)), text)
    outer, bodies = _extract_bodies(text, dialect)
    splitter = _Splitter(file, dialect, issues)
    models: list[Model] = []
    builder = _Builder(file, dialect, kind, line_offset)
    for st in splitter.statements(outer):
        builder.take(st)
    models.extend(builder.models())
    for body in bodies:
        if body.language:
            issues.append(
                ParseIssue(
                    f"{file}:{_line_at(text, body.start) + line_offset}",
                    f"{body.name} is written in {body.language}, not SQL; "
                    "tables it writes are missing from lineage",
                )
            )
            continue
        b = _Builder(file, dialect, "procedure" if kind == "sql" else kind, line_offset, procedure=body.name)
        # parse the body in place (padded with the file's earlier lines) so line numbers stay file lines
        padded = "\n" * (_line_at(text, body.start) - 1) + body.text
        for st in splitter.statements(_strip_block(padded)):
            b.take(st)
        models.extend(b.models())
    return models, issues


def _strip_block(text: str) -> str:
    """Blank a body's outer BEGIN ... END, plain DECLARE sections and transaction words."""
    masked = _mask(text)
    m = re.search(r"\bBEGIN\b", masked, re.I)
    if m and (not masked[: m.start()].strip(" \n\t;") or re.match(r"(?is)\s*DECLARE\b", masked)):
        end = _balanced_end(masked, m.start())
        last = masked.rfind("END", m.start(), end) if end else -1
        text = _blank(text, m.start(), m.end())
        if last > 0:
            text = _blank(text, last, end)
        if re.match(r"(?is)\s*DECLARE\b", masked[: m.start()]):
            text = _blank(text, 0, m.start())
        masked = _mask(text)
    for w in re.finditer(
        r"(?im)^\s*(BEGIN\s+(?:TRAN(?:SACTION)?|TRY|CATCH)|END\s+(?:TRY|CATCH)|COMMIT(?:\s+TRAN(?:SACTION)?)?|"
        r"ROLLBACK(?:\s+TRAN(?:SACTION)?)?)\b[^;\n]*;?",
        masked,
    ):
        text = _blank(text, w.start(1), w.end())
    return text


def models_from_sql(
    text: str, file: str, dialect: str, kind: str = "sql", line_offset: int = 0
) -> list[Model]:
    """Strict variant (dbt models are a single statement): any unreadable statement is an error."""
    models, issues = parse_sql(text, file, dialect, kind, line_offset)
    if issues:
        raise issues[0]
    return models


def scan_sql(paths: list[Path], root: Path, dialect: str) -> tuple[list[Model], list[ParseIssue]]:
    models: list[Model] = []
    issues: list[ParseIssue] = []
    for base in paths:
        files = sorted(base.rglob("*.sql")) if base.is_dir() else [base]
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured SQL path does not exist"))
            continue
        for f in files:
            rel = _rel(f, root)
            try:
                found, problems = parse_sql(f.read_text(encoding="utf-8"), rel, dialect)
            except (ParseError, TokenError) as e:  # pragma: no cover - parse_sql handles these itself
                issues.append(ParseIssue(rel, f"SQL parse error: {e}"))
                continue
            models.extend(found)
            issues.extend(problems)
    return models, issues


def _rel(p: Path, root: Path) -> str:
    try:
        return str(p.relative_to(root))
    except ValueError:
        return str(p)
