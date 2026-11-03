"""PySpark jobs, translated into an equivalent SQL query.

The job is never executed. We walk the Python AST, follow DataFrame variables through the common
transformation methods, and emit one SQL CTE per transformation. Every CTE keeps a pointer to the Python
line that created it, so a filter that drops rows is reported as ``jobs/collateral.py:14``.

Anything we cannot translate faithfully (UDFs, unknown methods, dynamic table names) is recorded in
``Model.opaque``. The engine then still shows lineage but refuses to claim the numbers reconcile.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp

from ..model import Model, SourceRef
from . import referenced_tables
from .sql_parser import ParseIssue, _rel

_AGG = {
    "sum": "SUM",
    "count": "COUNT",
    "avg": "AVG",
    "mean": "AVG",
    "min": "MIN",
    "max": "MAX",
    "countDistinct": "COUNT_DISTINCT",
    "count_distinct": "COUNT_DISTINCT",
}
_FUNCS = {
    "coalesce": "COALESCE",
    "greatest": "GREATEST",
    "least": "LEAST",
    "abs": "ABS",
    "round": "ROUND",
    "upper": "UPPER",
    "lower": "LOWER",
    "trim": "TRIM",
    "to_date": "CAST_DATE",
    "current_date": "CURRENT_DATE",
    "concat": "CONCAT",
    "length": "LENGTH",
}
_CMP = {ast.Eq: "=", ast.NotEq: "<>", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}
_BIN = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.Mod: "%"}
_JOIN_HOW = {
    "inner": "INNER",
    "left": "LEFT",
    "left_outer": "LEFT",
    "leftouter": "LEFT",
    "right": "RIGHT",
    "right_outer": "RIGHT",
    "full": "FULL",
    "outer": "FULL",
    "full_outer": "FULL",
    "left_semi": "SEMI",
    "semi": "SEMI",
    "left_anti": "ANTI",
    "anti": "ANTI",
    "cross": "CROSS",
}


_SET_OPS = {
    "union": "UNION ALL",  # Spark's union keeps duplicates
    "unionAll": "UNION ALL",
    "unionByName": "UNION ALL",
    "intersect": "INTERSECT",
    "intersectAll": "INTERSECT ALL",
    "subtract": "EXCEPT",
    "exceptAll": "EXCEPT ALL",
}


class Opaque(Exception):
    pass


@dataclass
class Frame:
    """A DataFrame as a chain of SQL layers (CTEs), plus its column list when we know it."""

    layers: list[tuple[str, int]] = field(default_factory=list)  # (select sql using {prev}, python line)
    cols: list[str] | None = None

    def add(self, sql: str, line: int, cols: list[str] | bool | None = True) -> Frame:
        """``cols=True`` keeps the current columns, ``None`` marks them unknown."""
        new_cols = self.cols if cols is True else (cols or None) if cols is not False else None
        return Frame([*self.layers, (sql, line)], list(new_cols) if new_cols else None)


def _lit(node: ast.expr) -> str:
    if isinstance(node, ast.Constant):
        v = node.value
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return repr(v)
    raise Opaque(f"unsupported literal {ast.unparse(node)}")


def _str(node: ast.expr) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    raise Opaque(f"expected a string literal, got {ast.unparse(node)}")


def _strs(node: ast.expr) -> list[str]:
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_str(e) for e in node.elts]
    return [_str(node)]


def _fname(func: ast.expr) -> str | None:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def col_sql(node: ast.expr) -> str:
    """Translate a PySpark Column expression to SQL."""
    if isinstance(node, ast.Constant):
        return _lit(node)
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        return _ident(_str(node.slice))
    if isinstance(node, ast.BinOp):
        if isinstance(node.op, ast.BitAnd):
            return f"({col_sql(node.left)} AND {col_sql(node.right)})"
        if isinstance(node.op, ast.BitOr):
            return f"({col_sql(node.left)} OR {col_sql(node.right)})"
        if type(node.op) in _BIN:
            return f"({col_sql(node.left)} {_BIN[type(node.op)]} {col_sql(node.right)})"
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.Invert):
            return f"(NOT {col_sql(node.operand)})"
        if isinstance(node.op, ast.USub):
            return f"(-{col_sql(node.operand)})"
    if isinstance(node, ast.Compare) and len(node.ops) == 1 and type(node.ops[0]) in _CMP:
        return f"({col_sql(node.left)} {_CMP[type(node.ops[0])]} {col_sql(node.comparators[0])})"
    if isinstance(node, ast.Call):
        name = _fname(node.func)
        recv = node.func.value if isinstance(node.func, ast.Attribute) else None
        is_f = recv is not None and isinstance(recv, ast.Name) and recv.id in {"F", "functions", "f"}
        if name in {"col", "column"} and (is_f or recv is None):
            return _ident(_str(node.args[0]))
        if name == "lit" and (is_f or recv is None):
            return _lit(node.args[0])
        if name == "expr" and (is_f or recv is None):
            return f"({_str(node.args[0])})"
        if name == "when" and (is_f or recv is None):
            return f"CASE WHEN {col_sql(node.args[0])} THEN {col_sql(node.args[1])} END"
        if recv is not None and not is_f:
            inner = col_sql(recv)
            if name == "alias":
                return inner  # aliases are handled by the caller
            if name == "isNull":
                return f"({inner} IS NULL)"
            if name == "isNotNull":
                return f"({inner} IS NOT NULL)"
            if name == "isin":
                vals = (
                    node.args[0].elts
                    if (len(node.args) == 1 and isinstance(node.args[0], (ast.List, ast.Tuple)))
                    else node.args
                )
                return f"({inner} IN ({', '.join(col_sql(v) for v in vals)}))"
            if name == "cast":
                return f"CAST({inner} AS {_str(node.args[0])})"
            if name == "otherwise" and inner.startswith("CASE WHEN") and inner.endswith(" END"):
                return f"{inner[:-4]} ELSE {col_sql(node.args[0])} END"
            if name == "when" and inner.startswith("CASE WHEN") and inner.endswith(" END"):
                return f"{inner[:-4]} WHEN {col_sql(node.args[0])} THEN {col_sql(node.args[1])} END"
            if name == "between":
                return f"({inner} BETWEEN {col_sql(node.args[0])} AND {col_sql(node.args[1])})"
        if is_f and name in _AGG:
            arg = "*" if name == "count" and node.args and _is_star(node.args[0]) else _arg(node.args[0])
            if _AGG[name] == "COUNT_DISTINCT":
                return f"COUNT(DISTINCT {arg})"
            return f"{_AGG[name]}({arg})"
        if is_f and name in _FUNCS:
            fn = _FUNCS[name]
            if fn == "CAST_DATE":
                return f"CAST({_arg(node.args[0])} AS DATE)"
            if fn == "CURRENT_DATE":
                return "CURRENT_DATE"
            return f"{fn}({', '.join(_arg(a) for a in node.args)})"
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id not in {"F"}:
        return _ident(node.attr)  # df.amount
    raise Opaque(f"cannot translate column expression `{ast.unparse(node)}`")


def _is_star(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value == "*"


def _arg(node: ast.expr) -> str:
    """Function arguments: bare strings are column names in PySpark."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return _ident(node.value)
    return col_sql(node)


def _ident(name: str) -> str:
    return name if name.replace("_", "").isalnum() and not name[0].isdigit() else f'"{name}"'


def _alias(node: ast.expr) -> str | None:
    if isinstance(node, ast.Call) and _fname(node.func) == "alias" and node.args:
        return _str(node.args[0])
    if isinstance(node, ast.Call) and _fname(node.func) in {"col", "column"} and node.args:
        return _str(node.args[0])
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


class _JobVisitor:
    def __init__(self, file: str, dialect: str, schema: dict[str, list[str]] | None = None) -> None:
        self.file = file
        self.schema = schema or {}
        self.dialect = dialect
        self.vars: dict[str, Frame] = {}
        self.views: dict[str, Frame] = {}
        self.models: list[Model] = []
        self.opaque: list[str] = []
        self.notes: list[str] = []

    # --- DataFrame expressions -------------------------------------------------------------------
    def frame(self, node: ast.expr) -> Frame:
        if isinstance(node, ast.Name):
            if node.id in self.vars:
                return self.vars[node.id]
            raise Opaque(f"line {node.lineno}: unknown DataFrame variable `{node.id}`")
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            raise Opaque(f"line {node.lineno}: unsupported DataFrame expression `{ast.unparse(node)[:80]}`")
        method = node.func.attr
        recv = node.func.value
        line = node.lineno

        # sources
        if (method == "table" and _is_spark(recv)) or (method == "table" and _is_reader(recv)):
            name = _str(node.args[0]).lower()
            if name in self.views:
                return self.views[name]
            return Frame([(f"SELECT * FROM {name}", line)], self.schema.get(name))
        if method == "sql" and _is_spark(recv):
            return Frame([(_str(node.args[0]).strip().rstrip(";"), line)], None)

        prev = self.frame(recv)
        a = node.args
        kw = {k.arg: k.value for k in node.keywords if k.arg}
        if method in {"filter", "where"}:
            cond = _str(a[0]) if isinstance(a[0], ast.Constant) else col_sql(a[0])
            return prev.add(f"SELECT * FROM {{prev}} WHERE {cond}", line)
        if method == "withColumn":
            name = _str(a[0]).lower()
            expr_sql = col_sql(a[1])
            if prev.cols is not None:
                sel = [f"{expr_sql} AS {_ident(name)}" if c == name else _ident(c) for c in prev.cols]
                if name not in prev.cols:
                    sel.append(f"{expr_sql} AS {_ident(name)}")
                cols = prev.cols if name in prev.cols else [*prev.cols, name]
                return prev.add(f"SELECT {', '.join(sel)} FROM {{prev}}", line, cols)
            self.opaque_note(
                f"line {line}: withColumn('{name}') on a DataFrame with unknown columns; "
                "if it overwrites an existing column the translation is ambiguous "
                "(connect a database so the schema is known)"
            )
            return prev.add(f"SELECT *, {expr_sql} AS {_ident(name)} FROM {{prev}}", line, None)
        if method == "withColumnRenamed":
            old, new = _str(a[0]).lower(), _str(a[1]).lower()
            if prev.cols is not None:
                sel = [f"{_ident(c)} AS {_ident(new)}" if c == old else _ident(c) for c in prev.cols]
                return prev.add(
                    f"SELECT {', '.join(sel)} FROM {{prev}}",
                    line,
                    [new if c == old else c for c in prev.cols],
                )
            return prev.add(f"SELECT *, {_ident(old)} AS {_ident(new)} FROM {{prev}}", line, None)
        if method == "select":
            cols, names = [], []
            for c in a:
                if isinstance(c, ast.Constant) and isinstance(c.value, str):
                    if c.value == "*":
                        cols.append("*")
                        names.extend(prev.cols or ["*"])
                    else:
                        cols.append(_ident(c.value.lower()))
                        names.append(c.value.lower())
                else:
                    alias = _alias(c)
                    expr_sql = col_sql(c)
                    cols.append(
                        f"{expr_sql} AS {_ident(alias.lower())}"
                        if alias and expr_sql != _ident(alias)
                        else expr_sql
                    )
                    names.append(alias.lower() if alias else "?")
            known = None if ("?" in names or "*" in names) else names
            return prev.add(f"SELECT {', '.join(cols)} FROM {{prev}}", line, known)
        if method == "drop":
            dropped = [_str(x).lower() for x in a]
            if prev.cols is not None:
                keep = [c for c in prev.cols if c not in dropped]
                return prev.add(f"SELECT {', '.join(_ident(c) for c in keep)} FROM {{prev}}", line, keep)
            self.opaque_note(f"line {line}: drop() on a DataFrame with unknown columns uses DuckDB EXCLUDE")
            return prev.add(
                f"SELECT * EXCLUDE ({', '.join(_ident(x) for x in dropped)}) FROM {{prev}}", line, None
            )
        if method in {"dropDuplicates", "drop_duplicates"}:
            dedup_keys = _strs(a[0]) if a else None
            if not dedup_keys:
                return prev.add("SELECT DISTINCT * FROM {prev}", line)
            k = ", ".join(_ident(x) for x in dedup_keys)
            self.note(
                f"line {line}: dropDuplicates keeps an arbitrary row per key in Spark; "
                "the passport keeps the first by key order"
            )
            return prev.add(
                f"SELECT * FROM {{prev}} QUALIFY ROW_NUMBER() OVER (PARTITION BY {k} ORDER BY {k}) = 1", line
            )
        if method == "distinct":
            return prev.add("SELECT DISTINCT * FROM {prev}", line)
        if method == "join":
            other = self.frame(a[0])
            on = a[1] if len(a) > 1 else kw.get("on")
            how_node = a[2] if len(a) > 2 else kw.get("how")
            how = _JOIN_HOW.get(_str(how_node).lower()) if how_node is not None else "INNER"
            if how is None:
                raise Opaque(f"line {line}: unsupported join type {ast.unparse(how_node)}")  # type: ignore[arg-type]
            right = _inline(other)
            keys: list[str] = []
            if on is None:
                cond = ""
            elif isinstance(on, (ast.Constant, ast.List, ast.Tuple)):
                keys = [x.lower() for x in _strs(on)]
                cond = f" USING ({', '.join(_ident(x) for x in keys)})"
            else:
                cond = f" ON {col_sql(on)}"
            join_cols: list[str] | None = None
            if how in {"SEMI", "ANTI"}:
                join_cols = prev.cols
            elif prev.cols is not None and other.cols is not None:
                join_cols = prev.cols + [c for c in other.cols if c not in keys]
                if len(set(join_cols)) != len(join_cols):
                    self.note(
                        f"line {line}: join produces duplicate column names "
                        f"{sorted({c for c in join_cols if join_cols.count(c) > 1})}"
                    )
            return prev.add(f"SELECT * FROM {{prev}} {how} JOIN ({right}) AS _r{line}{cond}", line, join_cols)
        if method == "groupBy" or method == "groupby":
            keys = [x for k in a for x in _strs(k)]
            return prev.add("__GROUP__:" + ",".join(keys), line, True)
        if method == "agg":
            if not prev.layers or not prev.layers[-1][0].startswith("__GROUP__:"):
                raise Opaque(f"line {line}: agg() without groupBy() is not supported")
            keys = prev.layers[-1][0].split(":", 1)[1].split(",")
            keys = [k for k in keys if k]
            aggs, names = [], []
            for c in a:
                alias = _alias(c)
                agg_sql = col_sql(c)
                aggs.append(f"{agg_sql} AS {_ident(alias.lower())}" if alias else agg_sql)
                names.append(alias.lower() if alias else "?")
            group = f" GROUP BY {', '.join(_ident(k) for k in keys)}" if keys else ""
            base = Frame(prev.layers[:-1], prev.cols)
            sel_sql = ", ".join([*(_ident(k) for k in keys), *aggs])
            out_cols = None if "?" in names else [*(k.lower() for k in keys), *names]
            return base.add(f"SELECT {sel_sql} FROM {{prev}}{group}", line, out_cols)
        if method in _SET_OPS:
            return self._set_op(prev, method, a, kw, line)
        if method in {"alias", "cache", "persist", "repartition", "coalesce", "hint", "checkpoint"}:
            return prev
        raise Opaque(f"line {line}: DataFrame method `{method}` is not supported")

    def _set_op(
        self, prev: Frame, method: str, a: list[ast.expr], kw: dict[str, ast.expr], line: int
    ) -> Frame:
        """union / unionAll / unionByName / intersect / subtract / exceptAll as one SQL set operation."""
        if not a:
            raise Opaque(f"line {line}: {method}() needs another DataFrame")
        other = self.frame(a[0])
        op = _SET_OPS[method]
        right = _inline(other)
        left_cols = prev.cols
        cols: list[str] | None = prev.cols
        left_sql = "SELECT * FROM {prev}"
        right_cols = "*"
        if left_cols is not None:  # explicit names keep column-level lineage working through the union
            left_sql = "SELECT " + ", ".join(_ident(c) for c in left_cols) + " FROM {prev}"
        if other.cols is not None:
            right_cols = ", ".join(_ident(c) for c in other.cols)
        if method == "unionByName":
            allow = kw.get("allowMissingColumns")
            allow_missing = isinstance(allow, ast.Constant) and allow.value is True
            if left_cols is None or other.cols is None:
                self.opaque_note(
                    f"line {line}: unionByName on DataFrames with unknown columns is matched by position "
                    "(connect a database so the schema is known)"
                )
                cols = None
            else:
                extra = [c for c in other.cols if c not in left_cols]
                missing = [c for c in left_cols if c not in other.cols]
                if (extra or missing) and not allow_missing:
                    raise Opaque(f"line {line}: unionByName with different columns {sorted(extra + missing)}")
                cols = [*left_cols, *extra]
                left_sql = (
                    "SELECT "
                    + ", ".join([*(_ident(c) for c in left_cols), *(f"NULL AS {_ident(c)}" for c in extra)])
                    + " FROM {prev}"
                )
                right_sel = ", ".join(_ident(c) if c in other.cols else f"NULL AS {_ident(c)}" for c in cols)
                right = f"SELECT {right_sel} FROM ({right}) AS _u{line}"
                right_cols = ", ".join(_ident(c) for c in cols)
        elif left_cols is not None and other.cols is not None and len(left_cols) != len(other.cols):
            raise Opaque(f"line {line}: {method}() of DataFrames with different column counts")
        elif method in {"union", "unionAll"} and left_cols and other.cols and left_cols != other.cols:
            self.note(
                f"line {line}: {method}() matches columns by position and the column names differ "
                f"({', '.join(left_cols)} vs {', '.join(other.cols)}); the result keeps the left names"
            )
        if method in {"subtract", "exceptAll", "intersect", "intersectAll"}:
            self.note(f"line {line}: {method}() removes rows; its effect is part of this hop's total")
        return prev.add(f"{left_sql} {op} SELECT {right_cols} FROM ({right}) AS _o{line}", line, cols)

    def opaque_note(self, msg: str) -> None:
        if msg not in self.opaque:
            self.opaque.append(msg)

    def note(self, msg: str) -> None:
        if msg not in self.notes:
            self.notes.append(msg)

    # --- statements -------------------------------------------------------------------------------
    def visit(self, tree: ast.Module) -> None:
        self.tree = tree
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                if _looks_like_frame(node.value, set(self.vars)):
                    try:
                        self.vars[node.targets[0].id] = self.frame(node.value)
                    except Opaque as e:
                        self.opaque_note(str(e))
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                self.handle_write(node.value)

    def handle_write(self, call: ast.Call) -> None:
        func = call.func
        if not isinstance(func, ast.Attribute):
            return
        if func.attr == "createOrReplaceTempView":
            try:
                self.views[_str(call.args[0]).lower()] = self.frame(func.value)
            except Opaque as e:
                self.opaque_note(str(e))
            return
        if func.attr not in {"saveAsTable", "insertInto"}:
            return
        writer = func.value
        while (
            isinstance(writer, ast.Call)
            and isinstance(writer.func, ast.Attribute)
            and writer.func.attr
            in {"mode", "format", "option", "options", "partitionBy", "bucketBy", "sortBy"}
        ):
            writer = writer.func.value
        if not (isinstance(writer, ast.Attribute) and writer.attr in {"write", "writeTo"}):
            return
        try:
            target = _str(call.args[0]).lower()
        except Opaque as e:
            self.opaque_note(f"line {call.lineno}: dynamic target table name ({e})")
            return
        try:
            frame = self.frame(writer.value)
        except Opaque as e:
            # Still record that code builds this table, so lineage never silently stops here.
            self.opaque_note(str(e))
            reads = sorted(
                {
                    _str(n.args[0]).lower()
                    for n in ast.walk(self.tree)
                    if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "table"
                    and n.args
                    and isinstance(n.args[0], ast.Constant)
                    and isinstance(n.args[0].value, str)
                }
            )
            placeholder = sqlglot.parse_one("SELECT NULL AS untranslated", dialect="duckdb")
            assert isinstance(placeholder, exp.Query)
            self.models.append(
                Model(
                    target=target,
                    query=placeholder,
                    kind="pyspark",
                    source=SourceRef(self.file, call.lineno),
                    dialect="duckdb",
                    upstream=reads,
                    opaque=list(self.opaque),
                    notes=list(self.notes),
                    line_map={},
                )
            )
            return
        sql, line_map = _render(frame)
        try:
            query = sqlglot.parse_one(sql, dialect="duckdb")
        except sqlglot.errors.ParseError as e:
            self.opaque_note(f"line {call.lineno}: generated SQL did not parse ({e})")
            return
        assert isinstance(query, exp.Query)
        self.models.append(
            Model(
                target=target,
                query=query,
                kind="pyspark",
                source=SourceRef(self.file, call.lineno),
                dialect="duckdb",
                upstream=referenced_tables(query),
                opaque=list(self.opaque),
                notes=list(self.notes),
                line_map=line_map,
            )
        )


def _root(node: ast.expr) -> ast.expr:
    while True:
        if isinstance(node, ast.Call):
            node = node.func
        elif isinstance(node, (ast.Attribute, ast.Subscript)):
            node = node.value
        else:
            return node


def _looks_like_frame(node: ast.expr, known: set[str]) -> bool:
    """A call chain rooted at the Spark session or at a DataFrame variable we already track."""
    if not isinstance(node, (ast.Call, ast.Name)):
        return False
    root = _root(node)
    return _is_spark(root) or (isinstance(root, ast.Name) and root.id in known)


def _is_spark(node: ast.expr) -> bool:
    return (isinstance(node, ast.Name) and node.id in {"spark", "session", "ss"}) or (
        isinstance(node, ast.Attribute) and node.attr in {"spark", "sparkSession", "spark_session"}
    )


def _is_reader(node: ast.expr) -> bool:
    return isinstance(node, ast.Attribute) and node.attr == "read" and _is_spark(node.value)


def _inline(frame: Frame) -> str:
    sql = ""
    for i, (layer, _) in enumerate(frame.layers):
        sql = layer if i == 0 else layer.replace("{prev}", f"({sql}) AS _s{i}")
    return sql


def _render(frame: Frame) -> tuple[str, dict[int, int]]:
    """One CTE per line so sqlglot token lines map straight back to Python lines."""
    lines: list[str] = []
    line_map: dict[int, int] = {}
    for i, (layer, pyline) in enumerate(frame.layers):
        body = layer.replace("{prev}", f"s{i - 1}") if i else layer
        body = " ".join(body.split())
        prefix = "WITH " if i == 0 else ", "
        lines.append(f"{prefix}s{i} AS ({body})")
        line_map[len(lines)] = pyline
    lines.append(f"SELECT * FROM s{len(frame.layers) - 1}")
    line_map[len(lines)] = frame.layers[-1][1]
    return "\n".join(lines), line_map


def models_from_pyspark(
    text: str, file: str, dialect: str, schema: dict[str, list[str]] | None = None
) -> list[Model]:
    try:
        tree = ast.parse(text, filename=file)
    except SyntaxError as e:
        raise ParseIssue(file, f"Python syntax error: {e}") from e
    v = _JobVisitor(file, dialect, schema)
    v.visit(tree)
    return v.models


def scan_pyspark(
    paths: list[Path], root: Path, dialect: str, schema: dict[str, list[str]] | None = None
) -> tuple[list[Model], list[ParseIssue]]:
    models: list[Model] = []
    issues: list[ParseIssue] = []
    for base in paths:
        if not base.exists():
            issues.append(ParseIssue(str(base), "configured PySpark path does not exist"))
            continue
        for f in sorted(base.rglob("*.py")) if base.is_dir() else [base]:
            try:
                models.extend(
                    models_from_pyspark(f.read_text(encoding="utf-8"), _rel(f, root), dialect, schema)
                )
            except ParseIssue as e:
                issues.append(e)
    return models, issues
