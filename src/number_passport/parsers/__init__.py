"""Parsers turn pipeline code (SQL, dbt, PySpark, Airflow) into :class:`~number_passport.model.Model`."""

from __future__ import annotations

from sqlglot import exp


def table_name(t: exp.Table) -> str:
    """Fully qualified, lower-case table name without alias (``schema.table`` or ``table``)."""
    parts = [p for p in (t.catalog, t.db, t.name) if p]
    return ".".join(parts).lower()


def referenced_tables(query: exp.Expression) -> list[str]:
    """Physical tables a query reads, excluding its own CTE names."""
    cte_names = {c.alias_or_name.lower() for c in query.find_all(exp.CTE)}
    out: list[str] = []
    for t in query.find_all(exp.Table):
        n = table_name(t)
        if n and n not in cte_names and n not in out:
            out.append(n)
    return out


def arg(node: exp.Expression, name: str) -> exp.Expression | None:
    """sqlglot renamed some arg keys (``with`` -> ``with_``, ``from`` -> ``from_``); read either."""
    v = node.args.get(name + "_")
    return v if v is not None else node.args.get(name)


def set_arg(node: exp.Expression, name: str, value: exp.Expression | None) -> None:
    key = name + "_" if (name + "_") in type(node).arg_types else name
    node.set(key, value)
