"""Command line: ``passport <command>``."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import __version__, service
from .config import ConfigError
from .connectors import ConnectorError
from .doctor import run_doctor, summarize
from .guard import GuardViolation
from .trace import TraceError

_SYMBOL = {"pass": "ok  ", "warn": "WARN", "fail": "FAIL", "skip": "skip"}

INIT_TEMPLATE = """# Number Passport configuration. Docs: https://github.com/sagarkonkanagaon-netizenk/number-passport
project: {name}
dialect: duckdb            # dialect your pipeline SQL is written in (duckdb, postgres, snowflake, spark, ...)

code:
  sql: models              # CREATE TABLE ... AS / INSERT INTO ... SELECT files
  # dbt:
  #   manifest: target/manifest.json   # preferred: run `dbt compile` first
  # pyspark: jobs
  # airflow: dags

connection:
  # Read-only credentials only. Environment variables are expanded: ${{VAR}} or ${{VAR:-default}}
  url: ${{PASSPORT_DB_URL}}

privacy:
  mode: aggregates_only    # only totals and counts ever leave the database

tolerance:
  absolute: 0.01

metrics:
  # total_rwa:
  #   label: Total risk-weighted assets
  #   table: gold.rwa_by_class
  #   column: rwa_usd
  #   report: {{file: reports/capital.csv, row: Total RWA, column: value}}
"""


def _print_json(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def cmd_doctor(a: argparse.Namespace) -> int:
    checks = run_doctor(a.project)
    overall = summarize(checks)
    if a.json:
        _print_json({"overall": overall, "checks": [c.to_dict() for c in checks]})
    else:
        width = max(len(c.name) for c in checks)
        for c in checks:
            print(f"[{_SYMBOL[c.status]}] {c.name.ljust(width)}  {c.detail}")
        print(f"\noverall: {overall.upper()}")
    return 1 if overall == "fail" else 0


def cmd_trace(a: argparse.Namespace) -> int:
    passport, pack = service.trace(a.project, a.metric, a.table, a.column, a.agg, a.where, a.out, a.offline)
    s = service.summary(passport, pack)
    if a.json:
        _print_json(s)
    else:
        m = s["metric"]
        print(f"{m['label'] or m['name']}: {s['status'].upper().replace('_', ' ')}")
        for h in s["hops"]:
            val = h["value"] if h["value"] is not None else "-"
            extra = f"  unexplained {h['unexplained']}" if h["unexplained"] not in (None, "0.00") else ""
            print(
                f"  hop {h['hop']:>2} {h['status']:<17} {h['table']:<32} {h['measure']:<26} {val:>22}{extra}"
            )
            for st in h["steps"]:
                rows = "" if st["rows_removed"] is None else f"{st['rows_removed']:>6} rows"
                eff = "" if st["value_effect"] is None else f" {st['value_effect']:>18}"
                print(f"           - {st['what'][:58]:<58} {rows}{eff}  {st['where']}")
            for f in h["findings"]:
                print(f"           ! {f}")
        print(f"\n{s['queries_run']} read-only queries · evidence pack: {pack}")
        print(f"open {pack / 'passport.html'}")
    return 2 if passport.status == "gaps_found" and a.strict else 0


def cmd_lineage(a: argparse.Namespace) -> int:
    _print_json(service.lineage(a.project, a.table))
    return 0


def cmd_metrics(a: argparse.Namespace) -> int:
    ms = service.metrics(a.project)
    if a.json:
        _print_json(ms)
    else:
        for m in ms:
            rep = " (report cell)" if m["has_report_cell"] else ""
            where = f" where {m['where']}" if m["where"] else ""
            print(f"{m['name']:<24} {m['agg']}({m['table']}.{m['column']}){where}{rep}")
    return 0


def cmd_verify(a: argparse.Namespace) -> int:
    res = service.verify(a.pack, None if a.no_project else a.project, a.offline)
    if a.json:
        _print_json(res)
    else:
        print("manifest:", "intact" if res["manifest_ok"] else "MODIFIED")
        for n in res["notes"]:
            print("  note:", n)
        print("code:", "unchanged" if not res["code_changes"] else "CHANGED")
        for c in res["code_changes"]:
            print("  -", c)
        print(f"queries: {res['queries_checked']} re-run, {len(res['query_mismatches'])} mismatch(es)")
        for q in res["query_mismatches"]:
            print("  -", q)
        print("result:", "VERIFIED" if res["ok"] else "NOT VERIFIED")
    return 0 if res["ok"] else 1


def cmd_init(a: argparse.Namespace) -> int:
    target = Path(a.project) / "passport.yml"
    if target.exists() and not a.force:
        print(f"{target} already exists (use --force to overwrite)", file=sys.stderr)
        return 1
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(INIT_TEMPLATE.format(name=Path(a.project).resolve().name), encoding="utf-8")
    print(f"wrote {target}; edit it, then run `passport doctor`")
    return 0


def cmd_mcp(a: argparse.Namespace) -> int:
    try:
        from .mcp_server import main as mcp_main
    except ImportError:
        print("the MCP server needs: pip install 'number-passport[mcp]'", file=sys.stderr)
        return 1
    mcp_main(a.project, a.http, a.host, a.port)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="passport", description="Trace a reported number to its source rows.")
    p.add_argument("--version", action="version", version=f"number-passport {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp: argparse.ArgumentParser, json_flag: bool = True) -> None:
        sp.add_argument("--project", "-p", default=".", help="folder with passport.yml (default: .)")
        if json_flag:
            sp.add_argument("--json", action="store_true", help="machine-readable output")

    d = sub.add_parser("doctor", help="check every connection point")
    common(d)
    d.set_defaults(func=cmd_doctor)

    t = sub.add_parser("trace", help="trace a number and write an evidence pack")
    t.add_argument("metric", nargs="?", help="configured metric name")
    t.add_argument("--table")
    t.add_argument("--column")
    t.add_argument("--agg", default="sum", choices=["sum", "count"])
    t.add_argument("--where", help="SQL filter on the metric table")
    t.add_argument("--out", help="evidence pack folder (default: passports/<metric>-<timestamp>)")
    t.add_argument("--offline", action="store_true", help="lineage only, no database")
    t.add_argument("--strict", action="store_true", help="exit 2 when gaps are found (for CI)")
    common(t)
    t.set_defaults(func=cmd_trace)

    li = sub.add_parser("lineage", help="code-only lineage for one table")
    li.add_argument("table")
    common(li, json_flag=False)
    li.set_defaults(func=cmd_lineage)

    me = sub.add_parser("metrics", help="list configured metrics")
    common(me)
    me.set_defaults(func=cmd_metrics)

    v = sub.add_parser("verify", help="re-check an evidence pack")
    v.add_argument("pack")
    v.add_argument("--offline", action="store_true", help="do not re-run queries")
    v.add_argument("--no-project", action="store_true", help="only check pack integrity")
    common(v)
    v.set_defaults(func=cmd_verify)

    i = sub.add_parser("init", help="write a starter passport.yml")
    i.add_argument("--force", action="store_true")
    common(i, json_flag=False)
    i.set_defaults(func=cmd_init)

    m = sub.add_parser("mcp", help="run the MCP server (stdio by default)")
    m.add_argument("--http", action="store_true", help="streamable HTTP instead of stdio")
    m.add_argument("--host", default="127.0.0.1")
    m.add_argument("--port", type=int, default=8765)
    common(m, json_flag=False)
    m.set_defaults(func=cmd_mcp)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except (ConfigError, ConnectorError, TraceError, GuardViolation) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
