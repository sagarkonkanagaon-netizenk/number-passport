"""SAS programs: translation and an end-to-end reconciliation on DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

from number_passport.config import load_config
from number_passport.connectors import connect
from number_passport.parsers.sas_parser import models_from_sas, sas_expr
from number_passport.project import load_project
from number_passport.trace import Tracer

PROGRAM = """/* nightly capital load */
libname dw oracle schema=gold;
%let floor = 0;

data work.live(keep=loan_id amount region rate);
  set raw.loans(where=(status ne "VOID"));
  if amount > &floor;
  if region = 'XX' then delete;
  if region in ('EU','UK') then rate = 0.5;
  else if region = 'US' then rate = 0.75;
  else rate = 1;
run;

proc sort data=live out=dedup nodupkey;
  by loan_id;
run;

proc sql;
  create table dw.rwa as
  select region, sum(amount * rate) as rwa, count(*) as n
  from dedup
  group by region;
quit;

proc means data=dedup nway noprint;
  class region;
  var amount;
  output out=dw.exposure sum=total_amount n=loans;
run;

data dw.adjusted;
  merge dw.rwa(in=a) ref.overrides(in=b);
  by region;
  if a;
  rwa_final = sum(rwa, override);
run;
"""

SCHEMA = {"raw.loans": ["loan_id", "amount", "region", "status"], "ref.overrides": ["region", "override"]}


def test_sas_expressions() -> None:
    got = sas_expr("x eq \"a\" and d le '01JAN2024'd and missing(y) and z ^= . and round(v, 0.01) > 1")
    assert got == "x = 'a' and d <= DATE '2024-01-01' and (y IS NULL) and z  IS NOT NULL and ROUND(v, 2) > 1"
    assert sas_expr("sum(a, b, c)") == "(COALESCE(a, 0) + COALESCE(b, 0) + COALESCE(c, 0))"
    assert sas_expr("sum(a)") == "sum(a)"  # one argument: the PROC SQL aggregate


def test_sas_program_translates() -> None:
    models, issues = models_from_sas(PROGRAM, "load.sas", None, SCHEMA)
    by = {m.target: m for m in models}
    assert set(by) == {"gold.rwa", "gold.exposure", "gold.adjusted"}  # WORK datasets are inlined
    assert issues == []
    assert by["gold.rwa"].upstream == ["raw.loans"] and by["gold.rwa"].opaque == []
    assert set(by["gold.adjusted"].upstream) == {"gold.rwa", "ref.overrides"}
    assert by["gold.adjusted"].opaque == []
    assert any("LEFT JOIN" in n for n in by["gold.adjusted"].notes)
    assert by["gold.exposure"].source.line == 25
    # each generated step points back at its SAS line
    assert sorted(set(by["gold.exposure"].line_map.values())) == [5, 6, 7, 8, 9, 14, 25]


def test_sas_unsupported_is_reported_not_hidden() -> None:
    prog = """data gold.x;
  set raw.y;
  retain running 0;
  running + amount;
run;
%do_stuff(a=1);
proc transpose data=raw.y out=gold.t; run;
"""
    models, issues = models_from_sas(prog, "p.sas", None, {"raw.y": ["amount"]})
    [m] = models
    assert m.target == "gold.x" and m.upstream == ["raw.y"]
    assert m.opaque and "RETAIN" in m.opaque[0]
    assert [i.where for i in issues] == ["p.sas:6", "p.sas:7"]


def _project(tmp: Path) -> Path:
    (tmp / "sas").mkdir()
    (tmp / "sas" / "load.sas").write_text(PROGRAM)
    (tmp / "passport.yml").write_text(
        "project: t\ndialect: duckdb\ncode:\n  sas:\n    paths: sas\n    libnames: {dw: gold}\n"
        "connection: duckdb:///w.duckdb\n"
        "metrics:\n  rwa:\n    table: gold.rwa\n    column: rwa\n"
        "  exposure:\n    table: gold.exposure\n    column: total_amount\n"
    )
    con = duckdb.connect(str(tmp / "w.duckdb"))
    con.execute("CREATE SCHEMA raw; CREATE SCHEMA ref; CREATE SCHEMA gold;")
    con.execute(
        "CREATE TABLE raw.loans AS SELECT * FROM (VALUES "
        "(1, 100.0, 'EU', 'OK'), (1, 100.0, 'EU', 'OK'), (2, 200.0, 'US', 'OK'), (3, 50.0, 'XX', 'OK'), "
        "(4, -5.0, 'EU', 'OK'), (5, 70.0, 'APAC', 'VOID'), (6, 30.0, 'APAC', 'OK')) "
        "AS t(loan_id, amount, region, status)"
    )
    con.execute("CREATE TABLE ref.overrides AS SELECT 'EU' AS region, 10.0 AS override")
    # what the SAS job produced (written by hand, independently of the translation)
    con.execute(
        "CREATE TABLE gold.rwa AS SELECT * FROM "
        "(VALUES ('EU', 50.0, 1), ('US', 150.0, 1), ('APAC', 30.0, 1)) "
        "AS t(region, rwa, n)"
    )
    con.execute(
        "CREATE TABLE gold.exposure AS SELECT * FROM (VALUES ('EU', 1, 100.0, 1), ('US', 1, 200.0, 1), "
        "('APAC', 1, 30.0, 1)) AS t(region, _freq_, total_amount, loans)"
    )
    con.close()
    return tmp


def test_sas_job_reconciles_end_to_end(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    cfg = load_config(proj)
    assert load_project(cfg).issues == []
    conn = connect(cfg.connection_url)
    try:
        rwa = Tracer(cfg, conn).trace(cfg.metrics["rwa"]).to_dict()
        ex = Tracer(cfg, conn).trace(cfg.metrics["exposure"]).to_dict()
    finally:
        conn.close()
    gold = rwa["hops"][0]
    assert gold["target_value"] == "230.00" and gold["status"] == "reconciled", gold
    where = {s["description"]: s["where"] for s in gold["steps"]}
    assert where["filter status <> 'VOID'"] == "sas/load.sas:6"
    assert where["filter amount > 0"] == "sas/load.sas:7"
    assert any(w == "sas/load.sas:14" for w in where.values())  # NODUPKEY
    deltas = {s["where"]: s["value_delta"] for s in gold["steps"]}
    assert deltas["sas/load.sas:8"] is not None
    assert ex["hops"][0]["status"] == "reconciled"
    assert ex["hops"][1]["target"] == "raw.loans" and ex["hops"][1]["target_value"] == "545.00"
