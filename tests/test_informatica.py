"""Informatica PowerCenter mapping exports."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

from number_passport.config import load_config
from number_passport.connectors import connect
from number_passport.parsers.informatica_parser import infa_expr, models_from_informatica
from number_passport.project import load_project
from number_passport.trace import Tracer

DATA = Path(__file__).parent / "data" / "m_load_rwa.xml"


def test_expression_language() -> None:
    assert infa_expr("IIF(ISNULL(W), 1, W)") == "(CASE WHEN (W IS NULL) THEN 1 ELSE W END)"
    assert infa_expr("DECODE(TRUE, A > 1, 'x', 'y')") == "(CASE WHEN A > 1 THEN 'x' ELSE 'y' END)"
    assert (
        infa_expr("DECODE(R, 'EU', 1, 'US', 2)")
        == "(CASE WHEN R = 'EU' THEN 1 WHEN R = 'US' THEN 2 ELSE NULL END)"
    )
    assert infa_expr("A != B -- note") == "A <> B"


def test_mapping_translates_router_lookup_and_aggregator() -> None:
    models, issues = models_from_informatica(DATA.read_text(), "m.xml")
    by = {m.target: m for m in models}
    assert set(by) == {"rwa_by_region", "rejected_loans"} and issues == []
    rwa = by["rwa_by_region"]
    assert rwa.opaque == [] and set(rwa.upstream) == {"raw.loans", "ref.risk_weights"}
    assert any("LEFT JOIN" in n for n in rwa.notes)
    assert rwa.line_map is not None and sorted(set(rwa.line_map.values())) == [25, 26, 35, 49, 57, 64]
    rej = by["rejected_loans"]
    assert "NOT (COALESCE(AMOUNT > 0, FALSE))" in rej.sql()


def test_entity_declarations_are_refused() -> None:
    bomb = '<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa">]><POWERMART>&a;</POWERMART>'
    models, issues = models_from_informatica(bomb, "evil.xml")
    assert models == [] and "entity" in str(issues[0])


def test_untranslatable_transformation_keeps_lineage() -> None:
    raw = DATA.read_text().replace('TYPE="Aggregator"', 'TYPE="Java"', 1)
    models, _ = models_from_informatica(raw, "m.xml")
    rwa = next(m for m in models if m.target == "rwa_by_region")
    assert rwa.opaque and "Java" in rwa.opaque[0]
    assert rwa.upstream == ["raw.loans", "ref.risk_weights"]


@pytest.fixture()
def infa_project(tmp_path: Path) -> Path:
    (tmp_path / "infa").mkdir()
    shutil.copy(DATA, tmp_path / "infa" / "m_load_rwa.xml")
    (tmp_path / "passport.yml").write_text(
        "project: t\ndialect: duckdb\ncode:\n  informatica:\n    paths: infa\n    schemas: {raw_db: raw}\n"
        "connection: duckdb:///w.duckdb\n"
        "metrics:\n  rwa:\n    table: rwa_by_region\n    column: rwa\n"
    )
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    con.execute("CREATE SCHEMA raw; CREATE SCHEMA ref;")
    con.execute(
        "CREATE TABLE raw.loans AS SELECT * FROM (VALUES (1, 100.0, 'EU', 'OK'), (2, 200.0, 'US', 'OK'), "
        "(3, -5.0, 'EU', 'OK'), (4, 70.0, 'EU', 'VOID'), (5, 30.0, 'APAC', 'OK')) "
        "AS t(loan_id, amount, region, status)"
    )
    con.execute(
        "CREATE TABLE ref.risk_weights AS "
        "SELECT * FROM (VALUES ('EU', 0.5), ('US', 1.0)) AS t(region, weight)"
    )
    con.execute(
        "CREATE TABLE rwa_by_region AS SELECT * FROM "
        "(VALUES ('EU', 50.0, 1), ('US', 200.0, 1), ('APAC', 30.0, 1)) "
        "AS t(region, rwa, loans)"
    )
    con.close()
    return tmp_path


def test_mapping_reconciles_end_to_end(infa_project: Path) -> None:
    cfg = load_config(infa_project)
    assert load_project(cfg).issues == []
    conn = connect(cfg.connection_url)
    try:
        d = Tracer(cfg, conn).trace(cfg.metrics["rwa"]).to_dict()
    finally:
        conn.close()
    top = d["hops"][0]
    assert top["target_value"] == "280.00" and top["status"] == "reconciled", top
    where = {s["description"]: s["where"] for s in top["steps"]}
    assert where["filter LOANS.STATUS <> 'VOID'"] == "infa/m_load_rwa.xml:26"
    assert where["filter AMOUNT > 0"] == "infa/m_load_rwa.xml:35"
    assert {h["target"] for h in d["hops"]} >= {"raw.loans", "ref.risk_weights"}
