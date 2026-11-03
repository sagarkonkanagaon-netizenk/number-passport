"""Stored procedures, MERGE, SELECT INTO, temp tables and post-load DELETE/UPDATE in SQL scripts."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from number_passport.config import load_config
from number_passport.connectors import connect
from number_passport.parsers.sql_parser import ParseIssue, models_from_sql, parse_sql
from number_passport.project import load_project
from number_passport.trace import Tracer


def _one(text: str, dialect: str, target: str):  # type: ignore[no-untyped-def]
    models, issues = parse_sql(text, "p.sql", dialect)
    by = {m.target: m for m in models}
    assert target in by, (list(by), [str(i) for i in issues])
    return by[target], issues


TSQL_PROC = """CREATE PROCEDURE dbo.load_gold
AS
BEGIN
    SET NOCOUNT ON
    TRUNCATE TABLE gold.exposure
    SELECT e.id, e.amount INTO #stage FROM silver.exposures e WHERE e.amount > 0
    IF @full = 1
        INSERT INTO gold.exposure (id, amt)
        SELECT id, amount FROM #stage
    INSERT INTO gold.exposure (id, amt)
    SELECT id, amount FROM silver.extra
    DELETE FROM gold.exposure WHERE amt > 1000000
    EXEC('INSERT INTO x SELECT 1')
END
GO
"""


def test_tsql_procedure_without_semicolons() -> None:
    m, issues = _one(TSQL_PROC, "tsql", "gold.exposure")
    assert m.kind == "procedure" and m.source.line == 8
    assert set(m.upstream) == {"silver.exposures", "silver.extra"}  # #stage is inlined, not a table
    assert "built inside dbo.load_gold" in m.notes
    assert any("only runs when `IF @full = 1`" in n for n in m.notes)
    assert any("2 INSERT statements" in n for n in m.notes)
    assert any("DELETE at line 12" in n for n in m.notes)
    assert m.opaque == []
    assert [str(i) for i in issues] == [
        "p.sql:13: dynamic SQL (EXEC of a string) cannot be read statically; tables it writes are missing "
        "from lineage"
    ]


def test_postgres_plpgsql_procedure_and_variables() -> None:
    sql = """CREATE OR REPLACE PROCEDURE gold.load()
LANGUAGE plpgsql
AS $$
DECLARE
  n int;
BEGIN
  SELECT count(*) INTO n FROM silver.exposures;  -- a variable, not a table
  IF n > 0 THEN
    INSERT INTO gold.rwa SELECT id, amount * 0.5 AS rwa FROM silver.exposures WHERE amount > 0;
  END IF;
  RAISE NOTICE 'done';
END;
$$;
CREATE TABLE gold.other AS SELECT 1 AS a FROM silver.x;
"""
    models, issues = parse_sql(sql, "p.sql", "postgres")
    assert {m.target for m in models} == {"gold.rwa", "gold.other"}
    rwa = next(m for m in models if m.target == "gold.rwa")
    assert rwa.source.line == 9 and rwa.kind == "procedure"
    assert any("IF n > 0 THEN" in n for n in rwa.notes)
    assert issues == []


def test_snowflake_merge_and_foreign_language() -> None:
    sql = """CREATE OR REPLACE PROCEDURE load_it()
RETURNS STRING LANGUAGE SQL AS
$$
BEGIN
  MERGE INTO gold.bal t USING (SELECT id, amt FROM silver.tx) s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET amt = s.amt, total = t.total + s.amt
  WHEN NOT MATCHED THEN INSERT (id, amt) VALUES (s.id, s.amt);
  RETURN 'ok';
END;
$$;
CREATE OR REPLACE PROCEDURE js() RETURNS STRING LANGUAGE JAVASCRIPT AS $$ return 1; $$;
"""
    m, issues = _one(sql, "snowflake", "gold.bal")
    assert m.upstream == ["silver.tx"] and m.source.line == 5
    assert m.opaque and "MERGE" in m.opaque[0]
    assert any("total" in n for n in m.notes)  # running total: depends on the existing row
    assert len(issues) == 1 and "JAVASCRIPT" in str(issues[0])


def test_bigquery_procedure_with_temp_table() -> None:
    sql = """CREATE OR REPLACE PROCEDURE ds.load_rwa(run_date DATE)
BEGIN
  DECLARE x INT64 DEFAULT 0;
  CREATE TEMP TABLE stage AS SELECT id, amount FROM ds.exposures WHERE d = run_date;
  INSERT INTO ds.rwa (id, rwa) SELECT id, amount * 0.5 FROM stage;
END;
"""
    m, _ = _one(sql, "bigquery", "ds.rwa")
    assert m.upstream == ["ds.exposures"] and m.source.line == 5
    assert "stage" not in [t for t in m.upstream]


def test_oracle_plsql_procedure() -> None:
    sql = """CREATE OR REPLACE PROCEDURE load_rwa IS
  v_n NUMBER;
BEGIN
  DELETE FROM gold.rwa;
  INSERT INTO gold.rwa SELECT id, amount AS rwa FROM silver.exposures;
  COMMIT;
END load_rwa;
"""
    m, _ = _one(sql, "oracle", "gold.rwa")
    assert m.upstream == ["silver.exposures"] and m.source.line == 5


def test_select_into_and_cte_before_insert() -> None:
    sql = """WITH big AS (SELECT * FROM s.t WHERE amt > 10)
INSERT INTO g.a SELECT id, amt FROM big;
SELECT id, amt INTO g.b FROM s.u;
"""
    models, issues = parse_sql(sql, "p.sql", "tsql")
    by = {m.target: m for m in models}
    assert by["g.a"].upstream == ["s.t"]  # the CTE travels with the INSERT
    assert by["g.b"].upstream == ["s.u"] and issues == []


def test_merge_star_and_delete_only() -> None:
    sql = """MERGE INTO g.t USING s.src AS s ON g.t.id = s.id WHEN NOT MATCHED THEN INSERT *;
MERGE INTO g.u USING s.src AS s ON g.u.id = s.id WHEN MATCHED THEN DELETE;
"""
    models, _ = parse_sql(sql, "p.sql", "databricks")
    assert [m.target for m in models] == ["g.t"]


def test_bad_statement_does_not_hide_the_rest() -> None:
    sql = """CREATE TABLE a.x AS SELECT (1 FROM t;
CREATE TABLE a.y AS SELECT id FROM b.z;
"""
    models, issues = parse_sql(sql, "p.sql", "duckdb")
    assert [m.target for m in models] == ["a.y"]
    assert len(issues) == 1 and issues[0].where == "p.sql:1"
    with pytest.raises(ParseIssue):  # the strict variant (dbt) still refuses
        models_from_sql(sql, "p.sql", "duckdb")


SCRIPT = """-- nightly load
CREATE TEMP TABLE stage AS
SELECT id, amount, region FROM src.loans WHERE status <> 'void';

DELETE FROM gold.loans;
INSERT INTO gold.loans (id, amount, region) SELECT id, amount, region FROM stage WHERE region = 'EU';
INSERT INTO gold.loans (id, amount, region) SELECT id, amount, region FROM stage WHERE region = 'US';
DELETE FROM gold.loans WHERE amount > 900;
UPDATE gold.loans SET amount = 0 WHERE amount < 0;
"""


def _project(tmp: Path) -> Path:
    (tmp / "sql").mkdir()
    (tmp / "sql" / "load.sql").write_text(SCRIPT)
    (tmp / "passport.yml").write_text(
        "project: t\ndialect: duckdb\ncode:\n  sql: sql\nconnection: duckdb:///w.duckdb\n"
        "metrics:\n  total:\n    table: gold.loans\n    column: amount\n"
    )
    con = duckdb.connect(str(tmp / "w.duckdb"))
    con.execute("CREATE SCHEMA src; CREATE SCHEMA gold;")
    con.execute(
        "CREATE TABLE src.loans AS SELECT * FROM (VALUES "
        "(1, 100, 'EU', 'ok'), (2, 250, 'US', 'ok'), (3, 999, 'US', 'ok'), (4, -40, 'EU', 'ok'), "
        "(5, 70, 'EU', 'void'), (6, 30, 'APAC', 'ok')) AS t(id, amount, region, status)"
    )
    con.execute("CREATE TABLE gold.loans (id INT, amount INT, region VARCHAR)")
    con.execute(SCRIPT)
    con.close()
    return tmp


def test_script_reconciles_end_to_end(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    p = load_project(load_config(proj))
    assert set(p.models) == {"gold.loans"} and p.issues == []
    cfg = load_config(proj)
    conn = connect(cfg.connection_url)
    try:
        d = Tracer(cfg, conn).trace(cfg.metrics["total"]).to_dict()
    finally:
        conn.close()
    gold = d["hops"][0]
    assert gold["target_value"] == "350.00"  # 100 + 250, -40 updated to 0, 999 deleted
    assert gold["status"] == "reconciled", gold
    whats = [s["description"] for s in gold["steps"]]
    assert any("status <> 'void'" in w for w in whats)  # the temp table's filter is a step too
    assert any("COALESCE" in w and "900" in w for w in whats)  # the DELETE, measured as a filter
    src = next(h for h in d["hops"] if h["target"] == "src.loans")
    assert src["target_value"] == "1409.00"
