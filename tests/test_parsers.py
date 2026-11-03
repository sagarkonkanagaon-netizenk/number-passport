import json
from pathlib import Path

import pytest

from number_passport.config import load_config
from number_passport.guard import Guard
from number_passport.parsers.airflow_parser import scan_airflow
from number_passport.parsers.dbt_parser import scan_dbt_manifest, scan_dbt_sources
from number_passport.parsers.pyspark_parser import models_from_pyspark
from number_passport.parsers.sql_parser import ParseIssue, models_from_sql
from number_passport.project import load_project
from number_passport.reconcile import HopAnalyzer, Tolerance

from .conftest import EXAMPLE


def test_sql_create_insert_and_view() -> None:
    sql = """
    CREATE TABLE a.x AS SELECT * FROM b.y;
    -- not a model
    GRANT SELECT ON a.x TO reader;
    INSERT INTO a.z SELECT id, amt FROM a.x JOIN c.w ON w.id = x.id;
    CREATE VIEW a.v AS WITH t AS (SELECT 1 AS k) SELECT * FROM t;
    """
    models = models_from_sql(sql, "m.sql", "duckdb")
    assert [m.target for m in models] == ["a.x", "a.z", "a.v"]
    assert models[1].upstream == ["a.x", "c.w"]
    assert models[2].upstream == []  # CTE names are not tables


def test_sql_parse_error_is_reported() -> None:
    with pytest.raises(ParseIssue):
        models_from_sql("CREATE TABLE x AS SELECT (1 FROM t", "bad.sql", "duckdb")


def test_example_project_models_and_lines() -> None:
    p = load_project(load_config(EXAMPLE))
    assert set(p.models) == {"silver.exposures", "silver.collateral_adj", "gold.rwa_by_class"}
    assert p.issues == [] and p.cycles() == []
    exp_m = p.models["silver.exposures"]
    an = HopAnalyzer(exp_m, Guard(lambda s: [], "duckdb"), {}, Tolerance())
    where = {s.description.split(" ")[0] + str(i): an._where(s.line) for i, s in enumerate(an.steps)}
    assert list(where.values()) == [
        "models/sql/silver_exposures.sql:8",
        "models/sql/silver_exposures.sql:18",
        "models/sql/silver_exposures.sql:20",
        "models/sql/silver_exposures.sql:21",
    ]
    gold = p.models["gold.rwa_by_class"]
    assert gold.kind == "dbt"
    assert set(gold.upstream) == {"silver.exposures", "silver.collateral_adj", "ref.risk_weights"}
    an = HopAnalyzer(gold, Guard(lambda s: [], "duckdb"), {}, Tolerance())
    assert an._where(an.steps[0].line) == "models/dbt/gold/rwa_by_class.sql:19"  # jinja config line kept


def test_orchestration_from_airflow() -> None:
    p = load_project(load_config(EXAMPLE))
    o = p.models["silver.collateral_adj"].orchestration
    assert o and o[0]["dag_id"] == "regulatory_reporting" and o[0]["task_id"] == "adjust_collateral"
    assert o[0]["schedule"] == "0 2 * * *"
    found, issues = scan_airflow([EXAMPLE / "dags"], EXAMPLE)
    assert issues == [] and "silver_exposures.sql" in found


def test_dbt_unknown_ref_and_opaque_jinja(tmp_path: Path) -> None:
    d = tmp_path / "models" / "marts"
    d.mkdir(parents=True)
    (d / "a.sql").write_text("select * from {{ ref('missing') }}")
    (d / "b.sql").write_text(
        "select id, {{ var('k') }} as x from t\n{% if is_incremental() %}where id > 0{% endif %}"
    )
    models, issues = scan_dbt_sources([tmp_path / "models"], tmp_path, "duckdb", {})
    assert any("ref('missing')" in str(i) for i in issues)
    b = next(m for m in models if m.target == "marts.b")
    assert len(b.opaque) == 3


def test_dbt_manifest(tmp_path: Path) -> None:
    manifest = {
        "nodes": {
            "model.p.m1": {
                "resource_type": "model",
                "schema": "gold",
                "name": "m1",
                "original_file_path": "models/m1.sql",
                "compiled_code": "select a, sum(b) as s from silver.t where c > 0 group by a",
            },
            "test.p.t1": {"resource_type": "test"},
        }
    }
    f = tmp_path / "manifest.json"
    f.write_text(json.dumps(manifest))
    models, issues = scan_dbt_manifest(f, tmp_path, "duckdb")
    assert issues == [] and models[0].target == "gold.m1" and models[0].upstream == ["silver.t"]
    assert models[0].source.line is None


PYSPARK = """
from pyspark.sql import functions as F
df = spark.table("bronze.trades")
df = df.filter((F.col("status") == "BOOKED") & F.col("notional").isNotNull())
df = df.withColumn("notional_usd", F.col("notional") * F.col("fx"))
df = df.withColumnRenamed("book", "desk")
j = df.join(spark.table("ref.desks"), on="desk", how="left")
agg = j.groupBy("desk").agg(F.sum("notional_usd").alias("total_usd"), F.count("*").alias("n"))
agg.write.mode("overwrite").saveAsTable("gold.desk_totals")
"""


def test_pyspark_translation_known_schema() -> None:
    schema = {"bronze.trades": ["id", "book", "status", "notional", "fx"], "ref.desks": ["desk", "region"]}
    [m] = models_from_pyspark(PYSPARK, "job.py", "duckdb", schema)
    assert m.target == "gold.desk_totals" and m.opaque == []
    assert m.upstream == ["bronze.trades", "ref.desks"]
    sql = m.sql()
    assert "notional * fx" in sql.replace("(", "").replace(")", "")
    assert "book AS desk" in sql
    assert "LEFT JOIN" in sql and "GROUP BY desk" in sql
    # each CTE maps back to its python line
    assert sorted(set(m.line_map.values())) == [3, 4, 5, 6, 7, 8]


def test_pyspark_without_schema_flags_ambiguity() -> None:
    [m] = models_from_pyspark(PYSPARK, "job.py", "duckdb", None)
    assert any("withColumn" in o for o in m.opaque)


@pytest.mark.parametrize(
    "snippet, needle",
    [
        ("df = spark.table('a.b').filter(my_udf(F.col('x')))", "cannot translate"),
        ("df = spark.table('a.b').rdd.map(lambda r: r)", "unsupported"),
        ("df = spark.table('a.b').transform(fn)", "not supported"),
    ],
)
def test_pyspark_untranslatable_is_opaque(snippet: str, needle: str) -> None:
    code = f"from pyspark.sql import functions as F\n{snippet}\ndf.write.saveAsTable('x.y')\n"
    [m] = models_from_pyspark(code, "job.py", "duckdb", {"a.b": ["x"]})
    # the table is still known to be built by this job, but flagged: never a silent wrong translation
    assert m.target == "x.y" and m.upstream == ["a.b"]
    assert any(needle in o for o in m.opaque)
