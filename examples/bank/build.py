"""Build the sample bank warehouse: bronze data -> silver -> gold -> Q3 capital report.

    python examples/bank/build.py              # DuckDB warehouse at examples/bank/warehouse/bank.duckdb
    python examples/bank/build.py --late-data  # also land 3 late loans after gold was built
    python examples/bank/build.py --postgres postgresql://user@localhost/bank   # same data in Postgres

The PySpark job runs on real Spark when ``pyspark`` (and Java) are installed. Without them the script
says so and builds that one table from a hand-written SQL equivalent instead, so the demo still works.

Planted on purpose so the passport has something to find:
* 30 loans re-sent by the source system (removed by the QUALIFY dedup),
* 120 closed loans and 8 reversals with amount <= 0 (removed by the WHERE filters),
* 15 loans in ZAR, a currency with no FX rate (dropped by the inner join),
* 6 collateral records with haircut >= 100% (filtered out in the Spark job),
* the published report includes a USD 3,100,000.00 manual adjustment that no code explains.
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
WAREHOUSE = HERE / "warehouse" / "bank.duckdb"
MANUAL_ADJUSTMENT = Decimal("3100000.00")

FX = {"USD": "1.00000000", "EUR": "1.08250000", "GBP": "1.27100000", "INR": "0.01198000", "JPY": "0.00667000"}
RISK_WEIGHTS = {
    "SOVEREIGN": "0.0000",
    "BANK": "0.2000",
    "CORPORATE": "1.0000",
    "RETAIL": "0.7500",
    "MORTGAGE": "0.3500",
    "SME": "0.8500",
}


def bronze_rows(seed: int = 7) -> dict[str, list[tuple]]:
    rnd = random.Random(seed)
    t0 = datetime(2026, 9, 30, 1, 0, 0)
    loans: list[tuple] = []
    currencies = ["USD"] * 6 + ["EUR"] * 2 + ["GBP", "INR", "JPY"]
    for i in range(1, 2001):
        cur = currencies[rnd.randrange(len(currencies))]
        if i <= 15:
            cur = "ZAR"  # no FX rate published for ZAR
        amount = Decimal(rnd.randrange(5_000, 2_500_000)).quantize(Decimal("0.01"))
        if cur == "INR":
            amount *= 80
        if cur == "JPY":
            amount *= 150
        status = "OPEN"
        if 100 < i <= 220:
            status = "CLOSED"
        if 300 < i <= 308:
            amount = -amount  # reversals
        loans.append(
            (
                i,
                10_000 + rnd.randrange(400),
                rnd.choice(list(RISK_WEIGHTS)),
                cur,
                amount,
                status,
                t0 + timedelta(minutes=i % 50),
            )
        )
    # 30 re-sends: same loan, later load time, slightly updated balance
    for i in range(1500, 1530):
        lid, cp, ac, cur, amt, st, ts = loans[i - 1]
        loans.append(
            (lid, cp, ac, cur, (amt * Decimal("0.98")).quantize(Decimal("0.01")), st, ts + timedelta(hours=1))
        )
    collateral: list[tuple] = []
    cid = 1
    for i in range(1, 2001, 3):
        value = Decimal(rnd.randrange(1_000, 900_000)).quantize(Decimal("0.01"))
        haircut = Decimal(rnd.choice(["0.0000", "0.1000", "0.2000", "0.2500", "0.4000"]))
        if cid <= 6:
            haircut = Decimal("1.0000")
        collateral.append((cid, i, value, haircut))
        cid += 1
    return {
        "loans": loans,
        "fx_rates": [(c, Decimal(r), datetime(2026, 9, 30).date()) for c, r in FX.items()],
        "collateral": collateral,
        "risk_weights": [(a, Decimal(w)) for a, w in RISK_WEIGHTS.items()],
    }


DDL = """
CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS silver; CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ref;
DROP TABLE IF EXISTS bronze.loans; DROP TABLE IF EXISTS bronze.fx_rates; DROP TABLE IF EXISTS bronze.collateral;
DROP TABLE IF EXISTS ref.risk_weights;
CREATE TABLE bronze.loans (loan_id INTEGER, counterparty_id INTEGER, asset_class VARCHAR(20), currency VARCHAR(3),
    amount DECIMAL(18,2), status VARCHAR(10), load_ts TIMESTAMP);
CREATE TABLE bronze.fx_rates (currency VARCHAR(3), rate_to_usd DECIMAL(18,8), as_of DATE);
CREATE TABLE bronze.collateral (collateral_id INTEGER, loan_id INTEGER, collateral_value_usd DECIMAL(18,2),
    haircut DECIMAL(5,4));
CREATE TABLE ref.risk_weights (asset_class VARCHAR(20), risk_weight DECIMAL(5,4));
"""

COLLATERAL_SQL_EQUIVALENT = """
CREATE TABLE silver.collateral_adj AS
SELECT loan_id, SUM(collateral_value_usd * (1 - haircut)) AS collateral_usd
FROM bronze.collateral WHERE haircut < 1 GROUP BY loan_id
"""


def render_dbt(path: Path) -> str:
    """The two Jinja calls this model uses. Real projects would run `dbt run`."""
    sql = path.read_text()
    sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*source\('(\w+)',\s*'(\w+)'\)\s*\}\}", r"\1.\2", sql)
    return f"CREATE OR REPLACE TABLE gold.{path.stem} AS {sql}"


def run_spark_job(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> bool:
    """Run jobs/collateral_adjust.py on real local Spark. Returns False if Spark is unavailable."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    if shutil.which("java") is None:
        return False
    with tempfile.TemporaryDirectory() as tmp:
        runner = Path(tmp) / "run.py"
        out = Path(tmp) / "out.csv"
        runner.write_text(f"""
import csv, runpy
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.warehouse.dir", {tmp!r} + "/wh").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
schema = StructType([StructField("collateral_id", IntegerType()), StructField("loan_id", IntegerType()),
                     StructField("collateral_value_usd", DecimalType(18, 2)), StructField("haircut", DecimalType(5, 4))])
rows = [(a, b, Decimal(c), Decimal(d)) for a, b, c, d in {[(r[0], r[1], str(r[2]), str(r[3])) for r in rows]!r}]
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.createDataFrame(rows, schema).write.mode("overwrite").saveAsTable("bronze.collateral")
runpy.run_path({str(HERE / "jobs" / "collateral_adjust.py")!r}, run_name="__main__")
with open({str(out)!r}, "w", newline="") as fh:
    w = csv.writer(fh)
    for r in spark.table("silver.collateral_adj").collect():
        w.writerow([r["loan_id"], str(r["collateral_usd"])])
spark.stop()
""")
        res = subprocess.run([sys.executable, str(runner)], capture_output=True, text=True, check=False)
        if res.returncode != 0:
            print(res.stderr[-3000:], file=sys.stderr)
            raise SystemExit("the PySpark job failed")
        con.execute("DROP TABLE IF EXISTS silver.collateral_adj")
        con.execute("CREATE TABLE silver.collateral_adj (loan_id INTEGER, collateral_usd DECIMAL(38,6))")
        with out.open() as fh:
            con.executemany(
                "INSERT INTO silver.collateral_adj VALUES (?, ?)",
                [(int(a), Decimal(b)) for a, b in csv.reader(fh)],
            )
    return True


def build(path: Path = WAREHOUSE, late_data: bool = False, spark: bool = True) -> dict[str, Decimal]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    con.execute(DDL)
    data = bronze_rows()
    con.executemany("INSERT INTO bronze.loans VALUES (?, ?, ?, ?, ?, ?, ?)", data["loans"])
    con.executemany("INSERT INTO bronze.fx_rates VALUES (?, ?, ?)", data["fx_rates"])
    con.executemany("INSERT INTO bronze.collateral VALUES (?, ?, ?, ?)", data["collateral"])
    con.executemany("INSERT INTO ref.risk_weights VALUES (?, ?)", data["risk_weights"])

    con.execute((HERE / "models/sql/silver_exposures.sql").read_text())
    used_spark = spark and run_spark_job(con, data["collateral"])
    if not used_spark:
        why = "skipped" if not spark else "pyspark/java not available"
        print(f"note: Spark {why}; built silver.collateral_adj from its SQL equivalent")
        con.execute("DROP TABLE IF EXISTS silver.collateral_adj")
        con.execute(COLLATERAL_SQL_EQUIVALENT)
    con.execute(render_dbt(HERE / "models/dbt/gold/rwa_by_class.sql"))

    totals = {
        "rwa": Decimal(str(con.execute("SELECT SUM(rwa_usd) FROM gold.rwa_by_class").fetchone()[0])),
        "ead": Decimal(str(con.execute("SELECT SUM(ead_usd) FROM silver.exposures").fetchone()[0])),
    }
    reported_rwa = (totals["rwa"] + MANUAL_ADJUSTMENT).quantize(Decimal("0.01"))
    with (HERE / "reports" / "q3_capital_report.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["line_item", "value_usd"])
        w.writerow(["Total RWA", f"{reported_rwa:,.2f}"])
        w.writerow(["Total EAD", f"{totals['ead'].quantize(Decimal('0.01')):,.2f}"])

    if late_data:
        # three loans land after the nightly build: silver no longer matches its own code
        con.executemany(
            "INSERT INTO bronze.loans VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (5001, 10001, "CORPORATE", "USD", Decimal("750000.00"), "OPEN", datetime(2026, 9, 30, 6, 0)),
                (5002, 10002, "RETAIL", "EUR", Decimal("120000.00"), "OPEN", datetime(2026, 9, 30, 6, 5)),
                (5003, 10003, "SME", "GBP", Decimal("300000.00"), "OPEN", datetime(2026, 9, 30, 6, 9)),
            ],
        )
    con.close()
    return totals


def copy_to_postgres(duck: Path, url: str) -> None:
    import psycopg

    src = duckdb.connect(str(duck), read_only=True)
    tables = [
        "bronze.loans",
        "bronze.fx_rates",
        "bronze.collateral",
        "ref.risk_weights",
        "silver.exposures",
        "silver.collateral_adj",
        "gold.rwa_by_class",
    ]
    with psycopg.connect(url, autocommit=True) as pg:
        for schema in ("bronze", "silver", "gold", "ref"):
            pg.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            pg.execute(f"CREATE SCHEMA {schema}")
        for t in tables:
            cols = src.execute(f"DESCRIBE {t}").fetchall()
            ddl = ", ".join(f"{c[0]} {_pg_type(c[1])}" for c in cols)
            pg.execute(f"CREATE TABLE {t} ({ddl})")
            rows = src.execute(f"SELECT * FROM {t}").fetchall()
            ph = ", ".join(["%s"] * len(cols))
            with pg.cursor() as cur:
                cur.executemany(f"INSERT INTO {t} VALUES ({ph})", rows)
    src.close()


def _pg_type(t: str) -> str:
    t = t.upper()
    if t.startswith("DECIMAL"):
        return t.replace("DECIMAL", "NUMERIC")
    return {"VARCHAR": "TEXT", "DOUBLE": "DOUBLE PRECISION", "HUGEINT": "NUMERIC", "BIGINT": "BIGINT"}.get(
        t, t
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--late-data", action="store_true", help="land 3 loans after the build (stale silver)")
    ap.add_argument("--no-spark", action="store_true", help="skip Spark even if installed")
    ap.add_argument("--postgres", metavar="URL", help="also copy the warehouse into this PostgreSQL database")
    ap.add_argument("--out", type=Path, default=WAREHOUSE)
    a = ap.parse_args()
    totals = build(a.out, a.late_data, spark=not a.no_spark)
    print(f"built {a.out}")
    print(f"  gold total RWA      {totals['rwa']:>20,.2f}")
    print(
        f"  reported total RWA  {totals['rwa'] + MANUAL_ADJUSTMENT:>20,.2f}  (includes a manual adjustment)"
    )
    if a.postgres:
        copy_to_postgres(a.out, a.postgres)
        print(f"copied to {a.postgres}")


if __name__ == "__main__":
    main()
