"""End-to-end traces on the sample bank (DuckDB)."""

from decimal import Decimal
from pathlib import Path

import duckdb

from number_passport.config import load_config
from number_passport.connectors import connect
from number_passport.trace import Tracer

from .conftest import make_bank


def _trace(project: Path, metric: str):
    cfg = load_config(project)
    conn = connect(cfg.connection_url)
    try:
        return Tracer(cfg, conn).trace(cfg.metrics[metric]).to_dict()
    finally:
        conn.close()


def _hop(d: dict, table: str) -> dict:
    return next(h for h in d["hops"] if h["target"] == table)


def _db(project: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(project / "warehouse" / "bank.duckdb"), read_only=True)


def test_total_rwa_finds_the_manual_adjustment(bank: Path) -> None:
    d = _trace(bank, "total_rwa")
    assert d["status"] == "gaps_found"
    rh = d["report_hop"]
    assert rh["status"] == "gap" and rh["residual"] == "3100000.00"
    assert any(f["code"] == "report_mismatch" for f in rh["findings"])
    gold = _hop(d, "gold.rwa_by_class")
    assert gold["status"] == "reconciled" and gold["residual"] == "0.00"
    with _db(bank) as con:
        expected = Decimal(str(con.execute("SELECT SUM(rwa_usd) FROM gold.rwa_by_class").fetchone()[0]))
    assert Decimal(gold["target_value"]) == expected.quantize(Decimal("0.01"))


def test_silver_steps_explain_every_removed_row(bank: Path) -> None:
    d = _trace(bank, "total_ead")
    assert d["status"] == "verified"
    silver = _hop(d, "silver.exposures")
    kinds = [(s["kind"], s["rows_removed"], s["where"]) for s in silver["steps"]]
    assert kinds == [
        ("dedup", 30, "models/sql/silver_exposures.sql:8"),
        ("inner_join", 15, "models/sql/silver_exposures.sql:18"),
        ("filter", 120, "models/sql/silver_exposures.sql:20"),
        ("filter", 8, "models/sql/silver_exposures.sql:21"),
    ]
    # contributions add up exactly: relaxed + sum(deltas) == recomputed
    total = Decimal(silver["relaxed_value"]) + sum(Decimal(s["value_delta"]) for s in silver["steps"])
    assert abs(total - Decimal(silver["recomputed_value"])) <= Decimal("0.02")
    # the 15 ZAR loans have no FX rate: their value is reported in local units, checked independently
    join = silver["steps"][1]
    with _db(bank) as con:
        zar = con.execute("""
            WITH latest AS (SELECT * FROM bronze.loans
                            QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY load_ts DESC) = 1)
            SELECT SUM(amount) FROM latest WHERE currency = 'ZAR'""").fetchone()[0]
    assert Decimal(join["native_value_removed"]) == Decimal(str(zar))
    # independently computed effect of the CLOSED filter, applied after dedup and join
    with _db(bank) as con:
        closed = con.execute("""
            WITH latest AS (SELECT * FROM bronze.loans
                            QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY load_ts DESC) = 1)
            SELECT SUM(l.amount * fx.rate_to_usd) FROM latest l JOIN bronze.fx_rates fx USING (currency)
            WHERE l.status = 'CLOSED'""").fetchone()[0]
    assert Decimal(silver["steps"][2]["value_delta"]) == -Decimal(str(closed)).quantize(Decimal("0.01"))


def test_lineage_reaches_sources_and_factors(bank: Path) -> None:
    d = _trace(bank, "total_rwa")
    targets = {h["target"]: h for h in d["hops"]}
    assert targets["bronze.loans"]["model_kind"] == "source" and targets["bronze.loans"]["role"] == "primary"
    assert targets["silver.collateral_adj"]["role"] == "factor"
    assert targets["silver.collateral_adj"]["status"] == "reconciled"
    assert targets["silver.collateral_adj"]["steps"][0]["where"] == "jobs/collateral_adjust.py:14"
    assert targets["silver.collateral_adj"]["steps"][0]["rows_removed"] == 6
    assert "bronze.fx_rates.rate_to_usd" in targets["silver.exposures"]["factors"]
    assert ["silver.exposures", "gold.rwa_by_class"] in d["edges"]
    # every source table appears exactly once
    sources = [h["target"] for h in d["hops"] if h["model_kind"] == "source"]
    assert len(sources) == len(set(sources))


def test_metric_filter_applies_to_first_hop_only(bank: Path) -> None:
    d = _trace(bank, "corporate_rwa")
    gold = d["hops"][0]
    with _db(bank) as con:
        corp = con.execute(
            "SELECT SUM(rwa_usd) FROM gold.rwa_by_class WHERE asset_class = 'CORPORATE'"
        ).fetchone()[0]
    assert Decimal(gold["target_value"]) == Decimal(str(corp)).quantize(Decimal("0.01"))
    assert gold["status"] == "reconciled"
    assert d["report_hop"] is None


def test_quality_checks(bank: Path) -> None:
    d = _trace(bank, "total_ead")
    loans = _hop(d, "bronze.loans")["quality"][0]
    assert loans["row_count"] == 2030 and loans["duplicate_keys"] == 0
    assert loans["latest_load"].startswith("2026-09-30")
    silver_q = _hop(d, "silver.exposures")["quality"][0]
    assert silver_q["key"] == ["loan_id"] and silver_q["duplicate_keys"] == 0


def test_every_query_was_aggregate_and_logged(bank: Path) -> None:
    d = _trace(bank, "total_rwa")
    assert len(d["audit"]) > 20
    for q in d["audit"]:
        assert all(len(row) <= 2 for row in q["result"])  # scalars, never rows of customer data
        assert q["sql"].upper().startswith(("SELECT", "WITH"))


def test_late_arriving_data_is_caught(tmp_path: Path) -> None:
    project = make_bank(tmp_path, late_data=True)
    d = _trace(project, "total_ead")
    silver = _hop(d, "silver.exposures")
    assert silver["status"] == "gap"
    assert any(f["code"] == "table_code_mismatch" for f in silver["findings"])
    # residual = table minus what code gives today = minus the 3 late loans in USD
    expected = -(
        Decimal("750000.00")
        + Decimal("120000.00") * Decimal("1.0825")
        + Decimal("300000.00") * Decimal("1.271")
    )
    assert Decimal(silver["residual"]) == expected.quantize(Decimal("0.01"))
    assert d["status"] == "gaps_found"


def test_join_fanout_is_flagged(fresh_bank: Path) -> None:
    con = duckdb.connect(str(fresh_bank / "warehouse" / "bank.duckdb"))
    con.execute("INSERT INTO bronze.fx_rates VALUES ('EUR', 1.09, DATE '2026-09-29')")  # second EUR rate
    con.close()
    d = _trace(fresh_bank, "total_ead")
    silver = _hop(d, "silver.exposures")
    fan = [f for f in silver["findings"] if f["code"] == "join_fanout"]
    assert (
        fan and "bronze.fx_rates" in fan[0]["message"] and fan[0]["where"].endswith("silver_exposures.sql:18")
    )
    # the table was built before the duplicate rate arrived, so the code now disagrees with the table too
    assert silver["status"] == "gap"


def test_offline_mode_is_lineage_only(bank: Path) -> None:
    cfg = load_config(bank)
    d = Tracer(cfg, None).trace(cfg.metrics["total_rwa"]).to_dict()
    assert d["status"] == "lineage_only" and d["audit"] == []
    silver = _hop(d, "silver.exposures")
    assert silver["status"] == "code_only" and len(silver["steps"]) == 4
    assert all(s["value_delta"] is None for s in silver["steps"])
