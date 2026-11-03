"""Evidence pack, verify, CLI, files connector."""

import json
from pathlib import Path

import duckdb
import pytest

from number_passport import service
from number_passport.cli import main
from number_passport.connectors import ConnectorError, DuckDBConnector, FilesConnector


def test_pack_contents_and_verify_roundtrip(fresh_bank: Path, tmp_path: Path) -> None:
    _passport, pack = service.trace(fresh_bank, "total_rwa", out=tmp_path / "pack")
    for f in ("passport.json", "passport.html", "replay.sql", "MANIFEST.sha256"):
        assert (pack / f).is_file()
    data = json.loads((pack / "passport.json").read_text())
    assert data["status"] == "gaps_found" and data["passport_version"] == 1
    html = (pack / "passport.html").read_text()
    assert "Gaps found" in html and "<svg" in html and "3,100,000.00" in html
    assert "silver_exposures.sql:20" in html
    replay = (pack / "replay.sql").read_text()
    assert replay.count("-- expected result:") == len(data["audit"])

    ok = service.verify(pack, fresh_bank)
    assert ok["ok"] and ok["queries_checked"] == len(data["audit"]), ok

    # 1) tampering with the pack is detected
    (pack / "passport.html").write_text(html.replace("Gaps found", "Verified"))
    res = service.verify(pack, fresh_bank, offline=True)
    assert not res["ok"] and not res["manifest_ok"]


def test_verify_detects_code_and_data_changes(fresh_bank: Path, tmp_path: Path) -> None:
    _, pack = service.trace(fresh_bank, "total_ead", out=tmp_path / "pack")
    sql = fresh_bank / "models" / "sql" / "silver_exposures.sql"
    sql.write_text(sql.read_text().replace("l.amount > 0", "l.amount >= 0"))
    con = duckdb.connect(str(fresh_bank / "warehouse" / "bank.duckdb"))
    con.execute("UPDATE bronze.loans SET amount = amount + 1 WHERE loan_id = 2000")
    con.close()
    res = service.verify(pack, fresh_bank)
    assert not res["ok"]
    assert any("silver_exposures.sql" in c for c in res["code_changes"])
    assert res["query_mismatches"]


def test_cli_doctor_trace_verify(bank: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["doctor", "-p", str(bank)]) == 0
    out = capsys.readouterr().out
    assert "overall: PASS" in out and "read-only" in out
    assert main(["trace", "total_rwa", "-p", str(bank), "--out", str(tmp_path / "p1"), "--strict"]) == 2
    assert "GAPS FOUND" in capsys.readouterr().out
    assert main(["trace", "total_ead", "-p", str(bank), "--out", str(tmp_path / "p2"), "--strict"]) == 0
    assert main(["verify", str(tmp_path / "p2"), "-p", str(bank)]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "trace",
                "--table",
                "silver.exposures",
                "--column",
                "ead_usd",
                "-p",
                str(bank),
                "--out",
                str(tmp_path / "p3"),
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["metric"]["table"] == "silver.exposures"
    assert main(["metrics", "-p", str(bank), "--json"]) == 0
    assert "total_rwa" in capsys.readouterr().out
    assert main(["trace", "nope", "-p", str(bank)]) == 1
    assert "unknown metric" in capsys.readouterr().err


def test_cli_trace_json_digest(bank: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["trace", "total_ead", "-p", str(bank), "--out", str(tmp_path / "p"), "--json"]) == 0
    digest = json.loads(capsys.readouterr().out)
    assert digest["status"] == "verified"
    assert digest["hops"][0]["table"].startswith("report")
    assert digest["html"].endswith("passport.html")


def test_init_writes_template(tmp_path: Path) -> None:
    assert main(["init", "-p", str(tmp_path)]) == 0
    assert "aggregates_only" in (tmp_path / "passport.yml").read_text()
    assert main(["init", "-p", str(tmp_path)]) == 1


def test_init_creates_missing_folder(tmp_path: Path) -> None:
    target = tmp_path / "new" / "project"
    assert main(["init", "-p", str(target)]) == 0
    assert (target / "passport.yml").is_file()


def test_doctor_reports_broken_setup(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "passport.yml").write_text(
        "project: broken\ncode:\n  sql: nowhere\nconnection:\n  url: duckdb:///missing.duckdb\n"
        "metrics:\n  m: {table: gold.t, column: x}\n"
    )
    assert main(["doctor", "-p", str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "does not exist" in out and "DuckDB file not found" in out and "overall: FAIL" in out


def test_files_connector_matches_warehouse(bank: Path, tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    con = duckdb.connect(str(bank / "warehouse" / "bank.duckdb"), read_only=True)
    for schema, table in [
        ("bronze", "loans"),
        ("bronze", "fx_rates"),
        ("bronze", "collateral"),
        ("ref", "risk_weights"),
        ("silver", "exposures"),
        ("silver", "collateral_adj"),
        ("gold", "rwa_by_class"),
    ]:
        (exports / schema).mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {schema}.{table} TO '{exports / schema / table}.parquet' (FORMAT parquet)")
    con.close()
    cfg = (bank / "passport.yml").read_text()
    project = tmp_path / "proj"
    import shutil

    shutil.copytree(bank, project, ignore=shutil.ignore_patterns("warehouse", "passports"))
    (project / "passport.yml").write_text(
        cfg.replace("${PASSPORT_DB_URL:-duckdb:///warehouse/bank.duckdb}", f"files:///{exports}")
    )
    passport, _ = service.trace(project, "total_ead", out=tmp_path / "pack")
    assert passport.status == "verified"
    fc = FilesConnector(str(exports))
    assert fc.probe_write() is False


def test_duckdb_connector_is_read_only(bank: Path) -> None:
    c = DuckDBConnector(str(bank / "warehouse" / "bank.duckdb"))
    assert c.probe_write() is False
    with pytest.raises(ConnectorError):
        c.query("DELETE FROM bronze.loans")
    c.close()
