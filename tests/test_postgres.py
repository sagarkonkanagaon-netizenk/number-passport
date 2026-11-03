"""Same trace against a real PostgreSQL warehouse.

Set NP_TEST_POSTGRES_URL (owner, used to load data) and optionally NP_TEST_POSTGRES_READER_URL (a role with
only SELECT grants). CI starts a postgres service and sets both.
"""

import os
import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from number_passport import service
from number_passport.connectors import PostgresConnector

from .conftest import build_mod

OWNER = os.environ.get("NP_TEST_POSTGRES_URL")
READER = os.environ.get("NP_TEST_POSTGRES_READER_URL")
pytestmark = pytest.mark.skipif(not OWNER, reason="NP_TEST_POSTGRES_URL not set")


@pytest.fixture(scope="module")
def pg_project(bank: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    import psycopg

    assert OWNER
    build_mod.copy_to_postgres(bank / "warehouse" / "bank.duckdb", OWNER)
    if READER:
        reader_role = READER.split("://", 1)[1].split(":", 1)[0].split("@", 1)[0]
        with psycopg.connect(OWNER, autocommit=True) as pg:
            for schema in ("bronze", "silver", "gold", "ref"):
                pg.execute(f"GRANT USAGE ON SCHEMA {schema} TO {reader_role}")
                pg.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {reader_role}")
    project = tmp_path_factory.mktemp("pg") / "bank"
    shutil.copytree(bank, project, ignore=shutil.ignore_patterns("warehouse", "passports"))
    return project


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PASSPORT_DB_URL", READER or OWNER or "")


def test_postgres_matches_duckdb(
    pg_project: Path, bank: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    duck, _ = service.trace(bank, "total_rwa", out=tmp_path / "duck")
    _env(monkeypatch)
    pg, pack = service.trace(pg_project, "total_rwa", out=tmp_path / "pg")
    assert pg.dialect == "postgres"
    d_duck, d_pg = duck.to_dict(), pg.to_dict()
    assert d_pg["status"] == d_duck["status"] == "gaps_found"
    assert d_pg["report_hop"]["residual"] == "3100000.00"
    for hd, hp in zip(d_duck["hops"], d_pg["hops"], strict=False):
        assert hd["target"] == hp["target"]
        assert hd["target_value"] == hp["target_value"], hd["target"]
        assert [s["rows_removed"] for s in hd["steps"]] == [s["rows_removed"] for s in hp["steps"]]
        for sd, sp in zip(hd["steps"], hp["steps"], strict=False):
            assert abs(Decimal(sd["value_delta"]) - Decimal(sp["value_delta"])) <= Decimal("0.01")
    # QUALIFY does not exist in PostgreSQL: the engine transpiled it
    assert any("ROW_NUMBER" in q["sql"] and "QUALIFY" not in q["sql"].upper() for q in d_pg["audit"])
    assert service.verify(pack, pg_project)["ok"]


def test_postgres_session_is_read_only(pg_project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    c = PostgresConnector(OWNER or "")  # even the owner role is forced read-only by the connector
    assert c.probe_write() is False
    c.close()


def test_doctor_on_postgres(pg_project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from number_passport.doctor import run_doctor, summarize

    _env(monkeypatch)
    checks = run_doctor(pg_project)
    bad = [c for c in checks if c.status == "fail"]
    assert summarize(checks) == "pass", bad
    assert any(c.name == "replay silver.exposures" and "postgres" in c.detail for c in checks)
