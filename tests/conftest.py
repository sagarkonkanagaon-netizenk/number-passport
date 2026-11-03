from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "bank"


def _load_build():
    spec = importlib.util.spec_from_file_location("bank_build", EXAMPLE / "build.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bank_build"] = mod
    spec.loader.exec_module(mod)
    return mod


build_mod = _load_build()


def make_bank(tmp: Path, late_data: bool = False, spark: bool = False) -> Path:
    """Copy the sample bank project into tmp and build its warehouse there."""
    dest = tmp / "bank"
    shutil.copytree(EXAMPLE, dest, ignore=shutil.ignore_patterns("warehouse", "passports", "__pycache__"))
    (dest / "warehouse").mkdir()
    # build.py writes its report next to itself; point it at the copy
    build_mod.HERE = dest
    try:
        build_mod.build(dest / "warehouse" / "bank.duckdb", late_data=late_data, spark=spark)
    finally:
        build_mod.HERE = EXAMPLE
    return dest


@pytest.fixture(scope="session")
def bank(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Session-wide clean sample bank (no Spark, for speed)."""
    return make_bank(tmp_path_factory.mktemp("clean"))


@pytest.fixture()
def fresh_bank(tmp_path: Path) -> Path:
    """A private copy a test may modify."""
    return make_bank(tmp_path)


@pytest.fixture(autouse=True)
def _no_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PASSPORT_DB_URL", raising=False)


def pg_url() -> str | None:
    return os.environ.get("NP_TEST_POSTGRES_URL")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):  # type: ignore[no-untyped-def]
    """In CI, NP_TEST_NO_SKIP=1 turns skips into failures, so a missing Postgres, Java or Spark can never
    make the integration tests pass by not running."""
    outcome = yield
    rep = outcome.get_result()
    if os.environ.get("NP_TEST_NO_SKIP") == "1" and rep.skipped and not hasattr(rep, "wasxfail"):
        rep.outcome = "failed"
        rep.longrepr = f"skipped while NP_TEST_NO_SKIP=1: {rep.longrepr}"
