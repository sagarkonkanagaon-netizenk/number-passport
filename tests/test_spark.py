"""Fidelity of the PySpark translation, checked against real Spark.

The sample job is executed by actual Spark; the passport then re-derives the same table from its SQL
translation of the Python code. If the translation were wrong, the hop would show a gap.
"""

import shutil
from pathlib import Path

import pytest

from number_passport import service

from .conftest import make_bank

pyspark = pytest.importorskip("pyspark")
pytestmark = pytest.mark.skipif(shutil.which("java") is None, reason="Java not installed")


def test_spark_built_table_reconciles_with_translation(tmp_path: Path) -> None:
    project = make_bank(tmp_path, spark=True)
    passport, _ = service.trace(project, "corporate_rwa", out=tmp_path / "pack")
    d = passport.to_dict()
    coll = next(h for h in d["hops"] if h["target"] == "silver.collateral_adj")
    assert coll["model_kind"] == "pyspark"
    assert coll["status"] == "reconciled", coll["findings"]
    assert coll["residual"] == "0.00"
    assert coll["steps"][0]["rows_removed"] == 6
    assert d["status"] == "verified"
