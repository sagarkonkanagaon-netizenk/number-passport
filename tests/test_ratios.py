"""Averages and ratios: traced as two additive parts, then re-derived."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
import yaml

from number_passport.cli import main
from number_passport.config import ConfigError, load_config, parse_config
from number_passport.connectors import connect
from number_passport.evidence import verify_pack, write_pack
from number_passport.model import Metric
from number_passport.trace import CompositePassport, Tracer


def _totals(bank: Path) -> tuple[Decimal, Decimal, int]:
    with duckdb.connect(str(bank / "warehouse" / "bank.duckdb"), read_only=True) as con:
        rwa, ead = con.execute("SELECT SUM(rwa_usd), SUM(ead_usd) FROM gold.rwa_by_class").fetchone()
        n = con.execute("SELECT COUNT(ead_usd) FROM gold.rwa_by_class").fetchone()[0]
    return Decimal(str(rwa)), Decimal(str(ead)), n


def _add_metrics(project: Path, extra: dict) -> Path:
    cfg = project / "passport.yml"
    raw = yaml.safe_load(cfg.read_text())
    raw["metrics"].update(extra)
    cfg.write_text(yaml.safe_dump(raw))
    return project


def test_metric_validation() -> None:
    avg = Metric("a", "t", "c", "avg", where="x = 1")
    assert avg.numerator and avg.numerator.agg == "sum"
    assert avg.denominator and avg.denominator.where == "(x = 1) AND c IS NOT NULL"
    with pytest.raises(ValueError):
        Metric("r", "t", "c", "ratio")
    with pytest.raises(ValueError):
        Metric("m", "t", "c", "median")


def test_ratio_config_parsing(tmp_path: Path) -> None:
    raw = {
        "metrics": {
            "rwa": {"table": "g.t", "column": "rwa"},
            "density": {
                "agg": "ratio",
                "numerator": "rwa",
                "denominator": {"table": "g.t", "column": "ead"},
                "scale": "percent",
            },
        }
    }
    cfg = parse_config(raw, tmp_path)
    d = cfg.metrics["density"]
    assert d.composite and d.scale == 100 and d.numerator is cfg.metrics["rwa"]
    assert d.denominator is not None and d.denominator.column == "ead"
    with pytest.raises(ConfigError):
        parse_config(
            {"metrics": {"r": {"agg": "ratio", "numerator": "missing", "denominator": "x"}}}, tmp_path
        )


def test_ratio_and_average_trace_and_verify(fresh_bank: Path, tmp_path: Path) -> None:
    rwa, ead, n = _totals(fresh_bank)
    density = rwa / ead * 100
    shown = density.quantize(Decimal("0.01"))
    _add_metrics(
        fresh_bank,
        {
            "rwa_density": {
                "label": "RWA density (%)",
                "agg": "ratio",
                "numerator": {"table": "gold.rwa_by_class", "column": "rwa_usd"},
                "denominator": "total_ead",
                "scale": "percent",
                "report": {"value": f"{shown}%"},
            },
            "avg_ead": {"table": "gold.rwa_by_class", "column": "ead_usd", "agg": "avg"},
        },
    )
    cfg = load_config(fresh_bank)
    conn = connect(cfg.connection_url)
    try:
        cp = Tracer(cfg, conn).run(cfg.metrics["rwa_density"])
        avg = Tracer(cfg, conn).run(cfg.metrics["avg_ead"])
    finally:
        conn.close()
    assert isinstance(cp, CompositePassport) and isinstance(avg, CompositePassport)
    assert cp.value is not None and abs(cp.value - density) < Decimal("1e-9")
    assert cp.report_hop is not None and cp.report_hop.status == "reconciled"
    assert set(cp.parts) == {"numerator", "denominator"}
    # each part has its own audit log, not the other part's queries
    assert cp.parts["numerator"].audit and cp.parts["numerator"].audit[0]["id"] == 1
    assert avg.value is not None and abs(avg.value - ead / n) < Decimal("0.01")  # parts are totals in cents

    pack = write_pack(cp, tmp_path / "pack")
    data = json.loads((pack / "passport.json").read_text())
    assert data["composite"] and data["parts"]["denominator"]["metric"] == "total_ead"
    assert (pack / "numerator" / "passport.html").is_file()
    assert "numerator/passport.html" in (pack / "passport.html").read_text()
    conn = connect(cfg.connection_url)
    try:
        res = verify_pack(pack, cfg.root, conn)
    finally:
        conn.close()
    assert res.ok and res.queries_checked > 0, res
    (pack / "denominator" / "passport.json").write_text("{}")
    assert not verify_pack(pack, None, None).ok


def test_ratio_report_mismatch_is_a_gap(
    fresh_bank: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _add_metrics(
        fresh_bank,
        {
            "rwa_density": {
                "agg": "ratio",
                "numerator": {"table": "gold.rwa_by_class", "column": "rwa_usd"},
                "denominator": {"table": "gold.rwa_by_class", "column": "ead_usd"},
                "scale": 100,
                "report": {"value": "1.5%"},
            }
        },
    )
    out = tmp_path / "p"
    assert main(["trace", "rwa_density", "-p", str(fresh_bank), "--out", str(out), "--strict"]) == 2
    text = capsys.readouterr().out
    assert "numerator" in text and "report 1.5" in text and "gap" in text
    assert (
        main(
            [
                "trace",
                "--table",
                "gold.rwa_by_class",
                "--column",
                "ead_usd",
                "--agg",
                "avg",
                "-p",
                str(fresh_bank),
                "--out",
                str(tmp_path / "a"),
                "--json",
            ]
        )
        == 0
    )
    digest = json.loads(capsys.readouterr().out)
    assert digest["parts"]["denominator"]["metric"]["agg"] == "count"
