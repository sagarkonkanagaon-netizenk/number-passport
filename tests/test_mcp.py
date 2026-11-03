"""The MCP server, exercised over the real stdio protocol, as Claude / Cursor would."""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("mcp")

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def _payload(result) -> dict | list:
    if getattr(result, "structured_content", None) is not None:
        sc = result.structured_content
        return sc.get("result", sc) if isinstance(sc, dict) and set(sc) == {"result"} else sc
    return json.loads(result.content[0].text)


async def _session_run(project: Path, out_dir: Path) -> dict:
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "number_passport", "mcp", "--project", str(project)]
    )
    results: dict = {}
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = await session.list_tools()
        results["tools"] = {t.name: t for t in tools.tools}
        results["doctor"] = _payload(await session.call_tool("passport_doctor", {}))
        results["metrics"] = _payload(await session.call_tool("list_metrics", {}))
        results["lineage"] = _payload(await session.call_tool("show_lineage", {"table": "silver.exposures"}))
        results["trace"] = _payload(await session.call_tool("trace_number", {"metric": "total_rwa"}))
        results["adhoc"] = _payload(
            await session.call_tool(
                "trace_number", {"table": "gold.rwa_by_class", "column": "exposure_count", "agg": "sum"}
            )
        )
        results["verify"] = _payload(
            await session.call_tool("verify_passport", {"pack_dir": results["trace"]["evidence_pack"]})
        )
        bad = await session.call_tool("trace_number", {"metric": "does_not_exist"})
        results["bad"] = bad
    return results


def test_mcp_end_to_end(bank: Path, tmp_path: Path) -> None:
    r = asyncio.run(_session_run(bank, tmp_path))
    assert set(r["tools"]) == {
        "passport_doctor",
        "list_metrics",
        "show_lineage",
        "trace_number",
        "verify_passport",
    }
    assert r["tools"]["show_lineage"].annotations.read_only_hint is True
    assert r["doctor"]["overall"] == "pass"
    assert {m["name"] for m in r["metrics"]["metrics"]} == {"total_rwa", "total_ead", "corporate_rwa"}
    assert r["lineage"]["built_by"]["file"] == "models/sql/silver_exposures.sql:3"
    assert len(r["lineage"]["built_by"]["row_removing_steps"]) == 4
    t = r["trace"]
    assert t["status"] == "gaps_found" and t["hops"][0]["unexplained"] == "3100000.00"
    assert Path(t["html"]).is_file()
    assert r["adhoc"]["status"] == "verified"
    assert r["verify"]["ok"] is True
    assert r["bad"].is_error is True
    assert "unknown metric" in r["bad"].content[0].text
