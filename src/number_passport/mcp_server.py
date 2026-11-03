"""MCP server: the same capabilities as the CLI, for Claude, ChatGPT, Cursor and any MCP client.

passport mcp --project /path/to/repo      # stdio (Claude Code, Claude Desktop, Cursor)
passport mcp --http --port 8765          # streamable HTTP (remote connectors)
"""

from __future__ import annotations

import os
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from . import __version__, service
from .config import ConfigError
from .connectors import ConnectorError
from .doctor import run_doctor, summarize
from .guard import GuardViolation
from .trace import TraceError

F = TypeVar("F", bound=Callable[..., Any])


def _clean_errors(fn: F) -> F:
    """Known problems (bad config, unreachable database, unknown metric) become a clear tool error."""

    @wraps(fn)
    def inner(*a: Any, **kw: Any) -> Any:
        try:
            return fn(*a, **kw)
        except (ConfigError, ConnectorError, TraceError, GuardViolation, FileNotFoundError) as e:
            raise ToolError(str(e)) from e

    return inner  # type: ignore[return-value]


INSTRUCTIONS = """Number Passport traces a number on a risk or regulatory report back to source rows.
Use trace_number when someone asks where a figure comes from, why two figures differ, or for audit evidence.
Report the status honestly: only say a number is verified when status is "verified". Quote file:line for
every step you mention. Unexplained differences must be called out, never smoothed over. Only totals and
counts leave the database; never ask for raw rows."""

READ = ToolAnnotations(
    read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False
)
WRITES_PACK = ToolAnnotations(
    read_only_hint=False, destructive_hint=False, idempotent_hint=False, open_world_hint=False
)


def build_server(default_project: str | None = None) -> MCPServer:
    project_default = default_project or os.environ.get("NUMBER_PASSPORT_PROJECT") or os.getcwd()
    srv: MCPServer = MCPServer("number-passport", version=__version__, instructions=INSTRUCTIONS)

    def proj(p: str | None) -> str:
        return p or project_default

    @srv.tool(annotations=READ)
    @_clean_errors
    def passport_doctor(project_dir: str | None = None) -> dict[str, Any]:
        """Check every connection point: config, code paths, parsing, database, read-only access, privacy
        guard, model replay in the warehouse dialect, metrics and report cells. Run this first."""
        checks = run_doctor(proj(project_dir))
        return {"overall": summarize(checks), "checks": [c.to_dict() for c in checks]}

    @srv.tool(annotations=READ)
    @_clean_errors
    def list_metrics(project_dir: str | None = None) -> dict[str, Any]:
        """Metrics configured in passport.yml (named numbers people ask about)."""
        return {"metrics": service.metrics(proj(project_dir))}

    @srv.tool(annotations=READ)
    @_clean_errors
    def show_lineage(table: str, project_dir: str | None = None) -> dict[str, Any]:
        """Code-only lineage for one table: what builds it (file:line, DAG task), its row-removing steps,
        upstream and downstream tables. Never touches the database."""
        return service.lineage(proj(project_dir), table)

    @srv.tool(annotations=WRITES_PACK)
    @_clean_errors
    def trace_number(
        metric: str | None = None,
        table: str | None = None,
        column: str | None = None,
        agg: str = "sum",
        where: str | None = None,
        project_dir: str | None = None,
        offline: bool = False,
    ) -> dict[str, Any]:
        """Trace a number to its source rows and reconcile every hop. Give a configured metric name, or a
        table + column (+ optional agg sum|count|avg and SQL where). Averages and configured ratios are
        traced as their two additive parts. Writes an evidence pack (HTML, JSON, replay.sql, manifest)
        and returns a digest with status, per-hop values, steps and findings."""
        passport, pack = service.trace(proj(project_dir), metric, table, column, agg, where, offline=offline)
        return service.summary(passport, pack)

    @srv.tool(annotations=READ)
    @_clean_errors
    def verify_passport(
        pack_dir: str, project_dir: str | None = None, offline: bool = False
    ) -> dict[str, Any]:
        """Re-check an evidence pack: file integrity, whether the code changed since, and (online) whether
        every query still returns the same result."""
        return service.verify(pack_dir, proj(project_dir), offline=offline)

    return srv


def main(project: str | None = None, http: bool = False, host: str = "127.0.0.1", port: int = 8765) -> None:
    srv = build_server(project)
    if http:
        srv.run("streamable-http", host=host, port=port)
    else:
        srv.run("stdio")
