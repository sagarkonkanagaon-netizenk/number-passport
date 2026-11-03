---
name: number-passport
description: Trace a number on a risk, finance or regulatory report back to its source rows through the real pipeline code (SQL, dbt, PySpark, Airflow), reconcile every hop with file:line explanations, and produce an auditor-ready evidence pack. Use when someone asks where a reported number comes from, why two numbers differ, whether a report figure can be trusted, or needs BCBS 239 / SOX lineage evidence.
---

# Number Passport

You help people prove where a reported number comes from. A "passport" is a chain of hops from the report
cell, through every table that feeds it, down to the raw source tables. At each hop the number is
reconciled: what the table holds, what the code would produce now, and exactly which code step (filter,
join, dedup) added or removed how much, with the file and line.

The engine does the work. You never compute reconciliations yourself, never guess lineage from table names,
and never read raw rows. Use the `number-passport` MCP tools (or the `passport` CLI if MCP is not
available).

## Tools

| Tool | Use it to |
|------|-----------|
| `passport_doctor` | Check every connection point before the first trace, and whenever a trace fails. |
| `list_metrics` | See the named numbers configured in `passport.yml`. |
| `show_lineage` | Answer "what builds this table?" from code only. No database access. |
| `trace_number` | Trace a metric (or table + column + agg + where) and write an evidence pack. |
| `verify_passport` | Re-check an existing evidence pack: integrity, code drift, and same query results. |

CLI equivalents: `passport doctor`, `passport metrics`, `passport lineage <table>`,
`passport trace <metric> [--offline] [--strict] [--json]`, `passport verify <pack>`.

## Method

1. **Find the project.** It is the folder with `passport.yml`. If there is none, run `passport init` and
   help the person fill in code paths, the read-only connection URL and one metric. Do not invent table or
   column names: ask, or read them from the pipeline code.
2. **Run `passport_doctor` once.** If anything fails, fix that first and explain the fix in one line. A
   failed read-only probe means the database login can write: stop and ask for a read-only login.
3. **Pick the number.** Prefer a configured metric. For an ad-hoc number, pass `table`, `column`, `agg`
   (`sum` or `count`) and an optional SQL `where`.
4. **Trace it** with `trace_number`. Use `offline=true` when there is no database access; that gives
   lineage only and the status says so.
5. **Report the result** in this order:
   - the status, in words (see below);
   - every CRITICAL finding, word for word from the digest, with its file:line;
   - the biggest step effects, in money terms, with file:line (for example "the CLOSED filter at
     models/sql/silver_exposures.sql:20 removes 120 loans worth 153,180,711.57");
   - the path to `passport.html` in the evidence pack.
6. **Before anyone relies on an old pack**, run `verify_passport` on it.

## Status words: be exact

| Status | What you may say |
|--------|------------------|
| `verified` | Every hop reconciles within tolerance and the report matches the table. |
| `gaps_found` | The number does **not** fully reconcile. Name each gap and its amount. Never call this verified. |
| `partially_verified` | Some hops could not be checked (opaque code, missing tables). Say which ones. |
| `lineage_only` | Offline: the code path is known but no value was checked. Say no numbers were reconciled. |

Rules that do not bend:

- Never round away a residual. A gap of 0.02 above tolerance is a gap.
- Never explain a gap the engine did not explain. If a residual is unexplained, say "unexplained" and list
  the likely causes the finding gives (late data, manual adjustment, code changed since the load).
- `table_code_mismatch` means the table no longer equals what its code produces now: the data changed after
  the load, or the code changed. Say both are possible unless the finding narrows it.
- Treat models marked `opaque` (Jinja macros, untranslatable PySpark) as unverified hops, and say so.
- Quote numbers as the engine formats them. Do not convert currencies.

## Privacy

Number Passport sends only totals and counts to you. The privacy guard rejects any query that would return
row-level data, writes, table functions or file reads, and every query is logged with a SHA-256 of its
result in the evidence pack. Do not try to work around the guard, do not ask the person to paste raw rows,
and do not run your own SQL against the warehouse to "double check". If someone needs a breakdown by a
dimension, the data owner must set `privacy.mode: allow_dimensions` and list that column (as
`schema.table.column`) under `privacy.safe_dimensions` in `passport.yml`.

## Install

```bash
pip install "number-passport[mcp]"          # add [postgres] or [sqlalchemy] for those warehouses
```

**Claude Code**

```bash
claude mcp add number-passport -- passport mcp --project /path/to/project
```

and copy this folder to `~/.claude/skills/number-passport/` (or `.claude/skills/` in the repo).

**Claude Desktop** (`claude_desktop_config.json`), **Cursor** (`.cursor/mcp.json`) and any MCP client:

```json
{
  "mcpServers": {
    "number-passport": {
      "command": "passport",
      "args": ["mcp", "--project", "/path/to/project"],
      "env": { "PASSPORT_DB_URL": "postgresql://reader@db.internal/risk" }
    }
  }
}
```

**Remote clients** (ChatGPT connectors, hosted agents): run `passport mcp --http --port 8765` behind your
own authenticating proxy and register the `/mcp` URL. The server has no authentication of its own; never
expose it directly.
