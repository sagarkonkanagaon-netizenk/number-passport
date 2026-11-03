# Security and privacy model

Number Passport is built to be pointed at a production risk warehouse holding customer data. This page
describes what it can and cannot do, so your security, data protection and model risk teams can approve it.
To report a vulnerability, see [SECURITY.md](../SECURITY.md).

## Summary for reviewers

| Question | Answer |
|----------|--------|
| Can it change data? | No. Read-only is enforced at the connection and by the query guard. `passport doctor` fails when the login can write. |
| Can it read customer rows? | No. Every query must return aggregates only (totals and counts), and at most 50 rows. |
| What leaves the database? | Totals, counts and their SHA-256 hashes. Each one is logged with the query that produced it and why. |
| Does it call the internet? | No. The engine makes no network calls of its own; it only connects to the database URL you configure. |
| What does an AI assistant see? | Only through the MCP server: totals, counts, table and column names, code locations and findings. Never rows. |
| Where are credentials? | In environment variables or your secret store, referenced as `${VAR}` in `passport.yml`. |
| Is there telemetry? | No. |

## Threat model

| Threat | Control |
|--------|---------|
| A prompt-injected assistant asks for raw data | The guard rejects any query whose output is not aggregate-only, before it reaches the database. The MCP tools take metric names and filters; they have no free-form SQL tool. |
| A crafted `where` filter tries to write or read files | Each query is parsed (sqlglot) and must be one `SELECT`. DDL, DML, `COPY`, `ATTACH`, `PRAGMA`, `SET`, transactions and `SELECT ... INTO` are rejected, and so are table functions and file readers (`read_*`, `scan*`, `glob`, `pg_read*`, `lo_*`, `dblink`, `load_*`, `system`, `sleep` and others). Table references must be plain identifiers. |
| A write-capable login is used by mistake | DuckDB files are opened `read_only=True`. PostgreSQL sessions are `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`, and every query is rolled back. `passport doctor` probes for write access and fails if it works. |
| Small groups re-identify a customer | `GROUP BY` output is off by default. It is only allowed in `allow_dimensions` mode, on columns the data owner lists in `safe_dimensions`. |
| Evidence is edited after the fact | `MANIFEST.sha256` covers every pack file, and `passport verify` checks it. Code files and query results are hashed too, so code or data drift is detected. |
| Evidence describes code that no longer runs | `verify` re-hashes every pipeline file the trace read and reports any change. |
| Untrusted pipeline code is executed | Pipeline code is **parsed, never imported or executed**. PySpark jobs are read as a syntax tree and translated to SQL; DAG files are parsed the same way. SAS programs and stored procedures are read as text; nothing is submitted to SAS or the database. |
| A crafted Informatica export attacks the XML parser | Exports with entity declarations are refused before parsing, and the DTD reference is dropped, never fetched. |
| Catalog reads leak data | To follow views that exist only in the warehouse, the connector reads view definitions from the catalog (`duckdb_views()`, `pg_views`, `pg_matviews` or the SQLAlchemy inspector). These are metadata reads, like the column lists read from `information_schema`; no rows of any table are read. Turn them off with `connection.read_views: false`. |
| The MCP HTTP server is exposed | `passport mcp --http` binds to `127.0.0.1` by default and has no authentication of its own. Run it behind your authenticating gateway; never expose it directly. |

## The query guard, precisely

Every query passes `number_passport.guard.check_query` before it runs:

1. Exactly one statement, and it must be a query (`SELECT` or `WITH`). Anything sqlglot cannot classify is
   rejected.
2. In `aggregates_only` mode, every output column of the outermost query must be an aggregate, or
   arithmetic over aggregates and literals.
3. `GROUP BY` on the outermost query is only allowed in `allow_dimensions` mode, and only on listed
   `safe_dimensions`.
4. No forbidden expression appears anywhere in the tree, including subqueries and CTEs.
5. No blocked function name appears anywhere in the tree.
6. A result with more than 50 rows is rejected.

Then the query runs, and its purpose, SQL, timing and the SHA-256 of its result are appended to the audit
log that ships in the evidence pack. The test suite includes a catalogue of rejected queries
(`tests/test_guard.py`) and checks that the guard runs **before** the database is touched.

## Recommended deployment

1. Create a database role with `SELECT` on the reporting schemas only, and nothing else.
2. Store its URL in your secret manager, and expose it as `PASSPORT_DB_URL` to the job or the MCP server.
3. Run `passport doctor` and keep its output with your change ticket.
4. For assistants, run the MCP server over stdio on the analyst's machine, or over HTTP behind your gateway.
5. Store evidence packs in your normal document retention system; they contain no customer rows.

## Data protection notes

- Evidence packs contain table and column names, code excerpts, file paths, aggregates and hashes. Treat
  them as internal documents.
- `report.value`, metric labels and filters are copied into the pack as you write them; do not put
  personal data in them.
