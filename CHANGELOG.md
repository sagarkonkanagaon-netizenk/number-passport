# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Logo, social preview and detailed documentation: how it works, architecture, configuration reference,
  security model and FAQ.
- Governance files: NOTICE, SECURITY, CONTRIBUTING, Code of Conduct, SUPPORT, issue and pull request
  templates, CODEOWNERS and Dependabot.

## [0.1.0] - 2026-11-03

### Added

- Lineage from code: SQL, dbt (sources or manifest), PySpark (translated to SQL step by step) and Airflow.
- Per-hop reconciliation: table value vs code re-run, sequential step attribution with file:line, rows
  removed, inner-join loss in native units and join fan-out detection.
- Report hop: compares a published CSV cell or typed value with the warehouse.
- Data quality evidence: row counts, measure NULLs, duplicate keys and freshness.
- Privacy guard: aggregate-only queries, blocked functions and statements, 50-row cap and a hashed audit log.
- Read-only connectors: DuckDB, CSV/Parquet folders, PostgreSQL and SQLAlchemy.
- Evidence packs (HTML, JSON, replay SQL, SHA-256 manifest) and `passport verify`.
- `passport doctor`, the `passport` CLI, an MCP server and an agent skill.
- Sample bank pipeline and CI on Python 3.10 to 3.13 with real PostgreSQL and real Spark.

[Unreleased]: https://github.com/sagarkonkanagaon-netizenk/number-passport/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sagarkonkanagaon-netizenk/number-passport/releases/tag/v0.1.0
