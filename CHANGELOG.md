# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-11-03

### Added

- Stored procedures: SQL Server (with or without semicolons, `GO` batches), PostgreSQL PL/pgSQL (`$$`
  bodies and `DO` blocks), Snowflake, BigQuery and Oracle PL/SQL. Statements under `IF` are noted as
  conditional; dynamic SQL and JavaScript or Python procedures are reported as parse issues.
- SQL scripts: `SELECT ... INTO`, `INSERT` column lists, several `INSERT`s into one table (`UNION ALL`),
  `INSERT OVERWRITE`, `WITH ... INSERT`, temp and staging tables inlined, `MERGE` (lineage through the
  `USING` source, marked not reproducible), and `DELETE ... WHERE` / `UPDATE ... SET` after a load folded in
  as a measured filter and a `CASE WHEN`.
- SQL files are parsed statement by statement: one unreadable statement is reported without hiding the rest.
- Views that exist only in the warehouse are read from the catalog, followed view over view, fingerprinted
  in the pack and re-checked by `passport verify` (`connection.read_views`, on by default).
- PySpark: `union`, `unionAll`, `unionByName` (with `allowMissingColumns`), `intersect`, `subtract` and
  `exceptAll`.
- SAS programs (`code.sas`): `PROC SQL`, DATA steps, `PROC SORT`, `PROC MEANS/SUMMARY`, `PROC APPEND` and
  `%LET`, with WORK datasets inlined and librefs mapped to schemas.
- Informatica PowerCenter mapping exports (`code.informatica`): Source Qualifier, Filter, Expression,
  Aggregator, Joiner, Lookup, Router, Union, Sorter and Update Strategy.
- Averages and ratios: `agg: avg` and `agg: ratio` (numerator and denominator, optional `scale`), traced as
  two additive parts with a sub-pack each, re-derived and checked against the report at its precision.
  `passport trace --agg avg` for ad hoc averages.
- Claude Code plugin (`/plugin marketplace add sagarkonkanagaon-netizenk/number-passport`), installing the
  MCP server and the skill together. The skill moved to `skills/number-passport/SKILL.md`.

### Changed

- Requires sqlglot 30 or later (the procedural statement types it parses).

### Fixed

- Column lineage no longer fails when the warehouse schema mixes one- and two-part table names.
- `WITH ... INSERT` in T-SQL kept its CTEs out of the query; they now travel with it.

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
- Animated Trace N logo, animated README banner, social preview and detailed documentation: how it
  works, architecture, configuration reference, security model and FAQ.
- Governance files: NOTICE, SECURITY, CONTRIBUTING, Code of Conduct, SUPPORT, issue and pull request
  templates, CODEOWNERS and Dependabot.
- Release pipeline: tagging `vX.Y.Z` publishes to PyPI (trusted publishing) and creates the GitHub release.

[Unreleased]: https://github.com/sagarkonkanagaon-netizenk/number-passport/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/sagarkonkanagaon-netizenk/number-passport/releases/tag/v0.2.0
[0.1.0]: https://github.com/sagarkonkanagaon-netizenk/number-passport/releases/tag/v0.1.0
