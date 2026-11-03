# How Number Passport works

This page explains, step by step, what happens when you run `passport trace total_rwa`, and how to read
every number in the result. It uses the sample bank in [`examples/bank`](../examples/bank), so you can
reproduce each figure yourself.

- [1. The question a passport answers](#1-the-question-a-passport-answers)
- [2. Building the lineage graph from code](#2-building-the-lineage-graph-from-code)
- [3. Choosing the path for one number](#3-choosing-the-path-for-one-number)
- [4. Reconciling a hop](#4-reconciling-a-hop)
- [5. Step attribution, worked example](#5-step-attribution-worked-example)
- [6. Joins: lost rows and fan-out](#6-joins-lost-rows-and-fan-out)
- [7. The report hop](#7-the-report-hop)
- [8. Data quality evidence](#8-data-quality-evidence)
- [9. Statuses](#9-statuses)
- [10. Findings catalogue](#10-findings-catalogue)
- [11. The evidence pack and `verify`](#11-the-evidence-pack-and-verify)

## 1. The question a passport answers

A regulator, an auditor or your own CFO points at a number and asks three things:

1. **Where does it come from?** Which tables, which code, which scheduled job.
2. **Is it right?** Does every table still agree with the code that built it, and does the report agree
   with the warehouse?
3. **What happened to the money on the way?** What did each filter, join and dedup add or remove?

A passport answers all three for one number, with evidence that someone else can re-run.

## 2. Building the lineage graph from code

Number Passport never infers lineage from table names or a catalogue. It reads the code that builds the
tables:

| Code | What is parsed | Notes |
|------|----------------|-------|
| SQL files | `CREATE TABLE/VIEW ... AS`, `INSERT INTO ... SELECT` (column lists, several inserts into one table become `UNION ALL`), `INSERT OVERWRITE`, `SELECT ... INTO`, `MERGE`, and `DELETE ... WHERE` / `UPDATE ... SET` after a load (folded in as a filter and a `CASE WHEN`, so their effect is measured like any step) | Any dialect sqlglot supports; set `dialect:` in `passport.yml`. Statements are read one by one, so one unreadable statement is reported without hiding the rest of the file. |
| Stored procedures | Bodies of `CREATE PROCEDURE/FUNCTION` and anonymous blocks: SQL Server (with or without semicolons, `GO` batches), PostgreSQL PL/pgSQL (`$$` bodies, `DO`), Snowflake, BigQuery, Oracle PL/SQL | Temp and staging tables (`#tmp`, `CREATE TEMP TABLE`) are inlined into the statement that reads them. A statement under `IF` is noted as conditional. `SELECT ... INTO` a variable is ignored. Dynamic SQL and procedures written in JavaScript or Python are listed as parse issues. |
| Warehouse views | View definitions read from the catalog (`duckdb_views()`, `pg_views`/`pg_matviews`, the SQLAlchemy inspector) for tables no file builds | Only metadata is read. Each definition is fingerprinted; `verify` reports a changed view. `connection.read_views: false` turns it off. |
| dbt | Model files with `ref()`, `source()` and `config()`, or `target/manifest.json` | Jinja is blanked out so line numbers stay true. Other Jinja (macros, loops) marks the model `opaque`. With a manifest, the compiled SQL is used. |
| PySpark | `spark.table`, `spark.read.table`, `spark.sql`, then `filter/where`, `select`, `withColumn`, `withColumnRenamed`, `drop`, `join`, `groupBy().agg()`, `distinct`, `dropDuplicates`, and `saveAsTable` / `insertInto` | Each DataFrame line becomes one SQL CTE, so a step found later points back at the Python line. Column names are tracked through a schema read from the warehouse. |
| SAS | `PROC SQL`, DATA steps (`SET`, `MERGE ... BY` with `IN=`, `WHERE`, subsetting `IF`, `IF ... THEN DELETE`, assignments, `IF/ELSE` assignments, `KEEP`/`DROP`/`RENAME`), `PROC SORT` (`NODUPKEY`), `PROC MEANS/SUMMARY` (`NWAY`, `OUTPUT OUT=`), `PROC APPEND`, `%LET` | WORK datasets are inlined; librefs map to schemas. SAS operators, double-quoted strings, date literals, row-wise `sum()` and `round(x, 0.01)` are translated. Each step maps back to its `.sas` line. |
| Informatica PowerCenter | Repository XML exports: Source Qualifier (SQL override, source filter, user-defined join, distinct), Filter, Expression (variable ports), Aggregator, Joiner, connected Lookup, Router groups, Union, Sorter, Update Strategy | One CTE per transformation, mapped to its XML line. Ports flow through passive transformations as they do at run time. Expressions (`IIF`, `DECODE`, `ISNULL`, `SYSDATE`) are translated. |
| Airflow | DAG files | Tags each model with the DAG, task and schedule that run it. |

Each parsed model records its target table, upstream tables, source file and line, and the kind of code
that produced it. The result is one directed graph across every language. `passport doctor` checks this
graph for parse errors, duplicate producers of the same table and cycles.

Code that cannot be translated faithfully (a UDF, an RDD, dynamic SQL) still becomes a model, marked
**opaque**, with its upstream tables. The lineage continues through it; that hop is reported as not
reproducible instead of being skipped silently.

## 3. Choosing the path for one number

A metric is a table, a measure column, an aggregate (`sum` or `count`) and an optional SQL filter:

```yaml
total_rwa:
  table: gold.rwa_by_class
  column: rwa_usd
  agg: sum
```

Column-level lineage (via sqlglot) follows `rwa_usd` back through the model's SQL. Each upstream column
it finds is classified:

- **primary**: read through the model's `FROM` and carrying the traced value forward. One column per
  table is chosen as primary (the closest name match), so the trace follows money, not a rate.
- **factor**: read through a `JOIN`, such as an FX rate, a risk weight or a haircut. Factor tables become
  their own hops, so you can see their quality too, but they are not "the money".

The tracer walks the graph breadth-first from the metric table to tables that no model builds (the
sources).

## 4. Reconciling a hop

For every table on the path that has a model, Number Passport runs three aggregate queries:

| Value | Query | Meaning |
|-------|-------|---------|
| **target value** | `SUM(measure)` on the table as it is now | What reports read. |
| **recomputed value** | the model's own SQL, re-run now, aggregated | What the code produces today from today's inputs. |
| **relaxed value** | the model's SQL with every row-removing step turned off | The value before any filtering. |

If target and recomputed differ by more than the tolerance (`tolerance.absolute`, default 0.01), the hop is
a **gap** and you get a `table_code_mismatch` finding. The usual causes are:

- late-arriving source rows after the table was built;
- a manual change to the table;
- code that changed after the last load.

If they agree, the hop is **reconciled**: the table is exactly what its code says it should be.

All queries run in the warehouse's own dialect. The model SQL is transpiled where needed (for example, DuckDB
`QUALIFY` becomes a `ROW_NUMBER()` subquery on PostgreSQL), and every query passes the privacy guard before it
runs.

## 5. Step attribution, worked example

A row-removing step is any of:

- a `WHERE` condition (each `AND` part separately);
- an `INNER JOIN` (turned off by making it a `LEFT JOIN`);
- a dedup (`QUALIFY ROW_NUMBER() ... = 1`, `dropDuplicates`);
- `DISTINCT`;
- `HAVING`.

Number Passport turns all of them off, then turns them back on **one at a time, in code order**, measuring
the value after each. The difference each step makes is its **value delta**. The deltas add up to
`recomputed - relaxed`, so every unit of value is assigned to exactly one step, to the cent. For each step
it also counts the rows that step removed.

For `silver.exposures` in the sample bank, the relaxed value is 2,546,646,971.75 USD and the recomputed
value is 2,365,768,570.80 USD:

| Step | Where | Rows removed | Value delta (USD) |
|------|-------|-------------:|------------------:|
| dedup `ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY load_ts DESC) = 1` | `models/sql/silver_exposures.sql:8` | 30 | −34,697,141.71 |
| inner join `bronze.fx_rates` | `models/sql/silver_exposures.sql:18` | 15 | 0.00 (see §6) |
| filter `status <> 'CLOSED'` | `models/sql/silver_exposures.sql:20` | 120 | −153,180,711.57 |
| filter `amount > 0` | `models/sql/silver_exposures.sql:21` | 8 | +6,999,452.32 |

Two things stand out:

- **A filter can add value.** Removing loans with negative amounts raises the total. A report that only
  counted rows would miss this; the passport shows it in money.
- **The order is the code's order.** Attribution is sequential, so a row removed by two steps is assigned
  to the first one that removes it. That makes the deltas add up exactly and keeps the result
  deterministic.

If the deltas do not add up to the recomputed value (which can happen with unusual SQL), the hop gets an
`attribution_drift` warning and shows the difference.

## 6. Joins: lost rows and fan-out

**Rows lost to an inner join** often have no value in the output's units. In the sample, the 15 loans in
ZAR have no FX rate, so their USD value is NULL and the value delta is 0.00, even though real money was
dropped. Number Passport measures what those rows were worth in the driving table's own units:

> 15 row(s) had no match and were dropped: 18,635,283.00 in `SUM(l.amount)`

This is the classic missing-reference-data failure, and it is invisible in USD totals.

**Fan-out** is the opposite failure. If the right side of a join has duplicate keys (two EUR rates for the
same day, for example), each left row is counted twice and money is silently multiplied. For every join,
Number Passport checks whether the join keys are unique on the right side. A duplicate key is a CRITICAL
`join_fanout` finding naming the join, the keys and how many values repeat.

## 7. The report hop

When a metric has a `report:` cell (a CSV file, a row label and a column), that cell becomes hop 0. Its
value is compared with the metric table's total. In the sample, the Q3 capital report shows
1,088,769,797.07 but `gold.rwa_by_class` totals 1,085,669,797.07. The difference of 3,100,000.00 is a
CRITICAL `report_mismatch`: a manual adjustment or a report built from another source, which must be
documented or corrected.

## 8. Data quality evidence

BCBS 239 asks for accuracy, completeness and timeliness, not just lineage. For each table on the path, the
passport records:

| Check | Source | Finding if it fails |
|-------|--------|---------------------|
| Row count | always | `empty_table` (critical) |
| NULLs in the measure | always | `null_measure` (warning): `SUM` silently ignores them |
| Duplicate keys | `tables.<name>.key` in config, or the `PARTITION BY` of the model's dedup | `duplicate_keys` (warning) |
| Freshness | `freshness_column` and `max_age_hours`, compared with the metric's `as_of` | `stale_data` (warning) |

## 9. Statuses

| Status | Rule |
|--------|------|
| `verified` | No critical finding and every hop reconciled within tolerance. |
| `gaps_found` | Any critical finding, or any hop in status `gap` or `error`. |
| `partially_verified` | No gaps, but at least one hop could not be re-run (opaque code). |
| `lineage_only` | Run with `--offline`: lineage and code steps only, no values checked. |

`passport trace --strict` exits with code 2 when the status is `gaps_found`, so a CI job can block a
filing. A `partially_verified` result exits 0 but lists every hop that could not be checked.

## 10. Findings catalogue

| Code | Severity | Meaning |
|------|----------|---------|
| `report_mismatch` | critical | The report cell differs from the warehouse total. |
| `table_code_mismatch` | critical | A table differs from what its code produces today. |
| `join_fanout` | critical | A join's right side has duplicate keys; value may be multiplied. |
| `rerun_failed` | critical | The model's SQL failed when re-run in the warehouse. |
| `target_unreadable` / `source_unreadable` | critical | A table on the path cannot be read. |
| `report_unreadable` | critical | The report file, row or column cannot be found. |
| `empty_table` | critical | A table on the path has no rows. |
| `not_reproducible` | warning | The hop's code could not be re-run (opaque or untranslatable code); the hop is reported but not reconciled. |
| `attribution_drift` | warning | Step deltas do not add up to the recomputed value. |
| `null_measure` | warning | NULLs in the measure column. |
| `duplicate_keys` | warning | The table's key is not unique. |
| `stale_data` | warning | The latest load is older than the allowed age at the as-of time. |
| `opaque_code` | warning | Part of the code could not be translated; the hop is not reproducible. |
| `translation_note` | warning | A translation caveat (for example, Spark `dropDuplicates` row choice). |
| `lineage_incomplete` | warning | Column-level lineage could not be resolved for a column. |
| `trace_stops` | warning | No single upstream column carries the measure (for example, it is computed from several). |
| `fanout_unchecked` | warning | Join keys could not be checked for uniqueness. |
| `step_failed` | warning | One step's measurement query failed; other steps are still reported. |
| `quality_unavailable` | warning | Quality checks could not run on a table. |
| `trace_truncated` | warning | The trace hit the hop limit. |

## 11. The evidence pack and `verify`

Every online trace writes a folder:

| File | Contents |
|------|----------|
| `passport.html` | One self-contained page: status stamp, what needs attention, lineage diagram, a card per hop, quality checks, query audit and file fingerprints. No external scripts or fonts. |
| `passport.json` | The full machine-readable result. |
| `replay.sql` | Every query that ran, in order, with its purpose, so an auditor can re-run them by hand. |
| `MANIFEST.sha256` | SHA-256 of each file above. |

The JSON also records a SHA-256 of every pipeline code file that was read, and a SHA-256 of every query
result.

`passport verify <pack>` checks three things:

1. **Integrity:** every file matches `MANIFEST.sha256`, so nothing was edited after the trace.
2. **Code drift:** every pipeline file still has the same hash, so the code the passport describes is still the
   code that runs.
3. **Data drift:** every query still returns the same result hash (skipped with `--offline`).

A pack that verifies is the same evidence, on the same code and the same data, as on the day it was produced.
