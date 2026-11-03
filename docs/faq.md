# FAQ

## Is this a data lineage tool like OpenLineage, Collibra or dbt docs?

No. Those tools tell you which tables feed which. Number Passport takes one number and proves it: it re-runs
the code, reconciles every hop, explains every difference in money with file:line, and seals the result as
evidence. It can sit next to a lineage catalogue.

## Is it a data diff tool?

Data diff compares two versions of a table. Number Passport explains one value along the whole path from
source to report, including what each filter and join did, and whether the report matches the warehouse.

## Do I need to connect my database?

No. Start with code only (`passport trace --offline`) to get lineage with file:line. Add a read-only login,
or CSV/Parquet extracts, to reconcile values. See the
[three connection levels](../README.md#connecting-to-your-data-three-levels).

## Does my data go to an AI?

No rows ever leave the database. Only totals and counts are read, and only if you use the MCP server does an
assistant see them. See [security.md](security.md).

## Which regulations does it help with?

It was designed around BCBS 239 (risk data aggregation and reporting: accuracy, completeness, timeliness,
and the ability to reconcile reports to source). The same evidence supports SOX controls over financial
reporting, IFRS 9 and CECL provisioning reviews, Pillar 3 and COREP/FINREP submissions, and internal model
validation. It does not replace your control framework; it gives it evidence.

## What if our pipelines use stored procedures, Informatica or SAS?

All three are read directly (since 0.2.0):

- **Stored procedures** in `.sql` files: SQL Server (`GO` batches, no semicolons needed), PostgreSQL
  (PL/pgSQL `$$` bodies and `DO` blocks), Snowflake, BigQuery and Oracle (PL/SQL). The statements inside the
  body are read like any script; temp tables are inlined, `IF` branches are noted, and dynamic SQL is listed
  as a parse issue.
- **SAS** programs (`code.sas`): `PROC SQL`, DATA steps, `PROC SORT`, `PROC MEANS/SUMMARY`, `PROC APPEND`,
  `%LET` variables. WORK datasets are inlined; librefs map to warehouse schemas.
- **Informatica PowerCenter** (`code.informatica`): export the folder or workflow as XML from the Repository
  Manager. Each target in each mapping becomes a hop, with one step per transformation and the XML line.

Views that exist only in the database are read from its catalog, so a trace no longer stops at them.

## What if part of a PySpark job can't be translated?

That model is marked opaque. The trace continues through its upstream tables, and the hop is reported as
`not_reproducible` with the reason. The overall status becomes `partially_verified`, not `verified`.

## Can it handle averages or ratios?

Yes. Use `agg: avg`, or `agg: ratio` with a `numerator` and a `denominator` (and `scale: percent` for a
percentage). An average or a ratio cannot be reconciled hop by hop, because it does not add up across rows,
so each part is traced and reconciled on its own, with its own evidence, and the figure is re-derived from
the two reconciled totals. A report figure is checked at the precision it is printed with: 13.2% matches
13.2449%.

## How long does a trace take?

The sample trace runs 51 aggregate queries and takes a few seconds on DuckDB. On a large warehouse, time is
dominated by re-running each model once per row-removing step; every query is an aggregate, so no data is
moved.

## Can I run it in CI before a filing?

Yes. `passport trace <metric> --strict` exits with code 2 when gaps are found. Keep the evidence pack as a
build artifact.

## Is it free to use commercially?

Yes. Apache License 2.0: use, modify and distribute, including commercially, with the license and notice
kept. It includes an express patent grant.
