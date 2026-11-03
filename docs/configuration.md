# Configuration reference

Number Passport reads one file, `passport.yml`, from the project folder (or the nearest parent folder).
`passport init` writes a starter file, and `passport doctor` validates every setting against the real code and
database. Unknown top-level keys are rejected, so a typo never silently disables a check.

A complete working example is [`examples/bank/passport.yml`](../examples/bank/passport.yml).

## Full schema

```yaml
project: sample-bank-capital     # display name (default: folder name)
dialect: duckdb                  # SQL dialect the pipeline code is written in (any sqlglot dialect)

code:                            # every path is relative to this file; each accepts one path or a list
  sql: models/sql                # CREATE TABLE/VIEW AS and INSERT INTO ... SELECT files
  dbt:
    models: models/dbt           # dbt model sources (ref/source/config)...
    manifest: target/manifest.json   # ...or the compiled manifest (preferred when you run `dbt compile`)
    schema_map:                  # model folder -> warehouse schema, when they differ
      marts: gold
  pyspark: jobs                  # PySpark job files
  airflow: dags                  # Airflow DAG files
  sas:                           # SAS programs (.sas)
    paths: sas
    libnames:                    # SAS libref -> warehouse schema (LIBNAME ... SCHEMA= is read too)
      dw: gold
  informatica:                   # PowerCenter repository XML exports (.xml)
    paths: informatica
    schemas:                     # source/target owner or DBD name -> warehouse schema
      RAW_DB: raw

connection:
  url: ${PASSPORT_DB_URL:-duckdb:///warehouse/bank.duckdb}
  read_views: true               # read definitions of views that exist only in the warehouse (default)

privacy:
  mode: aggregates_only          # or allow_dimensions
  safe_dimensions:               # only used in allow_dimensions mode
    - gold.rwa_by_class.asset_class

tolerance:
  absolute: 0.01                 # a hop reconciles when |a - b| <= absolute + relative * |b|
  relative: 0

as_of: "2026-09-30T08:00:00+00:00"   # reporting time; freshness is judged against it (default: now)

output_dir: passports            # where evidence packs are written

tables:                          # optional per-table quality settings
  bronze.loans:
    key: [loan_id, load_ts]      # grain; duplicates are reported
    freshness_column: load_ts
    max_age_hours: 26

metrics:
  total_rwa:
    label: Total risk-weighted assets (USD)
    table: gold.rwa_by_class
    column: rwa_usd
    agg: sum                     # sum | count | avg | ratio
    where: asset_class = 'CORPORATE'   # optional SQL filter on the metric table
    report:                      # optional: the published figure to check
      file: reports/q3_capital_report.csv
      row: Total RWA             # value in the first column that labels the row
      column: value_usd          # header of the column holding the figure
      # or, when the report is not a CSV:
      # value: 1088769797.07
      # source: "Q3 Pillar 3 disclosure, table OV1, row 29"
  rwa_density:
    label: RWA density (%)
    agg: ratio
    numerator: total_rwa         # a metric name...
    denominator:                 # ...or table + column (+ agg, where)
      table: gold.rwa_by_class
      column: ead_usd
    scale: percent               # multiply by 100
    report: { value: "45.89%" }
```

## Connection URLs

| URL | Driver | Read-only enforcement |
|-----|--------|-----------------------|
| `duckdb:///path/to/file.duckdb` | built in | File opened with `read_only=True`. |
| `files:///path/to/exports` | built in | CSV and Parquet files are loaded into an in-memory DuckDB; `exports/<schema>/<table>.csv` or `.parquet` becomes `schema.table`. The files are never modified. |
| `postgresql://user@host:5432/db` | `pip install "number-passport[postgres]"` | Session set to `READ ONLY`, rollback after every query. |
| any SQLAlchemy URL (`snowflake://`, `databricks://`, `mssql+pyodbc://`, ...) | `pip install "number-passport[sqlalchemy]"` plus the dialect's driver | Uses the database role you provide; use a role with `SELECT` only. |

Relative `duckdb:///` and `files:///` paths are resolved against the folder holding `passport.yml`.

**Never put credentials in `passport.yml`.** Use `${VAR}` or `${VAR:-default}`, which are expanded from the
environment, and give the variable from your secret store.

`passport doctor` tries a harmless write with your credentials and **fails** if the write succeeds. A trace
can run with a write-capable login because every query is guarded, but the doctor tells you to replace it.

## Metrics

| Key | Required | Meaning |
|-----|----------|---------|
| `table` | yes (not for `ratio`) | Table the number is aggregated from. |
| `column` | yes (not for `ratio`) | Measure column. |
| `agg` | no | `sum` (default), `count`, `avg` (traced as sum and count of non-null values) or `ratio`. |
| `numerator`, `denominator` | for `ratio` | A configured metric name, or `table`, `column`, optional `agg` (sum or count) and `where`. |
| `scale` | no | Multiplier for the derived figure, e.g. `100` or `percent`. |
| `where` | no | SQL filter, in the warehouse dialect. |
| `label` | no | Human name shown in reports. |
| `report.file`, `report.row`, `report.column` | no | A CSV cell holding the published figure. Thousands separators are accepted. |
| `report.value`, `report.source` | no | The published figure typed in, with a citation of where it appears. |

For `avg` and `ratio`, the report figure is compared at the precision it is printed with, and `%` signs are
accepted. The evidence pack holds one sub-pack per part (`numerator/`, `denominator/`) and a cover page.

Ad hoc traces need no metric: `passport trace --table gold.rwa_by_class --column rwa_usd --where "..."`.

## Tables

| Key | Meaning |
|-----|---------|
| `key` | Columns that should be unique. When omitted, the `PARTITION BY` of the model's dedup is used. |
| `freshness_column` | Timestamp column for the latest load. |
| `max_age_hours` | Maximum age at the `as_of` time; older data is a `stale_data` finding. |

## Privacy modes

| Mode | Allowed queries |
|------|-----------------|
| `aggregates_only` (default) | Aggregate outputs only: totals and counts. No `GROUP BY` output. |
| `allow_dimensions` | Also `GROUP BY` on columns listed in `safe_dimensions` (as `schema.table.column`), for breakdowns that the data owner has cleared. |

See [security.md](security.md) for the full guard rules.

## Environment variables

| Variable | Used by | Meaning |
|----------|---------|---------|
| `PASSPORT_DB_URL` | the sample config | Connection URL (any name works with `${...}` expansion). |
| `NUMBER_PASSPORT_PROJECT` | `passport mcp` | Default project folder for MCP tools. |
| `NP_TEST_POSTGRES_URL`, `NP_TEST_POSTGRES_READER_URL` | test suite | Enable the PostgreSQL integration tests. |
| `NP_TEST_NO_SKIP=1` | test suite | Turn any skipped test into a failure (used in CI). |
