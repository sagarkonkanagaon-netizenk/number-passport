"""Read-only database connectors.

Supported URLs:

* ``duckdb:///path/to/warehouse.duckdb``: opened with ``read_only=True``.
* ``files:///path/to/exports``: CSV or Parquet extracts laid out as ``<schema>/<table>.(csv|parquet)``,
  loaded into an in-memory DuckDB as views. For teams that cannot grant any database access.
* ``postgresql://user@host/db``: psycopg 3, session forced to ``default_transaction_read_only``.
* anything SQLAlchemy understands (``snowflake://``, ``databricks://``, ``bigquery://``, ``mssql+pyodbc://``):
  generic path, executed inside a transaction that is always rolled back.

Every connector exposes the same small surface the engine needs: run a query, list columns, ping, and
prove that it cannot write.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


class ConnectorError(Exception):
    pass


class Connector(ABC):
    dialect: str
    url_display: str

    @abstractmethod
    def query(self, sql: str) -> list[tuple[Any, ...]]: ...

    @abstractmethod
    def columns(self, table: str) -> list[str]: ...

    @abstractmethod
    def probe_write(self) -> bool:
        """Try a harmless write. Return True if the database accepted it (which is bad)."""

    def ping(self) -> None:
        self.query("SELECT 1")

    def close(self) -> None:  # noqa: B027 - optional hook
        pass

    def schema(self, tables: list[str]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for t in tables:
            try:
                cols = self.columns(t)
            except ConnectorError:
                continue
            if cols:
                out[t] = cols
        return out


def _split(table: str) -> tuple[str | None, str]:
    parts = table.split(".")
    return (parts[-2] if len(parts) > 1 else None), parts[-1]


_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DuckDBConnector(Connector):
    dialect = "duckdb"

    def __init__(self, path: str) -> None:
        import duckdb

        self.url_display = f"duckdb:///{path}"
        if path != ":memory:" and not Path(path).is_file():
            raise ConnectorError(f"DuckDB file not found: {path}")
        try:
            self.con = duckdb.connect(path, read_only=path != ":memory:")
        except duckdb.Error as e:
            raise ConnectorError(f"cannot open DuckDB {path}: {e}") from e

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        import duckdb

        try:
            return list(self.con.execute(sql).fetchall())
        except duckdb.Error as e:
            raise ConnectorError(str(e).splitlines()[0]) from e

    def columns(self, table: str) -> list[str]:
        schema, name = _split(table)
        sql = "SELECT column_name FROM information_schema.columns WHERE lower(table_name) = ?"
        params: list[Any] = [name.lower()]
        if schema:
            sql += " AND lower(table_schema) = ?"
            params.append(schema.lower())
        sql += " ORDER BY ordinal_position"
        return [r[0].lower() for r in self.con.execute(sql, params).fetchall()]

    def probe_write(self) -> bool:
        import duckdb

        try:
            self.con.execute("CREATE TABLE __number_passport_probe (x INTEGER)")
        except duckdb.Error:
            return False
        self.con.execute("DROP TABLE IF EXISTS __number_passport_probe")
        return True

    def close(self) -> None:
        self.con.close()


class FilesConnector(DuckDBConnector):
    """CSV / Parquet extracts, one folder per schema."""

    def __init__(self, folder: str) -> None:
        import duckdb

        base = Path(folder)
        if not base.is_dir():
            raise ConnectorError(f"exports folder not found: {folder}")
        self.url_display = f"files:///{folder}"
        self.con = duckdb.connect(":memory:")
        loaded = 0
        for f in sorted(base.rglob("*")):
            if f.suffix.lower() not in {".csv", ".parquet"}:
                continue
            rel = f.relative_to(base)
            schema = rel.parts[0] if len(rel.parts) > 1 else "main"
            name = f.stem
            if not (_SAFE_IDENT.match(schema) and _SAFE_IDENT.match(name)):
                raise ConnectorError(f"unsupported export file name {rel}")
            reader = "read_parquet" if f.suffix.lower() == ".parquet" else "read_csv_auto"
            path_lit = str(f).replace("'", "''")
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            self.con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{name} AS SELECT * FROM {reader}('{path_lit}')"
            )
            loaded += 1
        if not loaded:
            raise ConnectorError(f"no .csv or .parquet files under {folder}")
        # Loading is done: from here on the session behaves like a read-only warehouse.
        self._sealed = True

    def probe_write(self) -> bool:
        # The in-memory copy is private to this process; writes cannot reach the source files.
        return False


class PostgresConnector(Connector):
    dialect = "postgres"

    def __init__(self, url: str) -> None:
        try:
            import psycopg
        except ImportError as e:
            raise ConnectorError("PostgreSQL support needs `pip install number-passport[postgres]`") from e
        p = urlparse(url)
        self.url_display = (
            f"postgresql://{p.username or ''}@{p.hostname or ''}{':' + str(p.port) if p.port else ''}{p.path}"
        )
        try:
            self.con = psycopg.connect(
                url.replace("postgresql+psycopg://", "postgresql://"), autocommit=False
            )
            with self.con.cursor() as cur:
                cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            self.con.commit()
        except psycopg.Error as e:
            raise ConnectorError(f"cannot connect to PostgreSQL: {str(e).strip()}") from e

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        import psycopg

        try:
            with self.con.cursor() as cur:
                cur.execute(sql)  # SQL passed the guard
                rows = cur.fetchall()
            return [tuple(r) for r in rows]
        except psycopg.Error as e:
            raise ConnectorError(str(e).strip().splitlines()[0]) from e
        finally:
            self.con.rollback()

    def columns(self, table: str) -> list[str]:
        schema, name = _split(table)
        sql = "SELECT column_name FROM information_schema.columns WHERE lower(table_name) = %s"
        params: list[Any] = [name.lower()]
        if schema:
            sql += " AND lower(table_schema) = %s"
            params.append(schema.lower())
        sql += " ORDER BY ordinal_position"
        try:
            with self.con.cursor() as cur:
                cur.execute(sql, params)
                return [r[0].lower() for r in cur.fetchall()]
        finally:
            self.con.rollback()

    def probe_write(self) -> bool:
        import psycopg

        try:
            with self.con.cursor() as cur:
                cur.execute("CREATE TEMP TABLE __number_passport_probe (x integer)")
            return True
        except psycopg.Error:
            return False
        finally:
            self.con.rollback()

    def close(self) -> None:
        self.con.close()


_SQLA_DIALECT = {
    "snowflake": "snowflake",
    "databricks": "databricks",
    "bigquery": "bigquery",
    "mssql": "tsql",
    "oracle": "oracle",
    "mysql": "mysql",
    "redshift": "redshift",
    "trino": "trino",
    "postgresql": "postgres",
    "sqlite": "sqlite",
}


class SQLAlchemyConnector(Connector):
    def __init__(self, url: str) -> None:
        try:
            import sqlalchemy as sa
        except ImportError as e:
            raise ConnectorError(
                "this URL needs `pip install number-passport[sqlalchemy]` plus the "
                "database's SQLAlchemy driver"
            ) from e
        self.sa = sa
        try:
            self.engine = sa.create_engine(url)
        except Exception as e:  # driver import errors surface here
            raise ConnectorError(f"cannot create engine: {e}") from e
        self.dialect = _SQLA_DIALECT.get(self.engine.dialect.name, self.engine.dialect.name)
        self.url_display = self.engine.url.render_as_string(hide_password=True)

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        try:
            with self.engine.connect() as c:
                rows = c.execute(self.sa.text(sql)).fetchall()
                c.rollback()
            return [tuple(r) for r in rows]
        except Exception as e:
            raise ConnectorError(str(e).splitlines()[0]) from e

    def columns(self, table: str) -> list[str]:
        schema, name = _split(table)
        insp = self.sa.inspect(self.engine)
        try:
            return [c["name"].lower() for c in insp.get_columns(name, schema=schema)]
        except Exception as e:
            raise ConnectorError(str(e)) from e

    def probe_write(self) -> bool:
        try:
            with self.engine.connect() as c:
                c.execute(self.sa.text("CREATE TEMPORARY TABLE __number_passport_probe (x integer)"))
                c.rollback()
            return True
        except Exception:
            return False


def connect(url: str) -> Connector:
    if url.startswith("duckdb:///"):
        return DuckDBConnector(url[len("duckdb:///") :])
    if url.startswith("files:///"):
        return FilesConnector(url[len("files:///") :])
    if url.startswith(("postgresql://", "postgres://", "postgresql+psycopg://")):
        return PostgresConnector(url.replace("postgres://", "postgresql://", 1))
    if "://" in url:
        return SQLAlchemyConnector(url)
    raise ConnectorError(f"unrecognised connection URL {url!r}")
