import pytest

from number_passport.guard import Guard, GuardViolation, check_query

UNSAFE = [
    "DROP TABLE gold.x",
    "DELETE FROM bronze.loans",
    "UPDATE bronze.loans SET amount = 0",
    "INSERT INTO t SELECT 1",
    "CREATE TABLE t AS SELECT 1",
    "SELECT * FROM bronze.loans",
    "SELECT loan_id FROM bronze.loans",
    "SELECT loan_id, SUM(amount) AS v FROM bronze.loans GROUP BY loan_id",
    "SELECT SUM(amount) AS v FROM bronze.loans; DROP TABLE bronze.loans",
    "COPY bronze.loans TO 'out.csv'",
    "SELECT SUM(a) AS v FROM read_csv('/etc/passwd')",
    "SELECT SUM(x) AS v FROM t UNION SELECT y FROM u",
    "ATTACH 'other.db'",
    "PRAGMA database_list",
    "SELECT MAX(amount) AS v INTO newtable FROM bronze.loans",
]


@pytest.mark.parametrize("sql", UNSAFE)
def test_unsafe_queries_are_rejected(sql: str) -> None:
    with pytest.raises(GuardViolation):
        check_query(sql, "duckdb")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT SUM(amount) AS v FROM bronze.loans",
        "SELECT COUNT(*) AS n FROM (SELECT * FROM bronze.loans WHERE status = 'OPEN') AS q",
        "SELECT COUNT(*) - COUNT(amount) AS n FROM bronze.loans",
        "WITH a AS (SELECT * FROM t) SELECT SUM(x) AS v, COUNT(*) AS n FROM a",
        "SELECT COALESCE(SUM(x), 0) AS v FROM t",
    ],
)
def test_aggregates_pass(sql: str) -> None:
    check_query(sql, "duckdb")


def test_safe_dimension_only_in_allow_mode() -> None:
    q = "SELECT currency, SUM(amount) AS v FROM bronze.loans GROUP BY currency"
    with pytest.raises(GuardViolation):
        check_query(q, "duckdb", "aggregates_only", {"bronze.loans.currency"})
    check_query(q, "duckdb", "allow_dimensions", {"bronze.loans.currency"})
    with pytest.raises(GuardViolation):
        check_query(q, "duckdb", "allow_dimensions", {"bronze.loans.status"})


def test_guard_logs_every_query_and_caps_rows() -> None:
    g = Guard(lambda sql: [(42,)], "duckdb")
    assert g.scalar("SELECT SUM(x) AS v FROM t", "purpose A") == 42
    assert len(g.log) == 1 and g.log[0].purpose == "purpose A" and len(g.log[0].sha256) == 64
    g2 = Guard(lambda sql: [(1,)] * 100, "duckdb", max_rows=50)
    with pytest.raises(GuardViolation):
        g2.run("SELECT SUM(x) AS v FROM t", "too many rows")


def test_guard_checks_before_executing() -> None:
    calls: list[str] = []
    g = Guard(lambda sql: calls.append(sql) or [(1,)], "duckdb")
    with pytest.raises(GuardViolation):
        g.scalar("SELECT * FROM t", "raw rows")
    assert calls == []
