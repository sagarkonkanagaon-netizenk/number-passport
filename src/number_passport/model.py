"""Core data structures shared by the parsers, the engine and the evidence writer."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from sqlglot import exp


class StepKind(str, Enum):
    FILTER = "filter"
    INNER_JOIN = "inner_join"
    DEDUP = "dedup"
    DISTINCT = "distinct"
    HAVING = "having"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True)
class SourceRef:
    """Where something lives in the code base."""

    file: str
    line: int | None = None

    def __str__(self) -> str:
        return f"{self.file}:{self.line}" if self.line else self.file


@dataclass
class Model:
    """One piece of pipeline code that materialises one table."""

    target: str
    query: exp.Query
    kind: str  # sql | dbt | pyspark
    source: SourceRef
    dialect: str
    upstream: list[str] = field(default_factory=list)
    orchestration: list[dict[str, str]] = field(default_factory=list)
    opaque: list[str] = field(default_factory=list)  # parts we could not translate faithfully
    notes: list[str] = field(default_factory=list)  # caveats that do not block reconciliation
    line_offset: int = 0  # added to sqlglot token lines to get file lines
    line_map: dict[int, int] | None = None  # for generated SQL (PySpark): SQL line -> file line

    def file_line(self, sql_line: int | None) -> int | None:
        if sql_line is None or self.source.line is None:
            return None
        if self.line_map is not None:
            return self.line_map.get(sql_line)
        return sql_line + self.line_offset

    def sql(self, dialect: str | None = None) -> str:
        return self.query.sql(dialect=dialect or self.dialect)


@dataclass
class Finding:
    severity: Severity
    code: str
    message: str
    where: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["severity"] = self.severity.value
        return d


@dataclass
class StepResult:
    kind: StepKind
    description: str
    where: str
    value_delta: str | None  # Decimal as string, change in the traced measure caused by this step
    rows_removed: int | None
    native_value_removed: str | None = None  # for joins whose lost rows cannot be expressed in target units
    native_measure: str | None = None
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["kind"] = self.kind.value
        return d


@dataclass
class QualityResult:
    table: str
    row_count: int | None
    measure_null_count: int | None = None
    duplicate_keys: int | None = None
    key: list[str] | None = None
    freshness_column: str | None = None
    latest_load: str | None = None
    checks: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Hop:
    """One edge in the trace: a table (or report cell) and what produced it."""

    index: int
    target: str
    measure: str
    target_value: str | None
    produced_by: str | None  # file:line of the model, or report location
    model_kind: str | None
    source_tables: list[str] = field(default_factory=list)
    recomputed_value: str | None = None
    relaxed_value: str | None = None
    steps: list[StepResult] = field(default_factory=list)
    residual: str | None = None
    status: str = "unknown"  # reconciled | gap | not_reproducible | code_only | source | error
    role: str = "primary"  # primary: carries the traced value; factor: a rate/weight/adjustment joined in
    quality: list[QualityResult] = field(default_factory=list)
    factors: list[str] = field(default_factory=list)
    orchestration: list[dict[str, str]] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["steps"] = [s.to_dict() for s in self.steps]
        d["quality"] = [q.to_dict() for q in self.quality]
        d["findings"] = [f.to_dict() for f in self.findings]
        return d


@dataclass
class Metric:
    """A number someone may ask about.

    ``sum`` and ``count`` are additive, so they are traced hop by hop. ``avg`` and ``ratio`` are not: they
    are traced as two additive parts (``avg`` = sum / count of non-null values; ``ratio`` = numerator /
    denominator, each a sum or a count) and the figure is re-derived from the two reconciled parts.
    """

    name: str
    table: str
    column: str
    agg: str = "sum"
    where: str | None = None
    label: str | None = None
    report: dict[str, Any] | None = None  # {"file":..., "row":..., "column":..., "value":...}
    numerator: Metric | None = None
    denominator: Metric | None = None
    scale: Decimal = Decimal(1)  # 100 for a percentage

    def __post_init__(self) -> None:
        self.agg = self.agg.lower()
        if self.agg not in {"sum", "count", "avg", "ratio"}:
            raise ValueError(f"metric {self.name!r}: aggregate {self.agg!r} must be sum, count, avg or ratio")
        if self.agg == "avg" and self.numerator is None:
            not_null = f"{self.column} IS NOT NULL"
            self.numerator = Metric(f"{self.name}__sum", self.table, self.column, "sum", self.where)
            self.denominator = Metric(
                f"{self.name}__count",
                self.table,
                self.column,
                "count",
                f"({self.where}) AND {not_null}" if self.where else not_null,
            )
        if self.agg == "ratio" and (self.numerator is None or self.denominator is None):
            raise ValueError(f"metric {self.name!r}: a ratio needs a numerator and a denominator")
        for part in (self.numerator, self.denominator):
            if part is not None and part.agg not in {"sum", "count"}:
                raise ValueError(f"metric {self.name!r}: ratio parts must be sums or counts, not {part.agg}")

    @property
    def composite(self) -> bool:
        return self.agg in {"avg", "ratio"}
