"""Logical plan node definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from barrow.expr import Expression


@dataclass(frozen=True)
class LogicalNode:
    """Base class for all logical plan nodes."""


@dataclass(frozen=True)
class Scan(LogicalNode):
    """Read data from a source."""

    path: str | None = None
    format: str | None = None
    delimiter: str | None = None
    columns: list[str] | None = None


@dataclass(frozen=True)
class Project(LogicalNode):
    """Select a subset of columns."""

    child: LogicalNode = field(default_factory=LogicalNode)
    columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Filter(LogicalNode):
    """Filter rows by expression."""

    child: LogicalNode = field(default_factory=LogicalNode)
    expression: Expression | None = None


@dataclass(frozen=True)
class Mutate(LogicalNode):
    """Add or replace columns via expressions."""

    child: LogicalNode = field(default_factory=LogicalNode)
    assignments: dict[str, Expression] = field(default_factory=dict)


@dataclass(frozen=True)
class Aggregate(LogicalNode):
    """Group and aggregate."""

    child: LogicalNode = field(default_factory=LogicalNode)
    group_keys: list[str] = field(default_factory=list)
    aggregations: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Join(LogicalNode):
    """Join two tables."""

    left: LogicalNode = field(default_factory=LogicalNode)
    right: LogicalNode = field(default_factory=LogicalNode)
    left_on: str = ""
    right_on: str = ""
    join_type: str = "inner"


@dataclass(frozen=True)
class Sort(LogicalNode):
    """Sort rows by columns."""

    child: LogicalNode = field(default_factory=LogicalNode)
    keys: list[str] = field(default_factory=list)
    descending: list[bool] = field(default_factory=list)


@dataclass(frozen=True)
class Window(LogicalNode):
    """Apply window functions."""

    child: LogicalNode = field(default_factory=LogicalNode)
    by: list[str] | None = None
    order_by: list[str] | None = None
    assignments: dict[str, Expression] = field(default_factory=dict)


@dataclass(frozen=True)
class GroupBy(LogicalNode):
    """Mark table as grouped."""

    child: LogicalNode = field(default_factory=LogicalNode)
    keys: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Ungroup(LogicalNode):
    """Remove grouping."""

    child: LogicalNode = field(default_factory=LogicalNode)


@dataclass(frozen=True)
class Limit(LogicalNode):
    """Limit number of rows."""

    child: LogicalNode = field(default_factory=LogicalNode)
    n: int = 0


@dataclass(frozen=True)
class View(LogicalNode):
    """Display table as CSV to STDOUT."""

    child: LogicalNode = field(default_factory=LogicalNode)


@dataclass(frozen=True)
class SqlQuery(LogicalNode):
    """Execute SQL against the input table."""

    child: LogicalNode = field(default_factory=LogicalNode)
    query: str = ""


@dataclass(frozen=True)
class Sink(LogicalNode):
    """Write data to a destination."""

    child: LogicalNode = field(default_factory=LogicalNode)
    path: str | None = None
    format: str | None = None
    delimiter: str | None = None


__all__ = [
    "LogicalNode",
    "Scan",
    "Project",
    "Filter",
    "Mutate",
    "Aggregate",
    "Join",
    "Sort",
    "Window",
    "GroupBy",
    "Ungroup",
    "Limit",
    "View",
    "SqlQuery",
    "Sink",
]
