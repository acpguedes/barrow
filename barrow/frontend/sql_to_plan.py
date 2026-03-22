"""Convert SQL input to a LogicalPlan."""

from __future__ import annotations

from barrow.core.nodes import Scan, Sink, SqlQuery
from barrow.core.plan import LogicalPlan


def sql_to_plan(
    query: str,
    input_path: str | None = None,
    input_format: str | None = None,
    input_delimiter: str | None = None,
    output_path: str | None = None,
    output_format: str | None = None,
    output_delimiter: str | None = None,
) -> LogicalPlan:
    """Build a plan from a SQL query string."""
    scan = Scan(path=input_path, format=input_format, delimiter=input_delimiter)
    sql_node = SqlQuery(child=scan, query=query)
    sink = Sink(
        child=sql_node,
        path=output_path,
        format=output_format,
        delimiter=output_delimiter,
    )
    return LogicalPlan(sink)
