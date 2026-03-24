"""Plan execution engine."""

from __future__ import annotations

import os
import sys
import time

from barrow.core.errors import ExecutionError
from barrow.core.nodes import (
    Aggregate,
    Filter,
    GroupBy,
    Join,
    Limit,
    LogicalNode,
    Mutate,
    Project,
    Scan,
    Sink,
    Sort,
    SqlQuery,
    Ungroup,
    View,
    Window,
)
from barrow.core.result import ExecutionResult

from .backends.arrow_backend import ArrowBackend
from .backends.duckdb_backend import DuckDBBackend

_arrow = ArrowBackend()
_duckdb = DuckDBBackend()

_PROFILE = os.environ.get("BARROW_PROFILE") == "1"


def execute(node: LogicalNode) -> ExecutionResult:
    """Execute a logical plan tree and return the result."""
    return _execute(node)


def _execute(node: LogicalNode) -> ExecutionResult:
    if isinstance(node, Scan):
        return _exec_scan(node)

    if isinstance(node, Sink):
        return _exec_sink(node)

    if isinstance(node, Join):
        left_result = _execute(node.left)
        right_result = _execute(node.right)
        return _arrow.execute_join(
            left_result.table,
            right_result.table,
            node.left_on,
            node.right_on,
            node.join_type,
        )

    # All other nodes have a single child
    child_result = _execute(node.child)  # type: ignore[attr-defined]
    table = child_result.table

    if isinstance(node, Project):
        return _arrow.execute_project(table, node.columns)

    if isinstance(node, Filter):
        assert node.expression is not None
        return _arrow.execute_filter(table, node.expression)

    if isinstance(node, Mutate):
        return _arrow.execute_mutate(table, node.assignments)

    if isinstance(node, Aggregate):
        return _arrow.execute_aggregate(table, node.group_keys, node.aggregations)

    if isinstance(node, Sort):
        return _arrow.execute_sort(table, node.keys, node.descending)

    if isinstance(node, Window):
        return _arrow.execute_window(table, node.by, node.order_by, node.assignments)

    if isinstance(node, GroupBy):
        return _arrow.execute_groupby(table, node.keys)

    if isinstance(node, Ungroup):
        return _arrow.execute_ungroup(table)

    if isinstance(node, Limit):
        return ExecutionResult(table.slice(0, node.n))

    if isinstance(node, View):
        return child_result

    if isinstance(node, SqlQuery):
        return _duckdb.execute_sql(table, node.query)

    raise ExecutionError(f"Unknown node type: {type(node).__name__}")


def _exec_scan(node: Scan) -> ExecutionResult:
    """Execute a Scan node by reading from file or STDIN."""
    t0 = time.perf_counter() if _PROFILE else 0.0
    from barrow.io import read_table

    table = read_table(node.path, node.format, node.delimiter)
    if node.columns:
        available = set(table.column_names)
        cols = [c for c in node.columns if c in available]
        if cols:
            table = table.select(cols)
    if _PROFILE:
        elapsed = time.perf_counter() - t0
        print(
            f"BARROW_PROFILE: scan={elapsed:.4f}s rows={table.num_rows} cols={table.num_columns}",
            file=sys.stderr,
        )
    return ExecutionResult(table)


def _exec_sink(node: Sink) -> ExecutionResult:
    """Execute a Sink node by writing to file or STDOUT."""
    child_result = _execute(node.child)
    t0 = time.perf_counter() if _PROFILE else 0.0
    from barrow.io import write_table

    write_table(child_result.table, node.path, node.format, node.delimiter)
    if _PROFILE:
        elapsed = time.perf_counter() - t0
        print(
            f"BARROW_PROFILE: sink={elapsed:.4f}s rows={child_result.table.num_rows}",
            file=sys.stderr,
        )
    return child_result
