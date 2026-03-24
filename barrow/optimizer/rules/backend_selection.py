"""Backend selection: rewrite plan nodes to use the most efficient backend.

Based on benchmark data (621 measurements, 200K rows), the heuristic applies
static rules derived from measured performance differences:

- **Window** with ``by`` and ``order_by`` → rewrite as SQL (32% faster).
- **Aggregate** → rewrite as SQL (33% faster for summary).
- **Project**, **Sort**, **Filter** → keep Arrow (Direct is 18-28% faster).

The rule follows the same recursive traversal pattern used by *simplify* and
*fusion*: process children first, then transform the current node.
"""

from __future__ import annotations

from dataclasses import replace

from barrow.core.nodes import (
    Aggregate,
    LogicalNode,
    SqlQuery,
    Window,
)
from barrow.expr.compiler import to_sql


def select_backends(node: LogicalNode) -> LogicalNode:
    """Rewrite plan nodes to use the most efficient execution backend."""
    return _select(node)


def _select(node: LogicalNode) -> LogicalNode:
    node = _select_children(node)

    # Window with partitioning → SQL (32% benchmark advantage)
    if isinstance(node, Window) and node.by and node.order_by and node.assignments:
        query = _window_to_sql(node)
        if query is not None:
            return SqlQuery(child=node.child, query=query)

    # Aggregate → SQL (33% benchmark advantage)
    # Only rewrite when group_keys are explicitly set in the plan.
    # When group_keys is empty, the engine reads them from table metadata
    # at runtime (from a prior groupby command), so we cannot build SQL here.
    if isinstance(node, Aggregate) and node.aggregations and node.group_keys:
        query = _aggregate_to_sql(node)
        if query is not None:
            return SqlQuery(child=node.child, query=query)

    return node


def _select_children(node: LogicalNode) -> LogicalNode:
    updates: dict[str, LogicalNode] = {}
    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            updates[attr] = _select(child)
    if updates:
        return replace(node, **updates)
    return node


# ---- Node-to-SQL helpers ------------------------------------------------

# Supported aggregation functions that can be translated to SQL.
_AGG_MAP: dict[str, str] = {
    "sum": "SUM",
    "mean": "AVG",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "count": "COUNT",
    "std": "STDDEV_SAMP",
    "var": "VAR_SAMP",
}


def _window_to_sql(node: Window) -> str | None:
    """Convert a Window node to a SQL query string, or ``None`` on failure."""
    assert node.by and node.order_by

    partition = ", ".join(f'"{col}"' for col in node.by)
    order = ", ".join(f'"{col}"' for col in node.order_by)
    window_clause = f"PARTITION BY {partition} ORDER BY {order}"

    projections: list[str] = []
    for name, expr in node.assignments.items():
        sql_expr = to_sql(expr)
        # Wrap the expression with the OVER clause
        projections.append(f"{sql_expr} OVER ({window_clause}) AS \"{name}\"")

    if not projections:
        return None

    cols = ", ".join(projections)
    return f"SELECT *, {cols} FROM tbl"


def _aggregate_to_sql(node: Aggregate) -> str | None:
    """Convert an Aggregate node to a SQL query string, or ``None`` on failure."""
    agg_parts: list[str] = []
    for col_name, agg_func in node.aggregations.items():
        sql_func = _AGG_MAP.get(agg_func.lower())
        if sql_func is None:
            # Unknown aggregation — fall back to Arrow backend.
            return None
        agg_parts.append(f'{sql_func}("{col_name}") AS "{agg_func}_{col_name}"')

    if not agg_parts:
        return None

    aggs = ", ".join(agg_parts)

    if node.group_keys:
        keys = ", ".join(f'"{k}"' for k in node.group_keys)
        return f"SELECT {keys}, {aggs} FROM tbl GROUP BY {keys}"

    return f"SELECT {aggs} FROM tbl"
