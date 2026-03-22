"""Logical plan construction and traversal."""

from __future__ import annotations

from typing import Iterator

from .nodes import Aggregate, Join, LogicalNode, Scan, Sink


class LogicalPlan:
    """Wrapper around a tree of :class:`LogicalNode` objects."""

    def __init__(self, root: LogicalNode) -> None:
        self._root = root

    @property
    def root(self) -> LogicalNode:
        return self._root

    def walk(self) -> Iterator[LogicalNode]:
        """Yield all nodes in bottom-up order."""
        yield from _walk(self._root)

    def __repr__(self) -> str:
        return format_plan(self._root)


def _walk(node: LogicalNode) -> Iterator[LogicalNode]:
    """Recursively yield nodes bottom-up."""
    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            yield from _walk(child)
    yield node


def format_plan(node: LogicalNode, indent: int = 0) -> str:
    """Format a plan tree as a human-readable string."""
    prefix = "  " * indent
    name = type(node).__name__
    detail = _node_detail(node)
    lines = [f"{prefix}{name}({detail})"]

    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            lines.append(format_plan(child, indent + 1))

    return "\n".join(lines)


def _node_detail(node: LogicalNode) -> str:
    if isinstance(node, Scan):
        src = node.path or "STDIN"
        parts = [f"source={src}"]
        if node.format:
            parts.append(f"format={node.format}")
        if node.columns:
            parts.append(f"columns={node.columns}")
        return ", ".join(parts)

    if isinstance(node, Sink):
        dst = node.path or "STDOUT"
        parts = [f"dest={dst}"]
        if node.format:
            parts.append(f"format={node.format}")
        return ", ".join(parts)

    if hasattr(node, "expression") and node.expression is not None:
        return f"expr={node.expression}"

    if hasattr(node, "query"):
        return f"query={node.query!r}"

    if isinstance(node, Join):
        return f"on={node.left_on}/{node.right_on}, type={node.join_type}"

    if isinstance(node, Aggregate):
        return f"keys={node.group_keys}, aggs={list(node.aggregations.keys())}"

    if hasattr(node, "columns"):
        return f"columns={node.columns}"

    if hasattr(node, "keys") and hasattr(node, "descending"):
        return f"keys={node.keys}"

    if hasattr(node, "keys"):
        return f"keys={node.keys}"

    if hasattr(node, "assignments"):
        return f"outputs={list(node.assignments.keys())}"

    if hasattr(node, "n"):
        return f"n={node.n}"

    return ""


__all__ = ["LogicalPlan", "format_plan"]
