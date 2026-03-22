"""Simplify rule: remove redundant plan nodes."""

from __future__ import annotations

from dataclasses import replace

from barrow.core.nodes import LogicalNode


def simplify(node: LogicalNode) -> LogicalNode:
    """Remove redundant plan nodes."""
    return _simplify(node)


def _simplify(node: LogicalNode) -> LogicalNode:
    node = _simplify_children(node)
    return node


def _simplify_children(node: LogicalNode) -> LogicalNode:
    updates: dict[str, LogicalNode] = {}
    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            updates[attr] = _simplify(child)
    if updates:
        return replace(node, **updates)
    return node
