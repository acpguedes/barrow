"""Filter pushdown rule: move filters closer to data sources."""

from __future__ import annotations

from dataclasses import replace

from barrow.core.nodes import Filter, LogicalNode, Sort


def push_filters_down(node: LogicalNode) -> LogicalNode:
    """Push filter operations closer to scan nodes."""
    return _push(node)


def _push(node: LogicalNode) -> LogicalNode:
    node = _push_children(node)

    # Filter(Sort(child)) -> Sort(Filter(child))
    if isinstance(node, Filter) and isinstance(node.child, Sort):
        return replace(
            node.child,
            child=replace(node, child=node.child.child),
        )

    return node


def _push_children(node: LogicalNode) -> LogicalNode:
    updates: dict[str, LogicalNode] = {}
    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            updates[attr] = _push(child)
    if updates:
        return replace(node, **updates)
    return node
