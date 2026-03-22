"""Projection pushdown: prune unused columns at scan."""

from __future__ import annotations

from dataclasses import replace

from barrow.core.nodes import LogicalNode, Project, Scan


def push_projections_down(node: LogicalNode) -> LogicalNode:
    """Push column pruning toward scan nodes."""
    return _push_proj(node)


def _push_proj(node: LogicalNode) -> LogicalNode:
    node = _push_proj_children(node)

    # Project(Scan(...)) -> Scan(columns=...)
    if isinstance(node, Project) and isinstance(node.child, Scan):
        scan = node.child
        if scan.columns is None:
            return replace(node, child=replace(scan, columns=list(node.columns)))

    return node


def _push_proj_children(node: LogicalNode) -> LogicalNode:
    updates: dict[str, LogicalNode] = {}
    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            updates[attr] = _push_proj(child)
    if updates:
        return replace(node, **updates)
    return node
