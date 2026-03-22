"""Fusion rule: merge adjacent compatible operations."""

from __future__ import annotations

from dataclasses import replace

from barrow.core.nodes import LogicalNode, Mutate, Project


def fuse(node: LogicalNode) -> LogicalNode:
    """Fuse adjacent compatible operations."""
    return _fuse(node)


def _fuse(node: LogicalNode) -> LogicalNode:
    node = _fuse_children(node)

    # Fuse Mutate(Mutate(...)) into single Mutate
    if isinstance(node, Mutate) and isinstance(node.child, Mutate):
        merged = dict(node.child.assignments)
        merged.update(node.assignments)
        return replace(node, child=node.child.child, assignments=merged)

    # Fuse Project(Project(...)) — outer columns win
    if isinstance(node, Project) and isinstance(node.child, Project):
        return replace(node, child=node.child.child)

    return node


def _fuse_children(node: LogicalNode) -> LogicalNode:
    updates: dict[str, LogicalNode] = {}
    for attr in ("child", "left", "right"):
        child = getattr(node, attr, None)
        if (
            child is not None
            and isinstance(child, LogicalNode)
            and type(child) is not LogicalNode
        ):
            updates[attr] = _fuse(child)
    if updates:
        return replace(node, **updates)
    return node
