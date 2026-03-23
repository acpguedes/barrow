"""Backend selection: annotate plan nodes with preferred execution backend.

Currently a pass-through — the execution engine uses type-based dispatch.
This rule exists as a hook for future cost-based backend selection.
"""

from __future__ import annotations

from barrow.core.nodes import LogicalNode


def select_backends(node: LogicalNode) -> LogicalNode:
    """Annotate plan nodes with backend hints (currently a no-op)."""
    return node
