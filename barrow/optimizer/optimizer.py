"""Main optimizer: applies transformation rules to a logical plan."""

from __future__ import annotations

from barrow.core.plan import LogicalPlan

from .rules.backend_selection import select_backends
from .rules.filter_pushdown import push_filters_down
from .rules.fusion import fuse
from .rules.projection_pushdown import push_projections_down
from .rules.simplify import simplify


def optimize(plan: LogicalPlan) -> LogicalPlan:
    """Apply optimization rules to *plan* and return the optimized plan."""
    root = plan.root
    root = simplify(root)
    root = fuse(root)
    root = push_filters_down(root)
    root = push_projections_down(root)
    root = select_backends(root)
    return LogicalPlan(root)


__all__ = ["optimize"]
