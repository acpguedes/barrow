"""Tests for projection pushdown optimizer rule."""

from barrow.core.nodes import Scan, Project
from barrow.optimizer.rules.projection_pushdown import push_projections_down


def test_push_projection_into_scan():
    scan = Scan(path="data.csv")
    proj = Project(child=scan, columns=["a", "b"])
    result = push_projections_down(proj)
    # Projection pushed into scan
    assert isinstance(result, Project)
    assert isinstance(result.child, Scan)
    assert result.child.columns == ["a", "b"]


def test_no_pushdown_when_scan_has_columns():
    scan = Scan(path="data.csv", columns=["a", "b", "c"])
    proj = Project(child=scan, columns=["a"])
    result = push_projections_down(proj)
    # No change — scan already has columns
    assert result.child.columns == ["a", "b", "c"]
