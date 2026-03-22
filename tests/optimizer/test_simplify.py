"""Tests for the simplify optimizer rule."""

from barrow.core.nodes import Scan, Project
from barrow.optimizer.rules.simplify import simplify


def test_simplify_passthrough():
    scan = Scan(path="data.csv")
    proj = Project(child=scan, columns=["a"])
    result = simplify(proj)
    assert isinstance(result, Project)
    assert isinstance(result.child, Scan)
