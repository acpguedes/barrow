"""Tests for the fusion optimizer rule."""

from barrow.core.nodes import Scan, Mutate, Project
from barrow.optimizer.rules.fusion import fuse
from barrow.expr import parse


def test_fuse_adjacent_mutates():
    scan = Scan()
    m1 = Mutate(child=scan, assignments={"c": parse("a + 1")})
    m2 = Mutate(child=m1, assignments={"d": parse("b + 2")})
    result = fuse(m2)
    assert isinstance(result, Mutate)
    assert isinstance(result.child, Scan)  # Inner mutate removed
    assert "c" in result.assignments
    assert "d" in result.assignments


def test_fuse_adjacent_projects():
    scan = Scan()
    p1 = Project(child=scan, columns=["a", "b", "c"])
    p2 = Project(child=p1, columns=["a", "b"])
    result = fuse(p2)
    assert isinstance(result, Project)
    assert isinstance(result.child, Scan)
    assert result.columns == ["a", "b"]


def test_no_fusion_needed():
    scan = Scan()
    proj = Project(child=scan, columns=["a"])
    result = fuse(proj)
    assert isinstance(result, Project)
    assert isinstance(result.child, Scan)
