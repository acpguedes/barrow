"""Tests for logical plan construction and traversal."""

from barrow.core.nodes import Scan, Project, Filter, Sink
from barrow.core.plan import LogicalPlan
from barrow.expr import parse


def test_plan_walk_bottom_up():
    scan = Scan(path="data.csv")
    proj = Project(child=scan, columns=["a"])
    sink = Sink(child=proj)
    plan = LogicalPlan(sink)

    nodes = list(plan.walk())
    assert len(nodes) == 3
    # Bottom-up: Scan first, then Project, then Sink
    assert isinstance(nodes[0], Scan)
    assert isinstance(nodes[1], Project)
    assert isinstance(nodes[2], Sink)


def test_format_plan():
    scan = Scan(path="data.csv", format="csv")
    filt = Filter(child=scan, expression=parse("a > 1"))
    sink = Sink(child=filt)
    plan = LogicalPlan(sink)

    text = repr(plan)
    assert "Sink" in text
    assert "Filter" in text
    assert "Scan" in text
    assert "data.csv" in text


def test_plan_root():
    scan = Scan(path="test.csv")
    plan = LogicalPlan(scan)
    assert plan.root is scan
