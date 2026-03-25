"""Tests for the backend selection optimizer rule."""

from barrow.core.nodes import (
    Aggregate,
    Project,
    Scan,
    Sink,
    Sort,
    SqlQuery,
    Window,
)
from barrow.expr import parse
from barrow.optimizer.rules.backend_selection import select_backends


def test_window_with_by_and_order_by_becomes_sql():
    """Window with partitioning and ordering should be rewritten to SqlQuery."""
    child = Scan(path="test.csv")
    node = Window(
        child=child,
        by=["grp"],
        order_by=["ts"],
        assignments={"rn": parse("row_number()")},
    )
    result = select_backends(node)
    assert isinstance(result, SqlQuery)
    assert result.child is child
    assert "OVER" in result.query
    assert "PARTITION BY" in result.query
    assert "ORDER BY" in result.query


def test_window_without_by_stays_arrow():
    """Window without partitioning should not be rewritten."""
    child = Scan(path="test.csv")
    node = Window(
        child=child,
        by=None,
        order_by=["ts"],
        assignments={"rn": parse("row_number()")},
    )
    result = select_backends(node)
    assert isinstance(result, Window)


def test_window_without_order_by_stays_arrow():
    """Window without ordering should not be rewritten."""
    child = Scan(path="test.csv")
    node = Window(
        child=child,
        by=["grp"],
        order_by=None,
        assignments={"rn": parse("row_number()")},
    )
    result = select_backends(node)
    assert isinstance(result, Window)


def test_aggregate_becomes_sql():
    """Aggregate with known functions should be rewritten to SqlQuery."""
    child = Scan(path="test.csv")
    node = Aggregate(
        child=child,
        group_keys=["grp"],
        aggregations={"a": "sum", "b": "mean"},
    )
    result = select_backends(node)
    assert isinstance(result, SqlQuery)
    assert "SUM" in result.query
    assert "AVG" in result.query
    assert "GROUP BY" in result.query


def test_aggregate_unknown_func_stays_arrow():
    """Aggregate with unsupported function should not be rewritten."""
    child = Scan(path="test.csv")
    node = Aggregate(
        child=child,
        group_keys=["grp"],
        aggregations={"a": "first"},
    )
    result = select_backends(node)
    assert isinstance(result, Aggregate)


def test_sort_stays_arrow():
    """Sort should not be rewritten to SQL (Arrow is faster)."""
    child = Scan(path="test.csv")
    node = Sort(child=child, keys=["a"])
    result = select_backends(node)
    assert isinstance(result, Sort)


def test_project_stays_arrow():
    """Project should not be rewritten to SQL (Arrow is faster)."""
    child = Scan(path="test.csv")
    node = Project(child=child, columns=["a", "b"])
    result = select_backends(node)
    assert isinstance(result, Project)


def test_nested_window_in_sink():
    """Backend selection should traverse into nested nodes."""
    scan = Scan(path="test.csv")
    window = Window(
        child=scan,
        by=["grp"],
        order_by=["ts"],
        assignments={"rn": parse("row_number()")},
    )
    sink = Sink(child=window, path="out.csv")
    result = select_backends(sink)
    assert isinstance(result, Sink)
    assert isinstance(result.child, SqlQuery)
