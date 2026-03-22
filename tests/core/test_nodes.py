"""Tests for logical plan nodes."""

from barrow.core.nodes import (
    Scan,
    Project,
    Filter,
    Mutate,
    Aggregate,
    Join,
    Sort,
    SqlQuery,
    Sink,
)
from barrow.expr import parse


def test_scan_defaults():
    s = Scan()
    assert s.path is None
    assert s.format is None
    assert s.columns is None


def test_scan_with_path():
    s = Scan(path="data.csv", format="csv")
    assert s.path == "data.csv"
    assert s.format == "csv"


def test_project_columns():
    s = Scan(path="data.csv")
    p = Project(child=s, columns=["a", "b"])
    assert p.columns == ["a", "b"]
    assert isinstance(p.child, Scan)


def test_filter_expression():
    s = Scan()
    expr = parse("a > 1")
    f = Filter(child=s, expression=expr)
    assert f.expression is not None


def test_mutate_assignments():
    s = Scan()
    expr = parse("a + b")
    m = Mutate(child=s, assignments={"c": expr})
    assert "c" in m.assignments


def test_join_nodes():
    left = Scan(path="left.csv")
    right = Scan(path="right.csv")
    j = Join(left=left, right=right, left_on="id", right_on="id", join_type="inner")
    assert j.left_on == "id"
    assert j.join_type == "inner"


def test_sort_keys():
    s = Scan()
    so = Sort(child=s, keys=["a", "b"], descending=[True, False])
    assert so.keys == ["a", "b"]
    assert so.descending == [True, False]


def test_aggregate():
    s = Scan()
    a = Aggregate(child=s, group_keys=["grp"], aggregations={"total": "sum"})
    assert a.group_keys == ["grp"]


def test_sql_query():
    s = Scan()
    sq = SqlQuery(child=s, query="SELECT * FROM tbl")
    assert sq.query == "SELECT * FROM tbl"


def test_sink_defaults():
    s = Scan()
    sink = Sink(child=s)
    assert sink.path is None
    assert sink.format is None


def test_nodes_are_frozen():
    s = Scan(path="data.csv")
    try:
        s.path = "other.csv"
        assert False, "Should not be able to modify frozen dataclass"
    except AttributeError:
        pass
