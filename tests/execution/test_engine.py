"""Tests for the execution engine."""

from barrow.core.nodes import Scan, Project, Filter, Sort, Sink, SqlQuery
from barrow.execution.engine import execute
from barrow.expr import parse


def test_execute_scan(sample_csv):
    scan = Scan(path=sample_csv, format="csv")
    result = execute(scan)
    assert result.num_rows == 3
    assert "a" in result.column_names


def test_execute_project(sample_csv):
    scan = Scan(path=sample_csv, format="csv")
    proj = Project(child=scan, columns=["a", "b"])
    result = execute(proj)
    assert result.column_names == ["a", "b"]


def test_execute_filter(sample_csv):
    scan = Scan(path=sample_csv, format="csv")
    filt = Filter(child=scan, expression=parse("a > 1"))
    result = execute(filt)
    assert result.num_rows == 2


def test_execute_sort(sample_csv):
    scan = Scan(path=sample_csv, format="csv")
    sort = Sort(child=scan, keys=["a"], descending=[True])
    result = execute(sort)
    assert result.table["a"].to_pylist() == [3, 2, 1]


def test_execute_sql(sample_csv):
    scan = Scan(path=sample_csv, format="csv")
    sql = SqlQuery(child=scan, query="SELECT a FROM tbl WHERE a > 1")
    result = execute(sql)
    assert set(result.table["a"].to_pylist()) == {2, 3}


def test_execute_sink(sample_csv, tmp_path):
    dst = tmp_path / "out.csv"
    scan = Scan(path=sample_csv, format="csv")
    proj = Project(child=scan, columns=["a", "b"])
    sink = Sink(child=proj, path=str(dst), format="csv")
    result = execute(sink)
    assert result.num_rows == 3
    assert dst.exists()
