"""Tests for the Arrow execution backend."""

from barrow.execution.backends.arrow_backend import ArrowBackend
from barrow.expr import parse


def test_arrow_project(sample_table):
    backend = ArrowBackend()
    result = backend.execute_project(sample_table, ["a", "b"])
    assert result.column_names == ["a", "b"]


def test_arrow_filter(sample_table):
    backend = ArrowBackend()
    result = backend.execute_filter(sample_table, parse("a > 1"))
    assert result.num_rows == 2


def test_arrow_sort(sample_table):
    backend = ArrowBackend()
    result = backend.execute_sort(sample_table, ["a"], [True])
    assert result.table["a"].to_pylist() == [3, 2, 1]


def test_arrow_groupby(sample_table):
    backend = ArrowBackend()
    result = backend.execute_groupby(sample_table, ["grp"])
    meta = result.table.schema.metadata
    assert b"grouped_by" in meta


def test_arrow_ungroup(sample_table):
    backend = ArrowBackend()
    grouped = backend.execute_groupby(sample_table, ["grp"])
    result = backend.execute_ungroup(grouped.table)
    meta = result.table.schema.metadata or {}
    assert b"grouped_by" not in meta
