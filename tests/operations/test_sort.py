"""Tests for the sort operation."""

import pyarrow as pa
from barrow.operations import sort


def test_sort_ascending(sample_table):
    result = sort(sample_table, ["a"])
    assert result["a"].to_pylist() == [1, 2, 3]


def test_sort_descending(sample_table):
    result = sort(sample_table, ["a"], [True])
    assert result["a"].to_pylist() == [3, 2, 1]


def test_sort_by_string_column(sample_table):
    result = sort(sample_table, ["grp"])
    assert result["grp"].to_pylist() == ["x", "x", "y"]


def test_sort_multi_column():
    table = pa.table({"x": [2, 1, 2, 1], "y": [3, 1, 1, 2]})
    result = sort(table, ["x", "y"])
    assert result["x"].to_pylist() == [1, 1, 2, 2]
    assert result["y"].to_pylist() == [1, 2, 1, 3]


def test_sort_preserves_all_rows(sample_table):
    result = sort(sample_table, ["a"])
    assert result.num_rows == sample_table.num_rows
    assert set(result.column_names) == set(sample_table.column_names)
