"""Tests for ExecutionResult."""

from barrow.core.result import ExecutionResult
from barrow.core.properties import LogicalProperties


def test_basic_result(sample_table):
    result = ExecutionResult(sample_table)
    assert result.to_table() is sample_table
    assert result.table is sample_table
    assert result.num_rows == 3
    assert result.num_columns == 3
    assert result.column_names == ["a", "b", "grp"]


def test_result_with_properties(sample_table):
    props = LogicalProperties(group_keys=["grp"])
    result = ExecutionResult(sample_table, properties=props)
    assert result.properties.group_keys == ["grp"]


def test_result_repr(sample_table):
    result = ExecutionResult(sample_table)
    r = repr(result)
    assert "rows=3" in r
    assert "cols=3" in r


def test_result_schema(sample_table):
    result = ExecutionResult(sample_table)
    assert result.schema == sample_table.schema
