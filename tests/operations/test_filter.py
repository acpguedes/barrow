import pytest

from barrow.expr import parse
from barrow.operations import filter as filter_rows


def test_filter_rows(sample_table):
    result = filter_rows(sample_table, parse("a > 1"))
    assert result["a"].to_pylist() == [2, 3]


def test_filter_invalid_expression(sample_table):
    with pytest.raises(NameError):
        filter_rows(sample_table, parse("c > 1"))


def test_filter_numpy_function(sample_table):
    result = filter_rows(sample_table, parse("sqrt(a) > 1"))
    assert result["a"].to_pylist() == [2, 3]


def test_filter_unknown_function(sample_table):
    expr = parse("nosuch(a)")
    with pytest.raises(NameError):
        filter_rows(sample_table, expr)

