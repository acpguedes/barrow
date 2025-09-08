import logging
import pytest

from barrow.expr import parse
from barrow.operations import filter as filter_rows


def test_filter_rows(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        result = filter_rows(sample_table, parse("a > 1"))
    assert result["a"].to_pylist() == [2, 3]
    assert "Filtering with expression" in caplog.text


def test_filter_invalid_expression(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(NameError):
            filter_rows(sample_table, parse("c > 1"))
    assert "Filtering with expression" in caplog.text


def test_filter_numpy_function(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        result = filter_rows(sample_table, parse("sqrt(a) > 1"))
    assert result["a"].to_pylist() == [2, 3]
    assert "Filtering with expression" in caplog.text


def test_filter_unknown_function(sample_table, caplog):
    expr = parse("nosuch(a)")
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(NameError):
            filter_rows(sample_table, expr)
    assert "Filtering with expression" in caplog.text

