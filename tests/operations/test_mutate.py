import numpy as np
import pytest

from barrow.expr import parse
from barrow.operations import mutate


def test_mutate_columns(sample_table):
    result = mutate(sample_table, c=parse("a + b"), b=parse("b * 2"))
    assert result.column_names == ["a", "b", "grp", "c"]
    assert result["b"].to_pylist() == [8, 10, 12]
    assert result["c"].to_pylist() == [5, 7, 9]


def test_mutate_invalid_expression(sample_table):
    with pytest.raises(NameError):
        mutate(sample_table, d=parse("unknown + 1"))


def test_mutate_numpy_function(sample_table):
    result = mutate(sample_table, d=parse("sqrt(a) + b"))
    expected = [np.sqrt(1) + 4, np.sqrt(2) + 5, np.sqrt(3) + 6]
    assert result["d"].to_pylist() == pytest.approx(expected)


def test_mutate_unknown_function(sample_table):
    expr = parse("nosuch(a)")
    with pytest.raises(NameError):
        mutate(sample_table, d=expr)

