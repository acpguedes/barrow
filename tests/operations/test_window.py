import pytest

from barrow.expr import parse
from barrow.operations import window


def test_row_number(sample_table):
    result = window(sample_table, by=["grp"], order_by=["a"], rn=parse("row_number()"))
    assert result["rn"].to_pylist() == [1, 2, 1]


def test_rolling_mean(sample_table):
    result = window(
        sample_table, by=["grp"], order_by=["a"], ma=parse("rolling_mean(a, 2)")
    )
    out = [round(x, 3) for x in result["ma"].to_pylist()]
    assert out == [1.0, 1.5, 3.0]


def test_invalid_expression(sample_table):
    with pytest.raises(NameError):
        window(sample_table, None, None, bad=parse("unknown + 1"))


def test_unknown_function(sample_table):
    with pytest.raises(NameError):
        window(sample_table, None, None, bad=parse("nosuch(a)"))
