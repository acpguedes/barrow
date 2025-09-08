import logging
import pytest

from barrow.expr import parse
from barrow.operations import filter as filter_rows


@pytest.mark.parametrize(
    "expr_str,expected",
    [
        ("a > 1", [2, 3]),
        ("sqrt(a) > 1", [2, 3]),
    ],
)
def test_filter_rows(sample_table, expr_str, expected, caplog):
    """Filtering with valid expressions returns expected rows."""
    with caplog.at_level(logging.DEBUG):
        result = filter_rows(sample_table, parse(expr_str))
    actual = result["a"].to_pylist()
    assert (
        actual == expected
    ), f"Filtering '{expr_str}' returned {actual}, expected {expected}"
    assert (
        "Filtering with expression" in caplog.text
    ), f"Missing log entry for expression '{expr_str}'"


@pytest.mark.parametrize("expr_str", ["c > 1", "nosuch(a)"])
def test_filter_invalid_expression(sample_table, expr_str, caplog):
    """Invalid expressions raise ``NameError`` mentioning the culprit."""
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(NameError) as exc_info:
            filter_rows(sample_table, parse(expr_str))
    token = expr_str.split("(")[0].split()[0]
    assert (
        token in str(exc_info.value)
    ), f"Error message does not reference '{token}': {exc_info.value}"
    assert (
        "Filtering with expression" in caplog.text
    ), f"Missing log entry for expression '{expr_str}'"

