import logging
import pytest

from barrow.operations import select


@pytest.mark.parametrize(
    "cols,expected",
    [
        (["a", "grp"], ["a", "grp"]),
        (["b"], ["b"]),
    ],
)
def test_select_columns(sample_table, cols, expected, caplog):
    """Selecting existing columns returns only those columns."""
    with caplog.at_level(logging.DEBUG):
        result = select(sample_table, cols)
    assert (
        result.column_names == expected
    ), f"Expected columns {expected} but got {result.column_names} for {cols}"
    assert (
        f"Selecting columns {cols}" in caplog.text
    ), f"Missing log entry for selecting {cols}"


@pytest.mark.parametrize("cols", [["a", "missing"], ["missing"]])
def test_select_missing_column(sample_table, cols, caplog):
    """Selecting missing columns raises a ``KeyError`` mentioning them."""
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(KeyError) as exc_info:
            select(sample_table, cols)
    missing = [c for c in cols if c not in sample_table.column_names]
    assert all(
        m in str(exc_info.value) for m in missing
    ), f"Error message does not mention missing columns {missing}: {exc_info.value}"
    assert (
        f"Selecting columns {cols}" in caplog.text
    ), f"Missing log entry for selecting {cols}"
