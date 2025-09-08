import logging
import pytest

from barrow.operations import select


def test_select_columns(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        result = select(sample_table, ["a", "grp"])
    assert result.column_names == ["a", "grp"]
    assert "Selecting columns ['a', 'grp']" in caplog.text


def test_select_missing_column(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(KeyError):
            select(sample_table, ["a", "missing"])
    assert "Selecting columns ['a', 'missing']" in caplog.text

