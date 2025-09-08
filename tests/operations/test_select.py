import pytest

from barrow.operations import select


def test_select_columns(sample_table):
    result = select(sample_table, ["a", "grp"])
    assert result.column_names == ["a", "grp"]


def test_select_missing_column(sample_table):
    with pytest.raises(KeyError):
        select(sample_table, ["a", "missing"])

