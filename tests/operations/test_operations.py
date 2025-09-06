import pyarrow as pa

from barrow.operations import (
    select,
    filter as filter_rows,
    mutate,
    groupby,
    summary,
)


def make_table():
    return pa.table({"a": [1, 2, 3], "b": [4, 5, 6], "grp": ["x", "x", "y"]})


def test_select_columns():
    table = make_table()
    result = select(table, ["a", "grp"])
    assert result.column_names == ["a", "grp"]


def test_filter_rows():
    table = make_table()
    result = filter_rows(table, "a > 1")
    assert result["a"].to_pylist() == [2, 3]


def test_mutate_columns():
    table = make_table()
    result = mutate(table, c="a + b", b="b * 2")
    assert result.column_names == ["a", "b", "grp", "c"]
    assert result["b"].to_pylist() == [8, 10, 12]
    assert result["c"].to_pylist() == [5, 7, 9]


def test_groupby_summary():
    table = make_table()
    gb = groupby(table.select(["grp", "a"]), "grp", use_threads=False)
    result = summary(gb, {"a": "sum"})
    out = dict(zip(result["grp"].to_pylist(), result["a_sum"].to_pylist()))
    assert out == {"x": 3, "y": 3}
