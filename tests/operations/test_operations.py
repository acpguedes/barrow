import pyarrow as pa
import pytest

from barrow.operations import (
    select,
    filter as filter_rows,
    mutate,
    groupby,
    summary,
    ungroup,
)
from barrow.expr import parse


def test_select_columns(sample_table):
    result = select(sample_table, ["a", "grp"])
    assert result.column_names == ["a", "grp"]


def test_select_missing_column(sample_table):
    with pytest.raises(KeyError):
        select(sample_table, ["a", "missing"])


def test_filter_rows(sample_table):
    result = filter_rows(sample_table, parse("a > 1"))
    assert result["a"].to_pylist() == [2, 3]


def test_filter_invalid_expression(sample_table):
    with pytest.raises(NameError):
        filter_rows(sample_table, parse("c > 1"))


def test_mutate_columns(sample_table):
    result = mutate(sample_table, c=parse("a + b"), b=parse("b * 2"))
    assert result.column_names == ["a", "b", "grp", "c"]
    assert result["b"].to_pylist() == [8, 10, 12]
    assert result["c"].to_pylist() == [5, 7, 9]


def test_mutate_invalid_expression(sample_table):
    with pytest.raises(NameError):
        mutate(sample_table, d=parse("unknown + 1"))


def test_groupby_summary(parquet_table):
    gb = groupby(parquet_table.select(["grp", "a"]), "grp")
    result = summary(gb, {"a": "sum"})
    out = dict(zip(result["grp"].to_pylist(), result["a_sum"].to_pylist()))
    assert out == {"x": 3, "y": 3}


def test_groupby_invalid_key(sample_table):
    gb = groupby(sample_table, "missing")
    with pytest.raises(pa.ArrowInvalid):
        summary(gb, {"a": "sum"})


def test_summary_invalid_aggregation(sample_table):
    gb = groupby(sample_table.select(["grp", "a"]), "grp")
    with pytest.raises(pa.ArrowKeyError):
        summary(gb, {"a": "nonesuch"})


def test_ungroup_removes_metadata(sample_table):
    gb = groupby(sample_table, "grp")
    assert (gb.schema.metadata or {}).get(b"grouped_by") == b"grp"
    result = ungroup(gb)
    assert (result.schema.metadata or {}).get(b"grouped_by") is None


def test_filter_unknown_function(sample_table):
    expr = parse("nosuch(a)")
    with pytest.raises(NameError):
        filter_rows(sample_table, expr)


def test_mutate_unknown_function(sample_table):
    expr = parse("nosuch(a)")
    with pytest.raises(NameError):
        mutate(sample_table, d=expr)

