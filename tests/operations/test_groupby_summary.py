import logging
import pyarrow as pa
import pytest

from barrow.operations import groupby, summary


def test_groupby_summary(parquet_table, caplog):
    with caplog.at_level(logging.DEBUG):
        gb = groupby(parquet_table.select(["grp", "a"]), "grp")
        result = summary(gb, {"a": "sum"})
    out = dict(zip(result["grp"].to_pylist(), result["a_sum"].to_pylist()))
    assert out == {"x": 3, "y": 3}
    assert "Grouping with keys ['grp']" in caplog.text
    assert "Summarizing with aggregations {'a': 'sum'}" in caplog.text


def test_groupby_invalid_key(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        gb = groupby(sample_table, "missing")
        with pytest.raises(pa.ArrowInvalid):
            summary(gb, {"a": "sum"})
    assert "Grouping with keys ['missing']" in caplog.text
    assert "Summarizing with aggregations {'a': 'sum'}" in caplog.text


def test_summary_invalid_aggregation(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        gb = groupby(sample_table.select(["grp", "a"]), "grp")
        with pytest.raises(pa.ArrowKeyError):
            summary(gb, {"a": "nonesuch"})
    assert "Grouping with keys ['grp']" in caplog.text
    assert "Summarizing with aggregations {'a': 'nonesuch'}" in caplog.text

