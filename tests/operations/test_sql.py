import duckdb
import pytest

import pyarrow as pa

from barrow.operations import sql


def test_simple_query(sample_table: pa.Table) -> None:
    result = sql(sample_table, "SELECT a FROM tbl")
    assert result.column_names == ["a"]
    assert result["a"].to_pylist() == [1, 2, 3]


def test_filtering(sample_table: pa.Table) -> None:
    result = sql(sample_table, "SELECT * FROM tbl WHERE a > 1")
    assert result["a"].to_pylist() == [2, 3]


def test_aggregation(sample_table: pa.Table) -> None:
    result = sql(
        sample_table,
        "SELECT grp, SUM(a) AS s FROM tbl GROUP BY grp ORDER BY grp",
    )
    assert result.to_pydict() == {"grp": ["x", "y"], "s": [3, 3]}


def test_syntax_error(sample_table: pa.Table) -> None:
    with pytest.raises(duckdb.ParserException):
        sql(sample_table, "SELEKT * FROM tbl")
