"""Tests for the DuckDB execution backend."""

from barrow.execution.backends.duckdb_backend import DuckDBBackend


def test_duckdb_sql(sample_table):
    backend = DuckDBBackend()
    result = backend.execute_sql(sample_table, "SELECT a FROM tbl WHERE a > 1")
    assert set(result.table["a"].to_pylist()) == {2, 3}


def test_duckdb_aggregation(sample_table):
    backend = DuckDBBackend()
    result = backend.execute_sql(
        sample_table, "SELECT grp, SUM(a) AS s FROM tbl GROUP BY grp ORDER BY grp"
    )
    assert result.table.to_pydict() == {"grp": ["x", "y"], "s": [3, 3]}
