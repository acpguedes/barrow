"""Execute SQL queries using DuckDB."""

from __future__ import annotations

import duckdb
import pyarrow as pa

# Module-level connection reused across calls to avoid repeated setup costs.
_connection: duckdb.DuckDBPyConnection | None = None


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Return a reusable module-level DuckDB connection."""
    global _connection
    if _connection is None:
        _connection = duckdb.connect()
    return _connection


def sql(table: pa.Table, query: str) -> pa.Table:
    """Return the result of *query* executed against *table*.

    The input table is registered as a view named ``tbl`` and the query is
    evaluated using DuckDB, returning the result as a new :class:`pa.Table`.
    """
    con = _get_connection()
    con.unregister("tbl")
    con.register("tbl", table)
    result = con.execute(query)
    return result.to_arrow_table()


__all__ = ["sql"]
