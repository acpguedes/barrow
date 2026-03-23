"""Execute SQL queries using DuckDB."""

from __future__ import annotations

import duckdb
import pyarrow as pa


def sql(table: pa.Table, query: str) -> pa.Table:
    """Return the result of *query* executed against *table*.

    The input table is registered as a view named ``tbl`` and the query is
    evaluated using DuckDB, returning the result as a new :class:`pa.Table`.
    """
    con = duckdb.connect()
    try:
        con.register("tbl", table)
        result = con.execute(query)
        if hasattr(result, "to_arrow_table"):
            return result.to_arrow_table()
        return result.fetch_arrow_table()
    finally:
        con.close()


__all__ = ["sql"]
