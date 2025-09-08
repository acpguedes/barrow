from __future__ import annotations

"""Execute SQL queries using DuckDB."""

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
        return con.execute(query).arrow()
    finally:
        con.close()


__all__ = ["sql"]
