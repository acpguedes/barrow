"""DuckDB execution backend."""

from __future__ import annotations

import pyarrow as pa

from barrow.core.result import ExecutionResult


class DuckDBBackend:
    """Execute operations using DuckDB."""

    def execute_sql(self, table: pa.Table, query: str) -> ExecutionResult:
        from barrow.operations import sql

        return ExecutionResult(sql(table, query))
