"""Execution result wrapper."""

from __future__ import annotations

import pyarrow as pa

from .properties import LogicalProperties


class ExecutionResult:
    """Normalized result from the execution engine."""

    def __init__(
        self,
        table: pa.Table,
        properties: LogicalProperties | None = None,
    ) -> None:
        self._table = table
        self._properties = properties or LogicalProperties()

    def to_table(self) -> pa.Table:
        """Return the underlying Arrow table."""
        return self._table

    @property
    def table(self) -> pa.Table:
        return self._table

    @property
    def schema(self) -> pa.Schema:
        return self._table.schema

    @property
    def num_rows(self) -> int:
        return self._table.num_rows

    @property
    def num_columns(self) -> int:
        return self._table.num_columns

    @property
    def column_names(self) -> list[str]:
        return self._table.column_names

    @property
    def properties(self) -> LogicalProperties:
        return self._properties

    def __repr__(self) -> str:
        return (
            f"ExecutionResult(rows={self.num_rows}, "
            f"cols={self.num_columns}, "
            f"columns={self.column_names})"
        )


__all__ = ["ExecutionResult"]
