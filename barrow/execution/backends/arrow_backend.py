"""Arrow-native execution backend."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from barrow.core.result import ExecutionResult

if TYPE_CHECKING:
    from barrow.expr import Expression


class ArrowBackend:
    """Execute plan operations using PyArrow and barrow.operations."""

    def execute_project(self, table: pa.Table, columns: list[str]) -> ExecutionResult:
        from barrow.operations import select

        return ExecutionResult(select(table, columns))

    def execute_filter(
        self, table: pa.Table, expression: Expression
    ) -> ExecutionResult:
        from barrow.operations import filter as op_filter

        return ExecutionResult(op_filter(table, expression))

    def execute_mutate(
        self, table: pa.Table, assignments: dict[str, Expression]
    ) -> ExecutionResult:
        from barrow.operations import mutate

        return ExecutionResult(mutate(table, **assignments))

    def execute_aggregate(
        self,
        table: pa.Table,
        group_keys: list[str],
        aggregations: dict[str, str],
    ) -> ExecutionResult:
        from barrow.operations import groupby, summary

        if group_keys:
            table = groupby(table, group_keys)
        # When group_keys is empty, the table should already carry
        # grouped_by metadata from a prior groupby command.
        return ExecutionResult(summary(table, aggregations))

    def execute_sort(
        self,
        table: pa.Table,
        keys: list[str],
        descending: list[bool] | None = None,
    ) -> ExecutionResult:
        from barrow.operations import sort

        return ExecutionResult(sort(table, keys, descending))

    def execute_window(
        self,
        table: pa.Table,
        by: list[str] | None,
        order_by: list[str] | None,
        assignments: dict[str, Expression],
    ) -> ExecutionResult:
        from barrow.operations import window

        return ExecutionResult(window(table, by, order_by, **assignments))

    def execute_join(
        self,
        left: pa.Table,
        right: pa.Table,
        left_on: str,
        right_on: str,
        join_type: str = "inner",
    ) -> ExecutionResult:
        from barrow.operations import join

        return ExecutionResult(join(left, right, left_on, right_on, join_type))

    def execute_groupby(self, table: pa.Table, keys: list[str]) -> ExecutionResult:
        from barrow.operations import groupby

        return ExecutionResult(groupby(table, keys))

    def execute_ungroup(self, table: pa.Table) -> ExecutionResult:
        from barrow.operations import ungroup

        return ExecutionResult(ungroup(table))
