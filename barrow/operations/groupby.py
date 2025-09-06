from __future__ import annotations

"""Grouping utilities."""

import pyarrow as pa


def groupby(table: pa.Table, keys: str | list[str], *, use_threads: bool = True) -> pa.TableGroupBy:
    """Group ``table`` by ``keys``.

    This is a thin wrapper over :meth:`pyarrow.Table.group_by` that exposes the
    ``use_threads`` parameter for deterministic testing.
    """
    return table.group_by(keys, use_threads=use_threads)


__all__ = ["groupby"]
