"""Column selection operation."""

from __future__ import annotations

import pyarrow as pa


def select(table: pa.Table, columns: str | list[str] | tuple[str, ...]) -> pa.Table:
    """Return a table with only *columns*.

    Parameters
    ----------
    table:
        Input table.
    columns:
        Column name or names to keep.
    """
    if isinstance(columns, (str, bytes)):
        cols = [columns]
    else:
        cols = list(columns)
    return table.select(cols)


__all__ = ["select"]
