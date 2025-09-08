"""Table join operation."""

from __future__ import annotations

import pyarrow as pa


def join(
    left: pa.Table,
    right: pa.Table,
    left_on: str,
    right_on: str,
    join_type: str = "inner",
) -> pa.Table:
    """Join ``left`` and ``right`` on the specified keys.

    Parameters
    ----------
    left:
        Left table.
    right:
        Right table.
    left_on:
        Join key in ``left``.
    right_on:
        Join key in ``right``.
    join_type:
        Type of join to perform. Defaults to ``"inner"``.
    """
    if left_on not in left.column_names:
        raise KeyError(left_on)
    if right_on not in right.column_names:
        raise KeyError(right_on)
    overlap = (set(left.column_names) & set(right.column_names)) - {left_on, right_on}
    if overlap:
        return left.join(
            right,
            keys=left_on,
            right_keys=right_on,
            join_type=join_type,
            right_suffix="_right",
        )
    return left.join(right, keys=left_on, right_keys=right_on, join_type=join_type)


__all__ = ["join"]
