from __future__ import annotations

"""Window functions for :mod:`barrow`."""

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ..expr import Expression
from ._expr_eval import evaluate_expression


def window(
    table: pa.Table,
    by: list[str] | None,
    order_by: list[str] | None,
    **expressions: Expression,
) -> pa.Table:
    """Evaluate window ``expressions`` over ``table``.

    Parameters
    ----------
    table:
        Input table.
    by:
        Columns used to partition the data.  ``None`` means no partitioning.
    order_by:
        Columns used to order the data inside each partition. ``None`` means
        the input order is used.
    expressions:
        Mapping of output column names to parsed :class:`~barrow.expr.Expression`
        objects describing the windowed computation to perform.
    """
    if not expressions:
        return table

    sort_keys: list[tuple[str, str]] = []
    if by:
        sort_keys.extend((col, "ascending") for col in by)
    if order_by:
        sort_keys.extend((col, "ascending") for col in order_by)

    if sort_keys:
        sort_idx = pc.sort_indices(table, sort_keys=sort_keys)
        sorted_table = table.take(sort_idx)
    else:
        sort_idx = pa.array(np.arange(table.num_rows))
        sorted_table = table

    # Build inverse permutation to restore original order
    sort_idx_np = sort_idx.to_numpy(zero_copy_only=False)
    inv_np = np.empty_like(sort_idx_np)
    inv_np[sort_idx_np] = np.arange(len(sort_idx_np))
    inv_idx = pa.array(inv_np)

    n = sorted_table.num_rows
    # Determine partition boundaries in the sorted table
    if by:
        by_lists = [sorted_table[col].to_pylist() for col in by]
        offsets = [0]
        for i in range(1, n):
            prev = tuple(lst[i - 1] for lst in by_lists)
            cur = tuple(lst[i] for lst in by_lists)
            if cur != prev:
                offsets.append(i)
        offsets.append(n)
    else:
        offsets = [0, n]

    # Environment for expression evaluation (sorted order)
    env: dict[str, pa.Array] = {name: sorted_table[name] for name in sorted_table.column_names}

    def row_number() -> pa.Array:
        out = np.empty(n, dtype=np.int64)
        for start, stop in zip(offsets[:-1], offsets[1:]):
            out[start:stop] = np.arange(1, stop - start + 1)
        return pa.array(out)

    def rolling_sum(col: pa.Array | str, window: int) -> pa.Array:
        arr = env[col] if isinstance(col, str) else pa.array(col)
        out = np.empty(n, dtype=np.float64)
        for start, stop in zip(offsets[:-1], offsets[1:]):
            seg = arr.slice(start, stop - start)
            csum = pc.cumulative_sum(seg).to_numpy(zero_copy_only=False)
            res = csum.copy()
            if window < len(res):
                res[window:] = csum[window:] - csum[:-window]
            out[start:stop] = res
        return pa.array(out)

    def rolling_mean(col: pa.Array | str, window: int) -> pa.Array:
        rs = rolling_sum(col, window).to_numpy(zero_copy_only=False)
        denom = np.empty(n, dtype=np.float64)
        for start, stop in zip(offsets[:-1], offsets[1:]):
            length = stop - start
            denom[start:stop] = np.minimum(np.arange(1, length + 1), window)
        return pa.array(rs / denom)

    env.update({
        name: getattr(pc, name) for name in dir(pc) if not name.startswith("_")
    })
    env.update({
        "row_number": row_number,
        "rolling_sum": rolling_sum,
        "rolling_mean": rolling_mean,
    })

    out = table
    for name, expr in expressions.items():
        value = evaluate_expression(expr, env)
        arr = value if isinstance(value, pa.Array) else pa.array(value)
        arr = arr.take(inv_idx)
        if name in out.column_names:
            idx = out.column_names.index(name)
            out = out.set_column(idx, name, arr)
        else:
            out = out.append_column(name, arr)
        env[name] = value

    return out


__all__ = ["window"]
