from __future__ import annotations

"""Column creation and transformation operation."""

import numpy as np
import pyarrow as pa

from ..expr import Expression
from ._expr_eval import evaluate_expression


def mutate(table: pa.Table, **expressions: Expression) -> pa.Table:
    """Return a new table with columns created or replaced.

    Each keyword argument represents the name of the resulting column and its
    value is a Python :class:`~barrow.expr.Expression` evaluated using the
    existing columns and functions from :mod:`numpy`.
    """
    env: dict[str, object] = {
        name: table[name].to_numpy(zero_copy_only=False) for name in table.column_names
    }
    env.update({name: getattr(np, name) for name in dir(np) if not name.startswith("_")})
    out = table
    for name, expr in expressions.items():
        value = evaluate_expression(expr, env)
        arr = pa.array(value)
        if name in out.column_names:
            idx = out.column_names.index(name)
            out = out.set_column(idx, name, arr)
        else:
            out = out.append_column(name, arr)
        env[name] = value
    return out


__all__ = ["mutate"]
