from __future__ import annotations

"""Column creation and transformation operation."""

import pyarrow as pa

from ..expr import Expression
from ._env import build_env
from ._expr_eval import evaluate_expression


def mutate(table: pa.Table, **expressions: Expression) -> pa.Table:
    """Return a new table with columns created or replaced.

    Each keyword argument represents the name of the resulting column and its
    value is a Python :class:`~barrow.expr.Expression` evaluated using the
    existing columns and functions from :mod:`numpy` provided by
    :func:`~barrow.operations._env.build_env`.
    """
    env = build_env(table)
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
