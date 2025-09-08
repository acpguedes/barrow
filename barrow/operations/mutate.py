"""Column creation and transformation operation."""

from __future__ import annotations

import logging
import pyarrow as pa

from ..expr import Expression
from ._env import build_env
from ._expr_eval import evaluate_expression


logger = logging.getLogger(__name__)


def mutate(table: pa.Table, **expressions: Expression) -> pa.Table:
    """Return a new table with columns created or replaced.

    Each keyword argument represents the name of the resulting column and its
    value is a Python :class:`~barrow.expr.Expression` evaluated using the
    existing columns and functions from :mod:`numpy` provided by
    :func:`~barrow.operations._env.build_env`.
    """
    logger.debug("Mutating with expressions: %s", list(expressions.keys()))
    env = build_env(table)
    out = table
    for name, expr in expressions.items():
        logger.debug("Evaluating expression for column '%s'", name)
        value = evaluate_expression(expr, env)
        arr = pa.array(value)
        if name in out.column_names:
            idx = out.column_names.index(name)
            out = out.set_column(idx, name, arr)
        else:
            out = out.append_column(name, arr)
        env[name] = value
        logger.debug(
            "Column '%s' added/replaced, total columns now %d", name, out.num_columns
        )
    return out


__all__ = ["mutate"]
