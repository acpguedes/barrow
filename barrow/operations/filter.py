from __future__ import annotations

"""Row filtering operation using Python expressions."""

import pyarrow as pa

from ..expr import Expression
from ._env import build_env
from ._expr_eval import evaluate_expression


def filter(table: pa.Table, expression: Expression) -> pa.Table:
    """Filter ``table`` by evaluating ``expression``.

    The expression is evaluated with a namespace containing the table's
    columns and functions from :mod:`numpy` provided by
    :func:`~barrow.operations._env.build_env`.
    """
    env = build_env(table)
    mask = evaluate_expression(expression, env)
    return table.filter(pa.array(mask))


__all__ = ["filter"]
