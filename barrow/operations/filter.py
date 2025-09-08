"""Row filtering operation using Python expressions."""

from __future__ import annotations

import logging
import pyarrow as pa

from ..expr import Expression
from ._env import build_env
from ._expr_eval import evaluate_expression


logger = logging.getLogger(__name__)


def filter(table: pa.Table, expression: Expression) -> pa.Table:
    """Filter ``table`` by evaluating ``expression``.

    The expression is evaluated with a namespace containing the table's
    columns and functions from :mod:`numpy` provided by
    :func:`~barrow.operations._env.build_env`.
    """
    logger.debug("Filtering with expression %s", expression)
    env = build_env(table)
    mask = evaluate_expression(expression, env)
    logger.debug("Filter mask length %d", len(mask))
    result = table.filter(pa.array(mask))
    logger.debug("Result has %d rows", result.num_rows)
    return result


__all__ = ["filter"]
