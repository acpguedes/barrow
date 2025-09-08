from __future__ import annotations

"""Utilities for evaluating parsed expressions."""

from typing import Any, Mapping

from ..expr import Expression


def evaluate_expression(expression: Expression, env: Mapping[str, Any]) -> Any:
    """Evaluate *expression* using the provided *env* mapping.

    Parameters
    ----------
    expression:
        A parsed :class:`~barrow.expr.Expression` instance.
    env:
        Mapping of variable names to values and functions available during
        evaluation.

    Returns
    -------
    Any
        The result of the evaluation.

    Raises
    ------
    NameError
        If a referenced name is not found in ``env``.
    """
    try:
        return expression.evaluate(env)
    except (KeyError, NameError) as exc:  # pragma: no cover - exercised in tests
        # ``KeyError`` occurs for missing variables while a ``NameError``
        # can be raised for missing functions.  Include the missing name
        # in the error message for clarity.
        missing = exc.args[0]
        raise NameError(f"name '{missing}' is not defined") from None
