"""Expression analysis: name resolution and basic validation."""

from __future__ import annotations

from barrow.expr.parser import (
    Expression,
    Name,
    Literal,
    UnaryExpression,
    BinaryExpression,
    FunctionCall,
)


def referenced_names(expr: Expression) -> set[str]:
    """Return all column/variable names referenced by an expression."""
    names: set[str] = set()
    _collect_names(expr, names)
    return names


def _collect_names(expr: Expression, acc: set[str]) -> None:
    if isinstance(expr, Name):
        acc.add(expr.identifier)
    elif isinstance(expr, Literal):
        pass
    elif isinstance(expr, UnaryExpression):
        _collect_names(expr.operand, acc)
    elif isinstance(expr, BinaryExpression):
        _collect_names(expr.left, acc)
        _collect_names(expr.right, acc)
    elif isinstance(expr, FunctionCall):
        for arg in expr.args:
            _collect_names(arg, acc)


def validate_expression(expr: Expression, available_columns: set[str]) -> list[str]:
    """Return a list of column names referenced but not available."""
    refs = referenced_names(expr)
    return sorted(refs - available_columns)
