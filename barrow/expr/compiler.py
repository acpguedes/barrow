"""Expression compilation utilities."""

from __future__ import annotations

from barrow.expr.parser import (
    Expression,
    Name,
    Literal,
    UnaryExpression,
    BinaryExpression,
    FunctionCall,
)


def to_sql(expr: Expression) -> str:
    """Compile an expression to a SQL string fragment."""
    if isinstance(expr, Literal):
        if isinstance(expr.value, str):
            return f"'{expr.value}'"
        return str(expr.value)

    if isinstance(expr, Name):
        return f'"{expr.name}"'

    if isinstance(expr, UnaryExpression):
        operand = to_sql(expr.operand)
        if expr.op == "not":
            return f"NOT ({operand})"
        return f"({expr.op}{operand})"

    if isinstance(expr, BinaryExpression):
        left = to_sql(expr.left)
        right = to_sql(expr.right)
        op = expr.op
        if op == "and":
            op = "AND"
        elif op == "or":
            op = "OR"
        elif op == "==":
            op = "="
        elif op == "!=":
            op = "<>"
        return f"({left} {op} {right})"

    if isinstance(expr, FunctionCall):
        args = ", ".join(to_sql(a) for a in expr.args)
        return f"{expr.name}({args})"

    return str(expr)
