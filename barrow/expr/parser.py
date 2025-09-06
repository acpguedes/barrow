from __future__ import annotations

"""Parse simple expressions into an abstract syntax tree.

The parser translates strings like ``"age > 30"`` into expression objects
that can later be evaluated given a mapping of variable names to values.  Only
a small subset of Python expressions is supported including arithmetic,
comparisons, boolean operations and calls to a restricted set of functions.
"""

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence
import ast
import math
import operator

from ..errors import InvalidExpressionError


class Expression:
    """Base class for all expression nodes."""

    def evaluate(self, env: Mapping[str, Any]) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError


@dataclass(frozen=True)
class Literal(Expression):
    value: Any

    def evaluate(self, env: Mapping[str, Any]) -> Any:
        return self.value


@dataclass(frozen=True)
class Name(Expression):
    identifier: str

    def evaluate(self, env: Mapping[str, Any]) -> Any:
        return env[self.identifier]


@dataclass(frozen=True)
class UnaryExpression(Expression):
    op: str
    operand: Expression

    def evaluate(self, env: Mapping[str, Any]) -> Any:
        func = _UNARY_OPERATORS[self.op]
        return func(self.operand.evaluate(env))


@dataclass(frozen=True)
class BinaryExpression(Expression):
    left: Expression
    op: str
    right: Expression

    def evaluate(self, env: Mapping[str, Any]) -> Any:
        if self.op == "and":
            return self.left.evaluate(env) and self.right.evaluate(env)
        if self.op == "or":
            return self.left.evaluate(env) or self.right.evaluate(env)
        func = _BINARY_OPERATORS[self.op]
        return func(self.left.evaluate(env), self.right.evaluate(env))


@dataclass(frozen=True)
class FunctionCall(Expression):
    name: str
    args: Sequence[Expression]

    def evaluate(self, env: Mapping[str, Any]) -> Any:
        func = env.get(self.name, _FUNCTIONS.get(self.name))
        if func is None:
            raise NameError(self.name)
        return func(*(arg.evaluate(env) for arg in self.args))


# Mapping of supported operators to their implementations
_BINARY_OPERATORS: dict[str, Callable[[Any, Any], Any]] = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "%": operator.mod,
    "**": operator.pow,
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}

_UNARY_OPERATORS: dict[str, Callable[[Any], Any]] = {
    "+": operator.pos,
    "-": operator.neg,
    "not": operator.not_,
}

_FUNCTIONS: dict[str, Callable[..., Any]] = {
    name: getattr(math, name) for name in dir(math) if not name.startswith("_")
}
_FUNCTIONS.update({"abs": abs, "max": max, "min": min})


def parse(expression: str) -> Expression:
    """Parse ``expression`` into an :class:`Expression` tree."""

    tree = ast.parse(expression, mode="eval")
    return _convert(tree.body)


def _convert(node: ast.AST) -> Expression:
    if isinstance(node, ast.BinOp):
        op_map = {
            ast.Add: "+",
            ast.Sub: "-",
            ast.Mult: "*",
            ast.Div: "/",
            ast.Mod: "%",
            ast.Pow: "**",
        }
        return BinaryExpression(_convert(node.left), op_map[type(node.op)], _convert(node.right))
    if isinstance(node, ast.UnaryOp):
        op_map = {ast.Not: "not", ast.USub: "-", ast.UAdd: "+"}
        return UnaryExpression(op_map[type(node.op)], _convert(node.operand))
    if isinstance(node, ast.BoolOp):
        op_map = {ast.And: "and", ast.Or: "or"}
        expr = _convert(node.values[0])
        for value in node.values[1:]:
            expr = BinaryExpression(expr, op_map[type(node.op)], _convert(value))
        return expr
    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise InvalidExpressionError("Chained comparisons are not supported")
        op_map = {
            ast.Eq: "==",
            ast.NotEq: "!=",
            ast.Lt: "<",
            ast.LtE: "<=",
            ast.Gt: ">",
            ast.GtE: ">=",
        }
        return BinaryExpression(_convert(node.left), op_map[type(node.ops[0])], _convert(node.comparators[0]))
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise InvalidExpressionError("Only simple function calls are supported")
        return FunctionCall(node.func.id, [_convert(arg) for arg in node.args])
    if isinstance(node, ast.Name):
        return Name(node.id)
    if isinstance(node, ast.Constant):
        return Literal(node.value)
    raise InvalidExpressionError(f"Unsupported expression: {ast.dump(node)}")


__all__ = [
    "Expression",
    "Literal",
    "Name",
    "UnaryExpression",
    "BinaryExpression",
    "FunctionCall",
    "parse",
]
