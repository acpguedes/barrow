"""Parse simple expressions into an abstract syntax tree.

The parser translates strings like ``"age > 30"`` into expression objects
that can later be evaluated given a mapping of variable names to values.  Only
a small subset of Python expressions is supported including arithmetic,
comparisons, boolean operations and calls to a restricted set of functions.
"""

from __future__ import annotations

import ast
import math
import operator
import re
import tokenize
from dataclasses import dataclass
from io import StringIO
from typing import Any, Callable, Mapping, Sequence, cast

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
    "in": lambda a, b: a in b,
    "not in": lambda a, b: a not in b,
    "like": lambda a, b: _like(a, b),
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


def _like(value: str, pattern: str) -> bool:
    """Simple SQL-like pattern matching."""
    regex = "^" + re.escape(pattern).replace("%", ".*").replace("_", ".") + "$"
    return re.match(regex, value) is not None


def _replace_like_tokens(expression: str) -> str:
    tokens = tokenize.generate_tokens(StringIO(expression).readline)
    new_tokens = []
    for tok in tokens:
        if tok.type == tokenize.NAME and tok.string == "like":
            new_tokens.append(
                tokenize.TokenInfo(tokenize.OP, "@", tok.start, tok.end, tok.line)
            )
        else:
            new_tokens.append(tok)
    return tokenize.untokenize(new_tokens)


def parse(expression: str) -> Expression:
    """Parse ``expression`` into an :class:`Expression` tree."""

    expression = _replace_like_tokens(expression)
    tree = ast.parse(expression, mode="eval")
    return _convert(tree.body)


def _convert(node: ast.AST) -> Expression:
    if isinstance(node, ast.BinOp):
        op_map_bin: dict[type[ast.operator], str] = {
            ast.Add: "+",
            ast.Sub: "-",
            ast.Mult: "*",
            ast.Div: "/",
            ast.Mod: "%",
            ast.Pow: "**",
            ast.MatMult: "like",
        }
        return BinaryExpression(
            _convert(node.left), op_map_bin[type(node.op)], _convert(node.right)
        )
    if isinstance(node, ast.UnaryOp):
        op_map_unary: dict[type[ast.unaryop], str] = {
            ast.Not: "not",
            ast.USub: "-",
            ast.UAdd: "+",
        }
        return UnaryExpression(op_map_unary[type(node.op)], _convert(node.operand))
    if isinstance(node, ast.BoolOp):
        op_map_bool: dict[type[ast.boolop], str] = {ast.And: "and", ast.Or: "or"}
        expr = _convert(node.values[0])
        for value in node.values[1:]:
            expr = BinaryExpression(expr, op_map_bool[type(node.op)], _convert(value))
        return expr
    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise InvalidExpressionError("Chained comparisons are not supported")
        op_map_cmp: dict[type[ast.cmpop], str] = {
            ast.Eq: "==",
            ast.NotEq: "!=",
            ast.Lt: "<",
            ast.LtE: "<=",
            ast.Gt: ">",
            ast.GtE: ">=",
            ast.In: "in",
            ast.NotIn: "not in",
        }
        return BinaryExpression(
            _convert(node.left),
            op_map_cmp[type(node.ops[0])],
            _convert(node.comparators[0]),
        )
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise InvalidExpressionError("Only simple function calls are supported")
        return FunctionCall(node.func.id, [_convert(arg) for arg in node.args])
    if isinstance(node, ast.Name):
        return Name(node.id)
    if isinstance(node, ast.Constant):
        return Literal(node.value)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        elements = [_convert(e) for e in node.elts]
        if not all(isinstance(e, Literal) for e in elements):
            raise InvalidExpressionError("Only literal sequences are supported")
        lit_elements = cast(list[Literal], elements)
        values = [e.value for e in lit_elements]
        if isinstance(node, ast.List):
            return Literal(values)
        if isinstance(node, ast.Tuple):
            return Literal(tuple(values))
        return Literal(set(values))
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
