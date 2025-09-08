import math
import pytest

from barrow.errors import InvalidExpressionError
from barrow.expr.parser import (
    BinaryExpression,
    FunctionCall,
    Literal,
    Name,
    UnaryExpression,
    parse,
)


def test_parse_simple_comparison():
    expr = parse("age > 30")
    expected = BinaryExpression(Name("age"), ">", Literal(30))
    assert expr == expected
    assert expr.evaluate({"age": 40}) is True
    assert expr.evaluate({"age": 20}) is False


def test_arithmetic_precedence():
    expr = parse("a + b * 2")
    expected = BinaryExpression(
        Name("a"),
        "+",
        BinaryExpression(Name("b"), "*", Literal(2)),
    )
    assert expr == expected
    assert expr.evaluate({"a": 1, "b": 3}) == 7


def test_logical_and_function_calls():
    expr = parse("not active or max(a, b) > 3")
    expected = BinaryExpression(
        UnaryExpression("not", Name("active")),
        "or",
        BinaryExpression(
            FunctionCall("max", [Name("a"), Name("b")]), ">", Literal(3)
        ),
    )
    assert expr == expected
    assert expr.evaluate({"active": False, "a": 1, "b": 2}) is True
    assert expr.evaluate({"active": True, "a": 1, "b": 2}) is False


def test_chained_comparisons_not_supported():
    with pytest.raises(InvalidExpressionError):
        parse("a < b < c")


def test_membership_operators():
    expr = parse("age in [20, 30]")
    expected = BinaryExpression(Name("age"), "in", Literal([20, 30]))
    assert expr == expected
    assert expr.evaluate({"age": 20}) is True
    assert expr.evaluate({"age": 25}) is False

    expr = parse("country not in ['US', 'CA']")
    expected = BinaryExpression(Name("country"), "not in", Literal(['US', 'CA']))
    assert expr == expected
    assert expr.evaluate({"country": 'US'}) is False
    assert expr.evaluate({"country": 'FR'}) is True


def test_like_operator():
    expr = parse("name like 'Jo%'")
    expected = BinaryExpression(Name("name"), "like", Literal("Jo%"))
    assert expr == expected
    assert expr.evaluate({"name": "John"}) is True
    assert expr.evaluate({"name": "Bob"}) is False
