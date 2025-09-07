import pytest

from barrow.errors import InvalidExpressionError
from barrow.expr import parse


def test_parse_invalid_syntax():
    with pytest.raises(SyntaxError):
        parse("a +")


def test_parse_attribute_call_not_supported():
    with pytest.raises(InvalidExpressionError):
        parse("obj.method()")


def test_parse_conditional_expression_not_supported():
    with pytest.raises(InvalidExpressionError):
        parse("a if b else c")

