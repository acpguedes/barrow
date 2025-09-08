import pytest

from barrow.errors import InvalidExpressionError
from barrow.expr import parse
from barrow.operations._expr_eval import evaluate_expression


def test_parse_invalid_syntax():
    with pytest.raises(SyntaxError) as excinfo:
        parse("a +")
    assert "invalid syntax" in str(excinfo.value)


def test_parse_attribute_call_not_supported():
    with pytest.raises(InvalidExpressionError) as excinfo:
        parse("obj.method()")
    assert "Only simple function calls are supported" in str(excinfo.value)


def test_parse_conditional_expression_not_supported():
    with pytest.raises(InvalidExpressionError) as excinfo:
        parse("a if b else c")
    assert "Unsupported expression" in str(excinfo.value)


def test_missing_variable_name_in_error():
    expr = parse("a + 1")
    with pytest.raises(NameError) as excinfo:
        evaluate_expression(expr, {})
    assert "name 'a' is not defined" in str(excinfo.value)


def test_missing_function_name_in_error():
    expr = parse("unknown_func(1)")
    with pytest.raises(NameError) as excinfo:
        evaluate_expression(expr, {})
    assert "name 'unknown_func' is not defined" in str(excinfo.value)

