"""Custom exceptions for the barrow project."""


class BarrowError(Exception):
    """Base class for all custom barrow exceptions."""


class InvalidExpressionError(BarrowError):
    """Raised when an expression cannot be parsed or evaluated."""


class UnsupportedFormatError(BarrowError):
    """Raised when an I/O function receives an unknown format."""


__all__ = [
    "BarrowError",
    "InvalidExpressionError",
    "UnsupportedFormatError",
]
