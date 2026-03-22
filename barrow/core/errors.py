"""Typed error domains for the barrow engine."""

from __future__ import annotations


class BarrowError(Exception):
    """Base class for all barrow exceptions."""


class InvalidExpressionError(BarrowError):
    """Raised when an expression cannot be parsed or evaluated."""


class UnsupportedFormatError(BarrowError):
    """Raised when an I/O function receives an unknown format."""


class ExecutionError(BarrowError):
    """Raised when the execution engine encounters a failure."""


class PlanningError(BarrowError):
    """Raised when plan construction or validation fails."""


class BarrowIOError(BarrowError):
    """Raised for I/O-specific errors."""


class FrontendError(BarrowError):
    """Raised when a frontend fails to parse user input into a plan."""


class OptimizationError(BarrowError):
    """Raised when an optimizer rule encounters an error."""


__all__ = [
    "BarrowError",
    "InvalidExpressionError",
    "UnsupportedFormatError",
    "ExecutionError",
    "PlanningError",
    "BarrowIOError",
    "FrontendError",
    "OptimizationError",
]
