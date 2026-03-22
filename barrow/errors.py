"""Custom exceptions for the barrow project.

This module re-exports the typed error hierarchy from :mod:`barrow.core.errors`
for backward compatibility.  New code should import directly from
``barrow.core.errors`` when possible.
"""

from .core.errors import (
    BarrowError,
    BarrowIOError,
    ExecutionError,
    FrontendError,
    InvalidExpressionError,
    OptimizationError,
    PlanningError,
    UnsupportedFormatError,
)

__all__ = [
    "BarrowError",
    "BarrowIOError",
    "ExecutionError",
    "FrontendError",
    "InvalidExpressionError",
    "OptimizationError",
    "PlanningError",
    "UnsupportedFormatError",
]
