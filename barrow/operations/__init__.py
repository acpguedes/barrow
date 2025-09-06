"""Data operations for :mod:`barrow`.

This package contains transformation primitives used by the command line
interface, such as selecting columns, filtering rows and performing
aggregations.
"""

from .select import select
from .filter import filter
from .mutate import mutate
from .groupby import groupby
from .summary import summary

__all__ = ["select", "filter", "mutate", "groupby", "summary"]

