"""I/O utilities for barrow.

This module contains helper functions to read and write tabular data
using Apache Arrow. It provides a small abstraction over the CSV and
Parquet readers so the rest of the project can operate on in-memory
Arrow tables.
"""

from __future__ import annotations

import sys
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


def read_table(path: str | None) -> pa.Table:
    """Read a CSV file from ``path`` or from ``STDIN`` when ``path`` is ``None``."""

    if path:
        return csv.read_csv(path)
    data = sys.stdin.buffer.read()
    return csv.read_csv(pa.BufferReader(data))


def write_table(table: pa.Table, path: str | None) -> None:
    """Write ``table`` to ``path`` as Parquet or emit CSV to ``STDOUT``.

    When ``path`` is ``None`` the table is written as CSV to ``STDOUT``.
    Otherwise a Parquet file is created at the given path.
    """

    if path:
        pq.write_table(table, path)
    else:
        csv.write_csv(table, sys.stdout.buffer)


__all__ = ["read_table", "write_table"]

