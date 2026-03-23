"""Parquet scan adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.parquet as pq


def scan_parquet(path: str | None = None) -> tuple[pa.Table, dict[str, str]]:
    """Read a Parquet file."""
    if path is not None:
        table = pq.read_table(path)
    else:
        table = pq.read_table(pa.PythonFile(sys.stdin.buffer))
    return table, {}
