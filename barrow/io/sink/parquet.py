"""Parquet sink adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.parquet as pq


def sink_parquet(table: pa.Table, path: str | None = None) -> None:
    """Write a table to Parquet format."""
    if path is not None:
        pq.write_table(table, path)
    else:
        pq.write_table(table, pa.PythonFile(sys.stdout.buffer))
