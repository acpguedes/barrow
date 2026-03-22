"""Feather sink adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.feather as pf


def sink_feather(table: pa.Table, path: str | None = None) -> None:
    """Write a table to Feather/IPC format."""
    if path is not None:
        pf.write_feather(table, path)
    else:
        pf.write_feather(table, pa.PythonFile(sys.stdout.buffer))
