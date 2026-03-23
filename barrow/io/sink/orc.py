"""ORC sink adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.orc as porc


def sink_orc(table: pa.Table, path: str | None = None) -> None:
    """Write a table to ORC format."""
    if path is not None:
        porc.write_table(table, path)
    else:
        porc.write_table(table, pa.PythonFile(sys.stdout.buffer))
