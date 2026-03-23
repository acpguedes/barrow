"""ORC scan adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.orc as porc


def scan_orc(path: str | None = None) -> tuple[pa.Table, dict[str, str]]:
    """Read an ORC file."""
    if path is not None:
        table = porc.read_table(path)
    else:
        table = porc.read_table(pa.PythonFile(sys.stdin.buffer))
    return table, {}
