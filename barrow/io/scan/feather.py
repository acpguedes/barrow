"""Feather scan adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.feather as pf


def scan_feather(path: str | None = None) -> tuple[pa.Table, dict[str, str]]:
    """Read a Feather/IPC file."""
    if path is not None:
        table = pf.read_table(path)
    else:
        table = pf.read_table(pa.PythonFile(sys.stdin.buffer))
    return table, {}
