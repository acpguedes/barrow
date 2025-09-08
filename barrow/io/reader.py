from __future__ import annotations

from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from ..errors import UnsupportedFormatError


def _detect_format(path: str | None, data: bytes | None) -> str:
    """Infer table format from file extension or magic bytes."""

    if path:
        ext = Path(path).suffix.lower()
        if ext == ".csv":
            return "csv"
        if ext == ".parquet":
            return "parquet"
        with open(path, "rb") as f:
            head = f.read(4)
    else:
        head = (data or b"")[:4]
    if head.startswith(b"PAR1"):
        return "parquet"
    return "csv"


def read_table(path: str | None, format: str | None) -> pa.Table:
    """Read a table from ``path`` or ``STDIN``.

    Parameters
    ----------
    path:
        Path to the input file. When ``None`` the data is read from ``STDIN``.
    format:
        The file format. Supported values are ``"csv"`` and ``"parquet"``.
        If ``None``, the format is inferred from ``path`` or the input data.
    """

    data: bytes | None = None
    fmt = format.lower() if format else None
    if fmt is None:
        if path:
            fmt = _detect_format(path, None)
        else:
            data = sys.stdin.buffer.read()
            fmt = _detect_format(None, data)

    if fmt == "csv":
        if path:
            return csv.read_csv(path)
        if data is None:
            data = sys.stdin.buffer.read()
        return csv.read_csv(pa.BufferReader(data))
    if fmt == "parquet":
        if path:
            return pq.read_table(path)
        if data is None:
            data = sys.stdin.buffer.read()
        return pq.read_table(pa.BufferReader(data))
    raise UnsupportedFormatError(f"Unsupported format: {format}")


__all__ = ["read_table"]

