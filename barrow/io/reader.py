from __future__ import annotations

import sys
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


def read_table(path: str | None, format: str) -> pa.Table:
    """Read a table from ``path`` or ``STDIN``.

    Parameters
    ----------
    path:
        Path to the input file. When ``None`` the data is read from ``STDIN``.
    format:
        The file format. Supported values are ``"csv"`` and ``"parquet"``.
    """
    fmt = format.lower()
    if fmt == "csv":
        if path:
            return csv.read_csv(path)
        data = sys.stdin.buffer.read()
        return csv.read_csv(pa.BufferReader(data))
    if fmt == "parquet":
        if path:
            return pq.read_table(path)
        data = sys.stdin.buffer.read()
        return pq.read_table(pa.BufferReader(data))
    raise ValueError(f"Unsupported format: {format}")
