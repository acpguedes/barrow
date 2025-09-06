from __future__ import annotations

import sys
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from ..errors import UnsupportedFormatError


def write_table(table: pa.Table, path: str | None, format: str) -> None:
    """Write ``table`` to ``path`` or ``STDOUT``.

    Parameters
    ----------
    table:
        Table to serialise.
    path:
        Destination path. When ``None`` the table is written to ``STDOUT``.
    format:
        The file format. Supported values are ``"csv"`` and ``"parquet"``.
    """
    fmt = format.lower()
    if fmt == "csv":
        if path:
            csv.write_csv(table, path)
        else:
            csv.write_csv(table, sys.stdout.buffer)
        return
    if fmt == "parquet":
        if path:
            pq.write_table(table, path)
        else:
            pq.write_table(table, sys.stdout.buffer)
        return
    raise UnsupportedFormatError(f"Unsupported format: {format}")
