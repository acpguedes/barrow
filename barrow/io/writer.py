from __future__ import annotations

from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from ..errors import UnsupportedFormatError


def write_table(table: pa.Table, path: str | None, format: str | None) -> None:
    """Write ``table`` to ``path`` or ``STDOUT``.

    Parameters
    ----------
    table:
        Table to serialise.
    path:
        Destination path. When ``None`` the table is written to ``STDOUT``.
    format:
        The file format. Supported values are ``"csv"`` and ``"parquet"``.
        If ``None``, the format is inferred from ``path`` when available and
        otherwise defaults to CSV.
    """

    fmt = format.lower() if format else None
    if fmt is None and path:
        ext = Path(path).suffix.lower()
        if ext == ".csv":
            fmt = "csv"
        elif ext == ".parquet":
            fmt = "parquet"
    if fmt is None:
        fmt = "csv"

    if fmt == "csv":
        grouped = (
            table.schema.metadata.get(b"grouped_by")
            if table.schema.metadata
            else None
        )
        comment = b"# grouped_by: " + grouped + b"\n" if grouped else None
        if path:
            if comment:
                with open(path, "wb") as f:
                    f.write(comment)
                    csv.write_csv(table, f)
            else:
                csv.write_csv(table, path)
        else:
            if comment:
                sys.stdout.buffer.write(comment)
            csv.write_csv(table, sys.stdout.buffer)
        return
    if fmt == "parquet":
        if path:
            pq.write_table(table, path)
        else:
            pq.write_table(table, sys.stdout.buffer)
        return
    raise UnsupportedFormatError(f"Unsupported format: {format}")


__all__ = ["write_table"]
