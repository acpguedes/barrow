"""CSV sink adapter."""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.csv as pcsv


def sink_csv(
    table: pa.Table,
    path: str | None = None,
    delimiter: str | None = None,
    grouped_by: str | None = None,
) -> None:
    """Write a table to CSV format."""
    delimiter = delimiter or ","
    write_opts = pcsv.WriteOptions(delimiter=delimiter)

    if path is not None:
        with open(path, "wb") as f:
            if grouped_by:
                f.write(f"# grouped_by: {grouped_by}\n".encode("utf-8"))
            pcsv.write_csv(table, f, write_options=write_opts)
    else:
        buf = pa.BufferOutputStream()
        pcsv.write_csv(table, buf, write_options=write_opts)
        header = b""
        if grouped_by:
            header = f"# grouped_by: {grouped_by}\n".encode("utf-8")
        sys.stdout.buffer.write(header + buf.getvalue().to_pybytes())
