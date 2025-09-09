from __future__ import annotations

from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.feather as feather
import pyarrow.orc as orc

from ..errors import UnsupportedFormatError


def write_table(
    table: pa.Table,
    path: str | None,
    format: str | None,
    output_delimiter: str | None = None,
) -> None:
    """Write ``table`` to ``path`` or ``STDOUT``.

    Parameters
    ----------
    table:
        Table to serialise.
    path:
        Destination path. When ``None`` the table is written to ``STDOUT``.
    format:
        The file format. Supported values are ``"csv"``, ``"parquet``,
        ``"feather"`` and ``"orc"``. If ``None``, the format is inferred from
        ``path`` when available and otherwise defaults to CSV.
    output_delimiter:
        Field delimiter for CSV outputs. When ``None`` a comma is used.
    """

    fmt = format.lower() if format else None
    if fmt is None and path:
        ext = Path(path).suffix.lower()
        if ext == ".csv":
            fmt = "csv"
        elif ext == ".parquet":
            fmt = "parquet"
        elif ext == ".feather":
            fmt = "feather"
        elif ext == ".orc":
            fmt = "orc"
    if fmt is None and table.schema.metadata:
        fmt_meta = table.schema.metadata.get(b"format")
        if fmt_meta:
            fmt = fmt_meta.decode().lower()
    if fmt is None:
        fmt = "csv"

    if fmt == "csv":
        grouped = (
            table.schema.metadata.get(b"grouped_by") if table.schema.metadata else None
        )
        comment = b"# grouped_by: " + grouped + b"\n" if grouped else None
        delimiter = output_delimiter or ","
        write_options = csv.WriteOptions(delimiter=delimiter)
        if path:
            if comment:
                with open(path, "wb") as f:
                    f.write(comment)
                    csv.write_csv(table, f, write_options=write_options)
            else:
                csv.write_csv(table, path, write_options=write_options)
        else:
            if comment:
                sys.stdout.buffer.write(comment)
            csv.write_csv(table, sys.stdout.buffer, write_options=write_options)
        return
    if fmt == "parquet":
        if path:
            pq.write_table(table, path)
        else:
            pq.write_table(table, sys.stdout.buffer)
        return
    if fmt == "feather":
        if path:
            feather.write_feather(table, path)
        else:
            feather.write_feather(table, sys.stdout.buffer)
        return
    if fmt == "orc":
        if path:
            orc.write_table(table, path)
        else:
            orc.write_table(table, sys.stdout.buffer)
        return
    raise UnsupportedFormatError(f"Unsupported format: {format}")


__all__ = ["write_table"]
