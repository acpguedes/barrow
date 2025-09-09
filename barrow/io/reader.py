from __future__ import annotations

from pathlib import Path
import sys

import csv as stdcsv
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


def read_table(
    path: str | None,
    format: str | None,
    input_delimiter: str | None = None,
) -> pa.Table:
    """Read a table from ``path`` or ``STDIN``.

    Parameters
    ----------
    path:
        Path to the input file. When ``None`` the data is read from ``STDIN``.
    format:
        The file format. Supported values are ``"csv"`` and ``"parquet"``.
        If ``None``, the format is inferred from ``path`` or the input data.
    input_delimiter:
        Field delimiter for CSV inputs. When ``None`` the delimiter is guessed
        from the data using :class:`csv.Sniffer`.
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
        metadata: dict[bytes, bytes] = {b"format": fmt.encode()}
        prefix = b"# grouped_by:"
        delimiter = input_delimiter
        if path:
            with open(path, "rb") as f:
                first = f.readline()
                sample = f.read(1024)
            if first.startswith(prefix):
                sample_bytes = sample
            else:
                sample_bytes = first + sample
            if delimiter is None:
                try:
                    delimiter = stdcsv.Sniffer().sniff(sample_bytes.decode()).delimiter
                except Exception:
                    delimiter = ","
            parse_options = csv.ParseOptions(delimiter=delimiter)
            with open(path, "rb") as f:
                first = f.readline()
                if first.startswith(prefix):
                    metadata[b"grouped_by"] = first[len(prefix) :].strip()
                else:
                    f.seek(0)
                table = csv.read_csv(f, parse_options=parse_options)
        else:
            if data is None:
                data = sys.stdin.buffer.read()
            if data.startswith(prefix):
                newline = data.find(b"\n")
                grouped = data[len(prefix) : newline].strip() if newline != -1 else b""
                metadata[b"grouped_by"] = grouped
                data = data[newline + 1 :] if newline != -1 else b""
            if delimiter is None:
                sample = data[:1024]
                try:
                    delimiter = stdcsv.Sniffer().sniff(sample.decode()).delimiter
                except Exception:
                    delimiter = ","
            parse_options = csv.ParseOptions(delimiter=delimiter)
            table = csv.read_csv(pa.BufferReader(data), parse_options=parse_options)
        if metadata:
            table = table.replace_schema_metadata(
                dict(table.schema.metadata or {}) | metadata
            )
        return table
    if fmt == "parquet":
        if path:
            table = pq.read_table(path)
        else:
            if data is None:
                data = sys.stdin.buffer.read()
            table = pq.read_table(pa.BufferReader(data))
        table = table.replace_schema_metadata(
            dict(table.schema.metadata or {}) | {b"format": fmt.encode()}
        )
        return table
    raise UnsupportedFormatError(f"Unsupported format: {format}")


__all__ = ["read_table"]
