"""CSV scan adapter."""

from __future__ import annotations

import csv
import io
import sys

import pyarrow as pa
import pyarrow.csv as pcsv


def scan_csv(
    path: str | None = None,
    delimiter: str | None = None,
) -> tuple[pa.Table, dict[str, str]]:
    """Read a CSV file and return (table, metadata).

    metadata may contain 'grouped_by' if a comment header is found.
    """
    metadata: dict[str, str] = {}

    if path is not None:
        with open(path, "r") as f:
            raw = f.read()
    else:
        raw = sys.stdin.buffer.read().decode("utf-8")

    lines = raw.split("\n")
    data_lines = []
    for line in lines:
        if line.startswith("# grouped_by:"):
            metadata["grouped_by"] = line.split(":", 1)[1].strip()
        else:
            data_lines.append(line)

    content = "\n".join(data_lines)

    if delimiter is None:
        try:
            sample = content[:8192]
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","

    read_opts = pcsv.ReadOptions()
    parse_opts = pcsv.ParseOptions(delimiter=delimiter)
    table = pcsv.read_csv(
        io.BytesIO(content.encode("utf-8")),
        read_options=read_opts,
        parse_options=parse_opts,
    )

    return table, metadata
