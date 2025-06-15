#!/usr/bin/env python3
"""Minimal command line interface for barrow."""

import argparse
import sys
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa


def read_table(path: str | None) -> pa.Table:
    """Read a CSV file from the given path or STDIN if path is None."""
    if path:
        return csv.read_csv(path)
    data = sys.stdin.buffer.read()
    return csv.read_csv(pa.BufferReader(data))


def write_table(table: pa.Table, path: str | None) -> None:
    """Write the table to the given path as parquet or to STDOUT as CSV."""
    if path:
        pq.write_table(table, path)
    else:
        csv.write_csv(table, sys.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="barrow: simple data tool")
    parser.add_argument("--input", "-i", help="Input CSV file. Reads STDIN if omitted.")
    parser.add_argument("--output", "-o", help="Output parquet file. Writes CSV to STDOUT if omitted.")
    args = parser.parse_args(argv)

    table = read_table(args.input)
    write_table(table, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
