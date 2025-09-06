#!/usr/bin/env python3
"""Command line interface for barrow."""

from __future__ import annotations

import argparse
import sys

from .errors import BarrowError
from .io import read_table, write_table


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``barrow`` command line tool."""

    parser = argparse.ArgumentParser(description="barrow: simple data tool")
    parser.add_argument("--input", "-i", help="Input CSV file. Reads STDIN if omitted.")
    parser.add_argument("--output", "-o", help="Output parquet file. Writes CSV to STDOUT if omitted.")
    args = parser.parse_args(argv)

    try:
        table = read_table(args.input, "csv")
        write_table(table, args.output, "parquet")
    except BarrowError as exc:  # pragma: no cover - error path
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

