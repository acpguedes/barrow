#!/usr/bin/env python3
"""Command line interface for barrow."""

from __future__ import annotations

import argparse
import sys

from .errors import BarrowError
from .expr import parse
from .io import read_table, write_table
from .operations import filter as op_filter, select as op_select


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``barrow`` command line tool."""

    parser = argparse.ArgumentParser(description="barrow: simple data tool")
    parser.add_argument("--input", "-i", help="Input CSV file. Reads STDIN if omitted.")
    parser.add_argument(
        "--output",
        "-o",
        help="Output parquet file. Writes CSV to STDOUT if omitted.",
    )
    args, rest = parser.parse_known_args(argv)

    try:
        table = read_table(args.input, "csv")

        idx = 0
        while idx < len(rest):
            op = rest[idx]
            idx += 1
            if op == "filter":
                if idx >= len(rest):
                    raise BarrowError("filter requires an expression")
                expr = rest[idx]
                idx += 1
                parse(expr)
                table = op_filter(table, expr)
            elif op == "select":
                if idx >= len(rest):
                    raise BarrowError("select requires column names")
                cols_arg = rest[idx]
                idx += 1
                cols = [c.strip() for c in cols_arg.split(",") if c.strip()]
                table = op_select(table, cols)
            else:  # pragma: no cover - defensive
                raise BarrowError(f"Unknown operation: {op}")

        write_table(table, args.output, "parquet")
    except BarrowError as exc:  # pragma: no cover - error path
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

