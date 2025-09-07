#!/usr/bin/env python3
"""Command line interface for barrow."""

from __future__ import annotations

import argparse
import sys
import pyarrow as pa

from .errors import BarrowError
from .expr import parse
from .io import read_table, write_table
from .operations import (
    filter as op_filter,
    select as op_select,
    mutate as op_mutate,
    groupby as op_groupby,
    summary as op_summary,
)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``barrow`` command line tool."""

    parser = argparse.ArgumentParser(description="barrow: simple data tool")
    parser.add_argument("--input", "-i", help="Input file. Reads STDIN if omitted.")
    parser.add_argument("--input-format", choices=["csv", "parquet"], default="csv")
    parser.add_argument(
        "--output",
        "-o",
        help="Output file. Writes to STDOUT if omitted.",
    )
    parser.add_argument("--output-format", choices=["csv", "parquet"], default="parquet")
    args, rest = parser.parse_known_args(argv)

    try:
        table = read_table(args.input, args.input_format)
        grouped: pa.TableGroupBy | None = None

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
            elif op == "mutate":
                if idx >= len(rest):
                    raise BarrowError("mutate requires column assignments")
                assigns = rest[idx]
                idx += 1
                pairs = [p.strip() for p in assigns.split(",") if p.strip()]
                expressions: dict[str, str] = {}
                for pair in pairs:
                    if "=" not in pair:
                        raise BarrowError("mutate arguments must be NAME=EXPR")
                    name, expr = pair.split("=", 1)
                    expressions[name.strip()] = expr.strip()
                table = op_mutate(table, **expressions)
            elif op == "groupby":
                if idx >= len(rest):
                    raise BarrowError("groupby requires column names")
                cols_arg = rest[idx]
                idx += 1
                cols = [c.strip() for c in cols_arg.split(",") if c.strip()]
                grouped = op_groupby(table, cols)
            elif op == "summary":
                if grouped is None:
                    raise BarrowError("summary requires a preceding groupby")
                if idx >= len(rest):
                    raise BarrowError("summary requires aggregations")
                aggs_arg = rest[idx]
                idx += 1
                pairs = [p.strip() for p in aggs_arg.split(",") if p.strip()]
                aggregations: dict[str, str] = {}
                for pair in pairs:
                    if "=" not in pair:
                        raise BarrowError("summary arguments must be COLUMN=AGG")
                    col, agg = pair.split("=", 1)
                    aggregations[col.strip()] = agg.strip()
                table = op_summary(grouped, aggregations)
                grouped = None
            else:  # pragma: no cover - defensive
                raise BarrowError(f"Unknown operation: {op}")

        write_table(table, args.output, args.output_format)
    except BarrowError as exc:  # pragma: no cover - error path
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

