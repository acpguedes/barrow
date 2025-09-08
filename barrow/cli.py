#!/usr/bin/env python3
"""Command line interface for barrow."""

from __future__ import annotations

import argparse
import sys

try:  # pragma: no cover - optional dependency
    import argcomplete
except Exception:  # pragma: no cover - optional dependency
    argcomplete = None

from .errors import BarrowError
from .expr import Expression, parse
from .io import read_table, write_table
from .operations import (
    filter as op_filter,
    groupby as op_groupby,
    join as op_join,
    mutate as op_mutate,
    select as op_select,
    summary as op_summary,
    ungroup as op_ungroup,
)


def _add_io_options(parser: argparse.ArgumentParser) -> None:
    """Add common I/O options to ``parser``."""

    parser.add_argument("--input", "-i", help="Input file. Reads STDIN if omitted.")
    parser.add_argument("--input-format", choices=["csv", "parquet"], help="Input format")
    parser.add_argument(
        "--output",
        "-o",
        help="Output file. Writes to STDOUT if omitted.",
    )
    parser.add_argument(
        "--output-format", choices=["csv", "parquet"], help="Output format"
    )


def _cmd_filter(args: argparse.Namespace) -> int:
    table = read_table(args.input, args.input_format)
    expr = parse(args.expression)
    table = op_filter(table, expr)
    write_table(table, args.output, args.output_format)
    return 0


def _cmd_select(args: argparse.Namespace) -> int:
    table = read_table(args.input, args.input_format)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    table = op_select(table, cols)
    write_table(table, args.output, args.output_format)
    return 0


def _cmd_mutate(args: argparse.Namespace) -> int:
    table = read_table(args.input, args.input_format)
    pairs = [p.strip() for p in args.assignments.split(",") if p.strip()]
    expressions: dict[str, Expression] = {}
    for pair in pairs:
        if "=" not in pair:
            raise BarrowError("mutate arguments must be NAME=EXPR")
        name, expr_str = pair.split("=", 1)
        expressions[name.strip()] = parse(expr_str.strip())
    table = op_mutate(table, **expressions)
    write_table(table, args.output, args.output_format)
    return 0


def _cmd_groupby(args: argparse.Namespace) -> int:
    table = read_table(args.input, args.input_format)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    table = op_groupby(table, cols)
    write_table(table, args.output, args.output_format)
    return 0


def _cmd_summary(args: argparse.Namespace) -> int:
    table = read_table(args.input, args.input_format)
    pairs = [p.strip() for p in args.aggregations.split(",") if p.strip()]
    aggregations: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise BarrowError("summary arguments must be COLUMN=AGG")
        col, agg = pair.split("=", 1)
        aggregations[col.strip()] = agg.strip()
    result = op_summary(table, aggregations)
    write_table(result, args.output, args.output_format)
    return 0


def _cmd_ungroup(args: argparse.Namespace) -> int:
    table = read_table(args.input, args.input_format)
    table = op_ungroup(table)
    write_table(table, args.output, args.output_format)
    return 0


def _cmd_join(args: argparse.Namespace) -> int:
    left = read_table(args.input, args.input_format)
    right = read_table(args.right, args.right_format)
    result = op_join(left, right, args.left_on, args.right_on, args.join_type)
    write_table(result, args.output, args.output_format)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Create and return the top-level argument parser."""

    parser = argparse.ArgumentParser(description="barrow: simple data tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p = subparsers.add_parser("filter", help="Filter rows")
    _add_io_options(p)
    p.add_argument("expression", help="Expression to evaluate")
    p.set_defaults(func=_cmd_filter)

    p = subparsers.add_parser("select", help="Select columns")
    _add_io_options(p)
    p.add_argument("columns", help="Comma-separated column names")
    p.set_defaults(func=_cmd_select)

    p = subparsers.add_parser("mutate", help="Add or modify columns")
    _add_io_options(p)
    p.add_argument("assignments", help="Comma-separated NAME=EXPR pairs")
    p.set_defaults(func=_cmd_mutate)

    p = subparsers.add_parser("groupby", help="Group rows by columns")
    _add_io_options(p)
    p.add_argument("columns", help="Comma-separated column names")
    p.set_defaults(func=_cmd_groupby)

    p = subparsers.add_parser("summary", help="Aggregate grouped table")
    _add_io_options(p)
    p.add_argument("aggregations", help="Comma-separated COLUMN=AGG pairs")
    p.set_defaults(func=_cmd_summary)

    p = subparsers.add_parser("ungroup", help="Remove grouping metadata")
    _add_io_options(p)
    p.set_defaults(func=_cmd_ungroup)

    p = subparsers.add_parser("join", help="Join two tables")
    _add_io_options(p)
    p.add_argument("left_on", help="Join key in the left table")
    p.add_argument("right_on", help="Join key in the right table")
    p.add_argument("--right", required=True, help="Right input file")
    p.add_argument(
        "--right-format", choices=["csv", "parquet"], help="Right file format"
    )
    p.add_argument(
        "--join-type",
        choices=["inner", "left", "right", "outer"],
        default="inner",
    )
    p.set_defaults(func=_cmd_join)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``barrow`` command line tool."""

    parser = build_parser()
    if argcomplete:  # pragma: no cover - optional dependency
        argcomplete.autocomplete(parser)
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except BarrowError as exc:  # pragma: no cover - error path
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

