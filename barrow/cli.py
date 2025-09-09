#!/usr/bin/env python3
"""Command line interface for barrow."""

from __future__ import annotations

import argparse
from pathlib import Path
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
    """Add common I/O options to ``parser``.

    When ``--output-format`` is omitted, the output format defaults to the
    input format.
    """

    parser.add_argument("--input", "-i", help="Input file. Reads STDIN if omitted.")
    parser.add_argument(
        "--input-format", choices=["csv", "parquet", "feather", "orc"], help="Input format"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file. Writes to STDOUT if omitted.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--output-format",
        choices=["csv", "parquet", "feather", "orc"],
        help="Output format",
    )
    group.add_argument(
        "--csv",
        dest="output_format",
        action="store_const",
        const="csv",
        help="Write output in CSV format",
    )
    group.add_argument(
        "--parquet",
        dest="output_format",
        action="store_const",
        const="parquet",
        help="Write output in Parquet format",
    )
    group.add_argument(
        "--feather",
        dest="output_format",
        action="store_const",
        const="feather",
        help="Write output in Feather format",
    )
    group.add_argument(
        "--orc",
        dest="output_format",
        action="store_const",
        const="orc",
        help="Write output in ORC format",
    )
    parser.add_argument(
        "--delimiter",
        help="Field delimiter for CSV input; also used for output",
    )
    parser.add_argument(
        "--csv-out-delimiter",
        dest="output_delimiter",
        help="Field delimiter for CSV output",
    )
    parser.add_argument(
        "--tmp",
        "-t",
        action="store_true",
        help="Use Feather for intermediate pipe format",
    )

    def _set_io_defaults(args: argparse.Namespace) -> None:
        if args.tmp:
            if args.output_format is None:
                args.output_format = "feather"
            if args.input is None and args.input_format is None:
                args.input_format = "feather"
        if args.output_format is None:
            if args.input_format is not None:
                args.output_format = args.input_format
            elif args.input:
                ext = Path(args.input).suffix.lower()
                if ext == ".csv":
                    args.output_format = "csv"
                elif ext == ".parquet":
                    args.output_format = "parquet"
                elif ext == ".feather":
                    args.output_format = "feather"
                elif ext == ".orc":
                    args.output_format = "orc"

    parser.set_defaults(_set_io_defaults=_set_io_defaults)


def _cmd_filter(args: argparse.Namespace) -> int:
    """Filter rows.

    The output format defaults to the input format when ``--output-format`` is
    not specified.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    expr = parse(args.expression)
    table = op_filter(table, expr)
    write_table(
        table,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_select(args: argparse.Namespace) -> int:
    """Select columns.

    Output format inherits from the input format unless ``--output-format`` is
    provided.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    table = op_select(table, cols)
    write_table(
        table,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_mutate(args: argparse.Namespace) -> int:
    """Add or modify columns.

    Unless ``--output-format`` is specified, the input format is used for the
    output.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    pairs = [p.strip() for p in args.assignments.split(",") if p.strip()]
    expressions: dict[str, Expression] = {}
    for pair in pairs:
        if "=" not in pair:
            raise BarrowError("mutate arguments must be NAME=EXPR")
        name, expr_str = pair.split("=", 1)
        expressions[name.strip()] = parse(expr_str.strip())
    table = op_mutate(table, **expressions)
    write_table(
        table,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_groupby(args: argparse.Namespace) -> int:
    """Group rows by columns.

    When ``--output-format`` is omitted, the output matches the input format.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    table = op_groupby(table, cols)
    write_table(
        table,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_summary(args: argparse.Namespace) -> int:
    """Aggregate a grouped table.

    The output uses the input format unless ``--output-format`` is given.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    pairs = [p.strip() for p in args.aggregations.split(",") if p.strip()]
    aggregations: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise BarrowError("summary arguments must be COLUMN=AGG")
        col, agg = pair.split("=", 1)
        aggregations[col.strip()] = agg.strip()
    result = op_summary(table, aggregations)
    write_table(
        result,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_ungroup(args: argparse.Namespace) -> int:
    """Remove grouping metadata.

    Output format defaults to the input format when not specified.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    table = op_ungroup(table)
    write_table(
        table,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_join(args: argparse.Namespace) -> int:
    """Join two tables.

    Unless ``--output-format`` is supplied, the output format matches the
    input.
    """

    left = read_table(args.input, args.input_format, args.delimiter)
    right = read_table(args.right, args.right_format, args.delimiter)
    result = op_join(left, right, args.left_on, args.right_on, args.join_type)
    write_table(
        result,
        args.output,
        args.output_format,
        args.output_delimiter or args.delimiter,
    )
    return 0


def _cmd_view(args: argparse.Namespace) -> int:
    """Display a table in a human-readable form.

    ``--output-format`` is accepted for API compatibility but ignored, and the
    table is always written to ``STDOUT`` in CSV format.
    """

    table = read_table(args.input, args.input_format, args.delimiter)
    write_table(table, None, "csv", args.output_delimiter or args.delimiter)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Create and return the top-level argument parser."""

    parser = argparse.ArgumentParser(
        description="barrow: simple data tool",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p = subparsers.add_parser(
        "filter",
        help="Filter rows using a boolean expression",
        description=(
            "Filter rows by evaluating an expression for each row.\n"
            "Only rows where the expression is true are written to the output."
        ),
        epilog="Example:\n  barrow filter 'age > 30' -i people.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("expression", help="Expression to evaluate")
    p.set_defaults(func=_cmd_filter)

    p = subparsers.add_parser(
        "select",
        help="Keep a subset of columns",
        description=(
            "Select a comma-separated list of columns and drop all others.\n"
            "Column names are case-sensitive."
        ),
        epilog="Example:\n  barrow select 'name,age' -i people.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("columns", help="Comma-separated column names")
    p.set_defaults(func=_cmd_select)

    p = subparsers.add_parser(
        "mutate",
        help="Add or modify columns via expressions",
        description=(
            "Create new columns or overwrite existing ones with NAME=EXPR"
            " assignments.\nExpressions may reference existing columns and use"
            " Python operators."
        ),
        epilog="Example:\n  barrow mutate 'double=value*2,total=a+b' -i data.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("assignments", help="Comma-separated NAME=EXPR pairs")
    p.set_defaults(func=_cmd_mutate)

    p = subparsers.add_parser(
        "groupby",
        help="Group rows by column values",
        description=(
            "Group rows using one or more columns. The grouping information is\n"
            "stored in the output so that 'summary' can aggregate over the groups later."
        ),
        epilog="Example:\n  barrow groupby 'city,year' -i sales.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("columns", help="Comma-separated column names")
    p.set_defaults(func=_cmd_groupby)

    p = subparsers.add_parser(
        "summary",
        help="Aggregate a grouped table",
        description=(
            "Compute aggregations for each group produced by 'groupby'.\n"
            "Each aggregation uses COLUMN=AGG where AGG is a function such as sum or mean."
        ),
        epilog="Example:\n  barrow summary 'total=sum(amount)' -i grouped.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("aggregations", help="Comma-separated COLUMN=AGG pairs")
    p.set_defaults(func=_cmd_summary)

    p = subparsers.add_parser(
        "ungroup",
        help="Remove grouping metadata",
        description=(
            "Drop grouping information created by 'groupby' so the table behaves\n"
            "like an ungrouped table."
        ),
        epilog="Example:\n  barrow ungroup -i grouped.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.set_defaults(func=_cmd_ungroup)

    p = subparsers.add_parser(
        "join",
        help="Join two tables on column keys",
        description=(
            "Combine a left table with another table supplied via --right.\n"
            "Specify key columns with LEFT_ON and RIGHT_ON and optionally choose\n"
            "a join type such as inner or outer."
        ),
        epilog="Example:\n  barrow join id id --right other.csv -i left.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("left_on", help="Join key in the left table")
    p.add_argument("right_on", help="Join key in the right table")
    p.add_argument("--right", required=True, help="Right input file")
    p.add_argument(
        "--right-format", choices=["csv", "parquet", "orc"], help="Right file format"
    )
    p.add_argument(
        "--join-type",
        choices=["inner", "left", "right", "outer"],
        default="inner",
    )
    p.set_defaults(func=_cmd_join)

    p = subparsers.add_parser(
        "view",
        help="Pretty-print a table to STDOUT",
        description=(
            "Display a table in CSV format for quick inspection. The output is\n"
            "always written to STDOUT and --output-format is ignored."
        ),
        epilog="Example:\n  barrow view -i data.parquet",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--input", "-i", help="Input file. Reads STDIN if omitted.")
    p.add_argument("--input-format", choices=["csv", "parquet", "orc"], help="Input format")
    p.add_argument(
        "--output-format",
        choices=["csv", "parquet", "orc"],
        help="Output format (ignored)",
    )
    p.add_argument(
        "--delimiter", help="Field delimiter for CSV input; also used for output"
    )
    p.add_argument(
        "--csv-out-delimiter",
        dest="output_delimiter",
        help="Field delimiter for CSV output",
    )
    p.set_defaults(func=_cmd_view)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``barrow`` command line tool."""

    parser = build_parser()
    if argcomplete:  # pragma: no cover - optional dependency
        argcomplete.autocomplete(parser)
    args = parser.parse_args(argv)
    if hasattr(args, "_set_io_defaults"):
        args._set_io_defaults(args)
    try:
        return args.func(args)
    except BarrowError as exc:  # pragma: no cover - error path
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
