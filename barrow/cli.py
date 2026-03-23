#!/usr/bin/env python3
"""Command line interface for barrow."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

try:  # pragma: no cover - optional dependency
    import argcomplete
except Exception:  # pragma: no cover - optional dependency
    argcomplete = None

from .core.plan import format_plan
from .errors import BarrowError
from .execution import execute
from .frontend.cli_to_plan import cli_to_plan
from .optimizer import optimize


def _add_io_options(parser: argparse.ArgumentParser) -> None:
    """Add common I/O options to ``parser``.

    When ``--output-format`` is omitted, the output format defaults to the
    input format.
    """

    parser.add_argument("--input", "-i", help="Input file. Reads STDIN if omitted.")
    parser.add_argument(
        "--input-format",
        choices=["csv", "parquet", "feather", "orc"],
        help="Input format",
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

    plan = cli_to_plan("filter", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_select(args: argparse.Namespace) -> int:
    """Select columns.

    Output format inherits from the input format unless ``--output-format`` is
    provided.
    """

    plan = cli_to_plan("select", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_mutate(args: argparse.Namespace) -> int:
    """Add or modify columns.

    Unless ``--output-format`` is specified, the input format is used for the
    output.
    """

    plan = cli_to_plan("mutate", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_groupby(args: argparse.Namespace) -> int:
    """Group rows by columns.

    When ``--output-format`` is omitted, the output matches the input format.
    """

    plan = cli_to_plan("groupby", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_summary(args: argparse.Namespace) -> int:
    """Aggregate a grouped table.

    The output uses the input format unless ``--output-format`` is given.
    """

    plan = cli_to_plan("summary", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_ungroup(args: argparse.Namespace) -> int:
    """Remove grouping metadata.

    Output format defaults to the input format when not specified.
    """

    plan = cli_to_plan("ungroup", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_join(args: argparse.Namespace) -> int:
    """Join two tables.

    Unless ``--output-format`` is supplied, the output format matches the
    input.
    """

    plan = cli_to_plan("join", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_view(args: argparse.Namespace) -> int:
    """Display a table in a human-readable form.

    ``--output-format`` is accepted for API compatibility but ignored, and the
    table is always written to ``STDOUT`` in CSV format.
    """

    plan = cli_to_plan("view", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_sort(args: argparse.Namespace) -> int:
    plan = cli_to_plan("sort", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_sql(args: argparse.Namespace) -> int:
    plan = cli_to_plan("sql", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_window(args: argparse.Namespace) -> int:
    plan = cli_to_plan("window", args)
    optimized = optimize(plan)
    execute(optimized.root)
    return 0


def _cmd_explain(args: argparse.Namespace) -> int:
    cmd = args.explain_command
    # Build a namespace compatible with cli_to_plan
    import copy

    plan_args = copy.copy(args)
    if cmd in ("filter",):
        plan_args.expression = args.expression
    elif cmd in ("select", "groupby", "sort"):
        plan_args.columns = args.expression
    elif cmd in ("mutate", "window"):
        plan_args.assignments = args.expression
    elif cmd in ("summary",):
        plan_args.aggregations = args.expression
    elif cmd in ("sql",):
        plan_args.query = args.expression
    # Set IO defaults
    plan_args.output = None
    plan_args.output_format = None
    plan_args.delimiter = None
    plan_args.output_delimiter = None

    plan = cli_to_plan(cmd, plan_args)
    print("Logical Plan:")
    print(format_plan(plan.root))
    print()
    optimized = optimize(plan)
    print("Optimized Plan:")
    print(format_plan(optimized.root))
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Create and return the top-level argument parser."""

    parser = argparse.ArgumentParser(
        description="barrow: simple data tool",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

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
        "--right-format",
        choices=["csv", "parquet", "feather", "orc"],
        help="Right file format",
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
    p.add_argument(
        "--input-format", choices=["csv", "parquet", "orc"], help="Input format"
    )
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

    p = subparsers.add_parser(
        "sort",
        help="Sort rows by column values",
        description=(
            "Sort rows using one or more columns.\n"
            "By default rows are sorted in ascending order."
        ),
        epilog="Example:\n  barrow sort 'name,age' -i people.csv --desc",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("columns", help="Comma-separated column names to sort by")
    p.add_argument("--desc", action="store_true", help="Sort in descending order")
    p.set_defaults(func=_cmd_sort)

    p = subparsers.add_parser(
        "sql",
        help="Execute a SQL query against the input table",
        description=(
            "Run a SQL query using DuckDB. The input table is available\n"
            "as 'tbl' in the query."
        ),
        epilog="Example:\n  barrow sql 'SELECT name, age FROM tbl WHERE age > 30' -i people.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("query", help="SQL query to execute (table is named 'tbl')")
    p.set_defaults(func=_cmd_sql)

    p = subparsers.add_parser(
        "window",
        help="Apply window functions",
        description=(
            "Compute window functions over partitions of the data.\n"
            "Use --by to partition and --order-by to order within partitions."
        ),
        epilog="Example:\n  barrow window 'rn=row_number()' --by grp --order-by val -i data.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _add_io_options(p)
    p.add_argument("assignments", help="Comma-separated NAME=EXPR pairs")
    p.add_argument("--by", help="Comma-separated partition columns")
    p.add_argument("--order-by", help="Comma-separated order columns")
    p.set_defaults(func=_cmd_window)

    p = subparsers.add_parser(
        "explain",
        help="Show the execution plan without running it",
        description=(
            "Display the logical and optimized plans for a command.\n"
            "Useful for understanding how barrow will execute a pipeline."
        ),
        epilog="Example:\n  barrow explain filter 'age > 30' -i people.csv",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # explain needs the subcommand and its args
    # For simplicity, accept command name + expression/columns as positional
    p.add_argument("explain_command", help="Command to explain (filter, select, etc.)")
    p.add_argument(
        "expression", nargs="?", help="Expression or columns for the command"
    )
    p.add_argument("--input", "-i", help="Input file")
    p.add_argument(
        "--input-format",
        choices=["csv", "parquet", "feather", "orc"],
        help="Input format",
    )
    p.set_defaults(func=_cmd_explain)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``barrow`` command line tool."""

    parser = build_parser()
    if argcomplete:  # pragma: no cover - optional dependency
        argcomplete.autocomplete(parser)
    args = parser.parse_args(argv)
    if args.command is None and os.environ.get("_ARGCOMPLETE") != "1":
        parser.print_usage(file=sys.stderr)
        return 1
    if hasattr(args, "_set_io_defaults"):
        args._set_io_defaults(args)
    try:
        return args.func(args)
    except BarrowError as exc:  # pragma: no cover - error path
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
