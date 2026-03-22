"""Convert CLI arguments to a LogicalPlan."""

from __future__ import annotations

import argparse

from barrow.core.nodes import (
    Scan,
    Sink,
    Project,
    Filter as FilterNode,
    Mutate as MutateNode,
    GroupBy as GroupByNode,
    Aggregate,
    Ungroup as UngroupNode,
    Join as JoinNode,
    Sort as SortNode,
    Window as WindowNode,
    View as ViewNode,
    SqlQuery,
)
from barrow.core.plan import LogicalPlan
from barrow.expr import parse, Expression


def cli_to_plan(command: str, args: argparse.Namespace) -> LogicalPlan:
    """Build a LogicalPlan from CLI command name and parsed args."""
    builders = {
        "filter": _build_filter,
        "select": _build_select,
        "mutate": _build_mutate,
        "groupby": _build_groupby,
        "summary": _build_summary,
        "ungroup": _build_ungroup,
        "join": _build_join,
        "sort": _build_sort,
        "sql": _build_sql,
        "window": _build_window,
        "view": _build_view,
    }
    builder = builders[command]
    root = builder(args)
    return LogicalPlan(root)


def _scan(args: argparse.Namespace) -> Scan:
    """Create a Scan node from common CLI args."""
    return Scan(
        path=getattr(args, "input", None),
        format=getattr(args, "input_format", None),
        delimiter=getattr(args, "delimiter", None),
    )


def _sink(child, args: argparse.Namespace) -> Sink:
    """Wrap an operation in a Sink node."""
    delimiter = getattr(args, "output_delimiter", None) or getattr(
        args, "delimiter", None
    )
    return Sink(
        child=child,
        path=getattr(args, "output", None),
        format=getattr(args, "output_format", None),
        delimiter=delimiter,
    )


def _build_filter(args: argparse.Namespace):
    scan = _scan(args)
    expr = parse(args.expression)
    op = FilterNode(child=scan, expression=expr)
    return _sink(op, args)


def _build_select(args: argparse.Namespace):
    scan = _scan(args)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    op = Project(child=scan, columns=cols)
    return _sink(op, args)


def _build_mutate(args: argparse.Namespace):
    scan = _scan(args)
    pairs = [p.strip() for p in args.assignments.split(",") if p.strip()]
    expressions: dict[str, Expression] = {}
    for pair in pairs:
        name, expr_str = pair.split("=", 1)
        expressions[name.strip()] = parse(expr_str.strip())
    op = MutateNode(child=scan, assignments=expressions)
    return _sink(op, args)


def _build_groupby(args: argparse.Namespace):
    scan = _scan(args)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    op = GroupByNode(child=scan, keys=cols)
    return _sink(op, args)


def _build_summary(args: argparse.Namespace):
    scan = _scan(args)
    pairs = [p.strip() for p in args.aggregations.split(",") if p.strip()]
    aggregations: dict[str, str] = {}
    for pair in pairs:
        col, agg = pair.split("=", 1)
        aggregations[col.strip()] = agg.strip()
    # Summary reads grouped_by from metadata at execution time
    # Build as Aggregate with empty group_keys (engine will read from metadata)
    op = Aggregate(child=scan, group_keys=[], aggregations=aggregations)
    return _sink(op, args)


def _build_ungroup(args: argparse.Namespace):
    scan = _scan(args)
    op = UngroupNode(child=scan)
    return _sink(op, args)


def _build_join(args: argparse.Namespace):
    left_scan = _scan(args)
    right_scan = Scan(
        path=args.right,
        format=getattr(args, "right_format", None),
        delimiter=getattr(args, "delimiter", None),
    )
    op = JoinNode(
        left=left_scan,
        right=right_scan,
        left_on=args.left_on,
        right_on=args.right_on,
        join_type=args.join_type,
    )
    return _sink(op, args)


def _build_sort(args: argparse.Namespace):
    scan = _scan(args)
    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    desc = []
    if hasattr(args, "desc") and args.desc:
        desc = [True] * len(cols)
    op = SortNode(child=scan, keys=cols, descending=desc)
    return _sink(op, args)


def _build_sql(args: argparse.Namespace):
    scan = _scan(args)
    op = SqlQuery(child=scan, query=args.query)
    return _sink(op, args)


def _build_window(args: argparse.Namespace):
    scan = _scan(args)
    by = None
    if hasattr(args, "by") and args.by:
        by = [c.strip() for c in args.by.split(",") if c.strip()]
    order_by = None
    if hasattr(args, "order_by") and args.order_by:
        order_by = [c.strip() for c in args.order_by.split(",") if c.strip()]
    pairs = [p.strip() for p in args.assignments.split(",") if p.strip()]
    expressions: dict[str, Expression] = {}
    for pair in pairs:
        name, expr_str = pair.split("=", 1)
        expressions[name.strip()] = parse(expr_str.strip())
    op = WindowNode(child=scan, by=by, order_by=order_by, assignments=expressions)
    return _sink(op, args)


def _build_view(args: argparse.Namespace):
    scan = Scan(
        path=getattr(args, "input", None),
        format=getattr(args, "input_format", None),
        delimiter=getattr(args, "delimiter", None),
    )
    op = ViewNode(child=scan)
    # View always outputs CSV to STDOUT
    return Sink(
        child=op,
        path=None,
        format="csv",
        delimiter=getattr(args, "output_delimiter", None)
        or getattr(args, "delimiter", None),
    )
