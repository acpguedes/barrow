"""Tests for CLI to plan conversion."""

import argparse
from barrow.core.nodes import Sink, Sort, SqlQuery
from barrow.frontend.cli_to_plan import cli_to_plan


def _make_args(**kwargs):
    defaults = {
        "input": "data.csv",
        "input_format": "csv",
        "output": "out.csv",
        "output_format": "csv",
        "delimiter": None,
        "output_delimiter": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_filter_plan():
    args = _make_args(expression="a > 1")
    plan = cli_to_plan("filter", args)
    nodes = list(plan.walk())
    types = [type(n).__name__ for n in nodes]
    assert "Scan" in types
    assert "Filter" in types
    assert "Sink" in types


def test_select_plan():
    args = _make_args(columns="a,b")
    plan = cli_to_plan("select", args)
    nodes = list(plan.walk())
    types = [type(n).__name__ for n in nodes]
    assert "Project" in types


def test_sort_plan():
    args = _make_args(columns="a,b", desc=True)
    plan = cli_to_plan("sort", args)
    nodes = list(plan.walk())
    sort_nodes = [n for n in nodes if isinstance(n, Sort)]
    assert len(sort_nodes) == 1
    assert sort_nodes[0].keys == ["a", "b"]
    assert sort_nodes[0].descending == [True, True]


def test_sql_plan():
    args = _make_args(query="SELECT * FROM tbl")
    plan = cli_to_plan("sql", args)
    nodes = list(plan.walk())
    sql_nodes = [n for n in nodes if isinstance(n, SqlQuery)]
    assert len(sql_nodes) == 1
    assert sql_nodes[0].query == "SELECT * FROM tbl"


def test_view_plan():
    args = _make_args()
    plan = cli_to_plan("view", args)
    nodes = list(plan.walk())
    types = [type(n).__name__ for n in nodes]
    assert "View" in types
    # Sink should be CSV to STDOUT
    sink_nodes = [n for n in nodes if isinstance(n, Sink)]
    assert sink_nodes[0].format == "csv"
    assert sink_nodes[0].path is None
