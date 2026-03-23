"""Tests for filter pushdown optimizer rule."""

from barrow.core.nodes import Scan, Sort, Filter
from barrow.optimizer.rules.filter_pushdown import push_filters_down
from barrow.expr import parse


def test_push_filter_past_sort():
    scan = Scan()
    sort = Sort(child=scan, keys=["a"])
    filt = Filter(child=sort, expression=parse("a > 1"))
    result = push_filters_down(filt)
    # After pushdown: Sort(Filter(Scan))
    assert isinstance(result, Sort)
    assert isinstance(result.child, Filter)
    assert isinstance(result.child.child, Scan)


def test_no_pushdown_without_sort():
    scan = Scan()
    filt = Filter(child=scan, expression=parse("a > 1"))
    result = push_filters_down(filt)
    assert isinstance(result, Filter)
    assert isinstance(result.child, Scan)
