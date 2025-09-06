from __future__ import annotations

"""Aggregation helpers for grouped tables."""

from collections.abc import Mapping

import pyarrow as pa


def summary(grouped: pa.TableGroupBy, aggregations: Mapping[str, str] | None = None, **kwargs: str) -> pa.Table:
    """Aggregate ``grouped`` according to ``aggregations``.

    Aggregations can be passed either as a mapping or as keyword arguments where
    keys are column names and values are the aggregation function (e.g. ``"sum"``).
    """
    if aggregations is None:
        aggregations = {}
    aggregations = {**aggregations, **kwargs}
    pairs = list(aggregations.items())
    return grouped.aggregate(pairs)


__all__ = ["summary"]
