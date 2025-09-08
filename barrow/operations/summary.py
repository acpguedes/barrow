"""Aggregation helpers for grouped tables."""

from __future__ import annotations

from collections.abc import Mapping

import logging
import pyarrow as pa

from ..errors import BarrowError


logger = logging.getLogger(__name__)


def summary(
    table: pa.Table, aggregations: Mapping[str, str] | None = None, **kwargs: str
) -> pa.Table:
    """Aggregate ``table`` according to ``aggregations`` using grouping metadata."""
    metadata = table.schema.metadata or {}
    grouped_by = metadata.get(b"grouped_by")
    logger.debug("Grouping metadata: %s", grouped_by)
    if not grouped_by:
        raise BarrowError("summary requires grouping metadata")
    keys = grouped_by.decode().split(",") if grouped_by else []
    if aggregations is None:
        aggregations = {}
    aggregations = {**aggregations, **kwargs}
    logger.debug("Summarizing with aggregations %s", aggregations)
    pairs = list(aggregations.items())
    result = table.group_by(keys).aggregate(pairs)
    logger.debug(
        "Summary result has %d rows and %d columns",
        result.num_rows,
        result.num_columns,
    )
    return result.replace_schema_metadata(metadata)


__all__ = ["summary"]
