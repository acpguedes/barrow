"""Column selection operation."""

from __future__ import annotations

import logging
import pyarrow as pa


logger = logging.getLogger(__name__)


def select(table: pa.Table, columns: str | list[str] | tuple[str, ...]) -> pa.Table:
    """Return a table with only *columns*.

    Parameters
    ----------
    table:
        Input table.
    columns:
        Column name or names to keep.
    """
    if isinstance(columns, (str, bytes)):
        cols = [columns]
    else:
        cols = list(columns)
    logger.debug("Selecting columns %s", cols)
    result = table.select(cols)
    logger.debug("Selected %d columns", result.num_columns)
    return result


__all__ = ["select"]
