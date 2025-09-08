"""Grouping utilities."""

from __future__ import annotations

import logging
import pyarrow as pa


logger = logging.getLogger(__name__)


def groupby(table: pa.Table, keys: str | list[str]) -> pa.Table:
    """Return ``table`` tagged with grouping metadata for ``keys``."""
    if isinstance(keys, str):
        keys = [keys]
    logger.debug("Grouping with keys %s", keys)
    metadata = dict(table.schema.metadata or {})
    metadata[b"grouped_by"] = ",".join(keys).encode()
    result = table.replace_schema_metadata(metadata)
    logger.debug("Grouping metadata set to %s", metadata[b"grouped_by"])
    return result


__all__ = ["groupby"]
