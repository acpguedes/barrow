"""Remove grouping metadata from a table."""

from __future__ import annotations

import logging
import pyarrow as pa


logger = logging.getLogger(__name__)


def ungroup(table: pa.Table) -> pa.Table:
    """Return ``table`` without grouping metadata."""
    logger.debug("Ungrouping table")
    metadata = dict(table.schema.metadata or {})
    removed = metadata.pop(b"grouped_by", None)
    logger.debug("Removed grouping metadata: %s", removed)
    return table.replace_schema_metadata(metadata or None)


__all__ = ["ungroup"]
