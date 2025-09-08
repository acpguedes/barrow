"""Remove grouping metadata from a table."""

from __future__ import annotations

import pyarrow as pa


def ungroup(table: pa.Table) -> pa.Table:
    """Return ``table`` without grouping metadata."""
    metadata = dict(table.schema.metadata or {})
    metadata.pop(b"grouped_by", None)
    return table.replace_schema_metadata(metadata or None)


__all__ = ["ungroup"]
