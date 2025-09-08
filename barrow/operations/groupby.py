"""Grouping utilities."""

from __future__ import annotations

import pyarrow as pa


def groupby(table: pa.Table, keys: str | list[str]) -> pa.Table:
    """Return ``table`` tagged with grouping metadata for ``keys``."""
    if isinstance(keys, str):
        keys = [keys]
    metadata = dict(table.schema.metadata or {})
    metadata[b"grouped_by"] = ",".join(keys).encode()
    return table.replace_schema_metadata(metadata)


__all__ = ["groupby"]
