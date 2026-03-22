"""Schema utilities for plan validation."""

from __future__ import annotations

import pyarrow as pa


def validate_columns(table: pa.Table, columns: list[str]) -> None:
    """Raise :class:`KeyError` if any column is missing from *table*."""
    available = set(table.column_names)
    missing = [c for c in columns if c not in available]
    if missing:
        raise KeyError(f"Columns not found: {missing}. Available: {sorted(available)}")


def columns_from_schema(schema: pa.Schema) -> list[str]:
    """Extract column names from an Arrow schema."""
    return [field.name for field in schema]


__all__ = ["validate_columns", "columns_from_schema"]
