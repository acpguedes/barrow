"""Sort rows by column values."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def sort(
    table: pa.Table,
    keys: list[str],
    descending: list[bool] | None = None,
) -> pa.Table:
    """Return *table* sorted by *keys*.

    Parameters
    ----------
    table:
        Input table.
    keys:
        Column names to sort by.
    descending:
        Per-key sort direction.  ``True`` means descending.  Defaults to
        ascending for all keys when ``None`` or empty.
    """
    sort_keys: list[tuple[str, str]] = []
    for i, key in enumerate(keys):
        order = (
            "descending"
            if descending and i < len(descending) and descending[i]
            else "ascending"
        )
        sort_keys.append((key, order))
    indices = pc.sort_indices(table, sort_keys=sort_keys)
    return table.take(indices)


__all__ = ["sort"]
