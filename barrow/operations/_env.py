"""Common environment for expression evaluation."""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Iterator

import numpy as np
import pyarrow as pa


class _NumpyFallbackDict(MutableMapping):
    """Dict that lazily resolves missing keys from :mod:`numpy`.

    This avoids iterating ``dir(numpy)`` (~600 names) upfront.  Column names
    are stored as regular dict entries; any unknown key is looked up in
    ``numpy`` on first access and cached.
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    # --- MutableMapping interface -----------------------------------------
    def __getitem__(self, key: str) -> object:
        try:
            return self._data[key]
        except KeyError:
            pass
        # Lazy resolve from numpy
        try:
            value = getattr(np, key)
        except AttributeError:
            raise KeyError(key) from None
        self._data[key] = value
        return value

    def __setitem__(self, key: str, value: object) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        if key in self._data:
            return True
        if isinstance(key, str):
            return hasattr(np, key)
        return False


def build_env(
    table: pa.Table,
    columns: set[str] | None = None,
) -> _NumpyFallbackDict:
    """Return an evaluation environment for *table*.

    The environment maps column names to :class:`numpy.ndarray` instances and
    lazily exposes all public functions from :mod:`numpy`.

    Parameters
    ----------
    table:
        The input table whose columns form the base namespace.
    columns:
        When provided, only these columns are converted to NumPy arrays.
        Unrequested columns are still accessible but converted on first use.
        Pass ``None`` to convert all columns eagerly (original behaviour).
    """

    if columns is not None:
        # Convert only the requested columns eagerly; defer the rest.
        data: dict[str, object] = {
            name: table[name].to_numpy(zero_copy_only=False)
            for name in columns
            if name in table.column_names
        }
        # Provide lazy access to remaining columns via a thin wrapper.
        _table_ref = table

        class _ColFallback(_NumpyFallbackDict):
            """Extends fallback to also resolve deferred table columns."""

            def __getitem__(self, key: str) -> object:
                try:
                    return self._data[key]
                except KeyError:
                    pass
                # Try deferred table column first
                if key in _table_ref.column_names:
                    value = _table_ref[key].to_numpy(zero_copy_only=False)
                    self._data[key] = value
                    return value
                # Then numpy
                try:
                    value = getattr(np, key)
                except AttributeError:
                    raise KeyError(key) from None
                self._data[key] = value
                return value

            def __contains__(self, key: object) -> bool:
                if key in self._data:
                    return True
                if isinstance(key, str):
                    if key in _table_ref.column_names:
                        return True
                    return hasattr(np, key)
                return False

        return _ColFallback(data)

    # Default: convert all columns eagerly (original behaviour).
    data = {
        name: table[name].to_numpy(zero_copy_only=False) for name in table.column_names
    }
    return _NumpyFallbackDict(data)


__all__ = ["build_env"]
