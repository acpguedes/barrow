from __future__ import annotations

"""Row filtering operation using Python expressions."""

import numpy as np
import pyarrow as pa


def filter(table: pa.Table, expression: str) -> pa.Table:
    """Filter ``table`` by evaluating ``expression``.

    The expression is evaluated with a namespace containing the table's
    columns as :class:`numpy.ndarray` objects and all functions from
    :mod:`numpy`.
    """
    env: dict[str, object] = {
        name: table[name].to_numpy(zero_copy_only=False) for name in table.column_names
    }
    env.update({name: getattr(np, name) for name in dir(np) if not name.startswith("_")})
    mask = eval(expression, {"__builtins__": {}}, env)
    return table.filter(pa.array(mask))


__all__ = ["filter"]
