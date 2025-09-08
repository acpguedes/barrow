"""Common environment for expression evaluation."""

from __future__ import annotations

import numpy as np
import pyarrow as pa


def build_env(table: pa.Table) -> dict[str, object]:
    """Return an evaluation environment for *table*.

    The environment maps column names to :class:`numpy.ndarray` instances and
    exposes all public functions from :mod:`numpy`.
    """

    env: dict[str, object] = {
        name: table[name].to_numpy(zero_copy_only=False) for name in table.column_names
    }
    env.update(
        {name: getattr(np, name) for name in dir(np) if not name.startswith("_")}
    )
    return env


__all__ = ["build_env"]
