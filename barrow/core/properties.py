"""Logical properties for plan nodes."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LogicalProperties:
    """Properties tracked through the logical plan."""

    group_keys: list[str] = field(default_factory=list)
    ordering: list[tuple[str, str]] = field(default_factory=list)
    source_format: str | None = None
    estimated_rows: int | None = None
    is_materialized: bool = False
    backend_hint: str | None = None


__all__ = ["LogicalProperties"]
