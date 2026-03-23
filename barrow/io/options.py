"""I/O option dataclasses."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ScanOptions:
    """Options for reading a table."""

    path: str | None = None
    format: str | None = None
    delimiter: str | None = None
    columns: list[str] | None = None


@dataclass
class SinkOptions:
    """Options for writing a table."""

    path: str | None = None
    format: str | None = None
    delimiter: str | None = None
