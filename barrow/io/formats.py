"""Format detection utilities."""

from __future__ import annotations

from pathlib import Path


#: Map file extensions to format names.
_EXT_MAP = {
    ".csv": "csv",
    ".parquet": "parquet",
    ".feather": "feather",
    ".orc": "orc",
}

#: Map magic byte prefixes to format names.
_MAGIC_MAP = {
    b"PAR1": "parquet",
    b"ARROW1": "feather",
    b"ORC": "orc",
}


def detect_format_from_path(path: str) -> str | None:
    """Infer format from a file extension."""
    ext = Path(path).suffix.lower()
    return _EXT_MAP.get(ext)


def detect_format_from_bytes(header: bytes) -> str | None:
    """Infer format from the first bytes of a file."""
    for magic, fmt in _MAGIC_MAP.items():
        if header[: len(magic)] == magic:
            return fmt
    return None
