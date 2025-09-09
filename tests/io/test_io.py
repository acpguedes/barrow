from __future__ import annotations

from pathlib import Path
import io
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from barrow.errors import UnsupportedFormatError
from barrow.io import read_table, write_table


def test_read_table_infers_format_from_extension(tmp_path: Path) -> None:
    data = "a,b\n1,2\n"
    csv_path = tmp_path / "input.csv"
    csv_path.write_text(data)
    table = read_table(str(csv_path), None)
    assert table.to_pylist() == [{"a": 1, "b": 2}]
    assert table.schema.metadata[b"format"] == b"csv"

    pq_path = tmp_path / "input.parquet"
    pq.write_table(pa.table({"a": [1]}), pq_path)
    table = read_table(str(pq_path), None)
    assert table.to_pydict() == {"a": [1]}
    assert table.schema.metadata[b"format"] == b"parquet"


def test_read_table_from_stdin_csv(monkeypatch) -> None:
    data = b"a,b\n1,2\n"

    class Dummy:
        def __init__(self, d: bytes) -> None:
            self.buffer = io.BytesIO(d)

    monkeypatch.setattr(sys, "stdin", Dummy(data))
    table = read_table(None, None)
    assert table.to_pylist() == [{"a": 1, "b": 2}]
    assert table.schema.metadata[b"format"] == b"csv"


def test_read_table_from_stdin_parquet(monkeypatch) -> None:
    buf = io.BytesIO()
    pq.write_table(pa.table({"a": [1]}), buf)

    class Dummy:
        def __init__(self, d: bytes) -> None:
            self.buffer = io.BytesIO(d)

    monkeypatch.setattr(sys, "stdin", Dummy(buf.getvalue()))
    table = read_table(None, None)
    assert table.to_pydict() == {"a": [1]}
    assert table.schema.metadata[b"format"] == b"parquet"


def test_write_table_to_stdout_defaults_csv(capsys) -> None:
    table = pa.table({"a": [1]})
    write_table(table, None, None)
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == '"a"'
    assert out[1] == "1"


def test_write_table_infers_from_extension(tmp_path: Path) -> None:
    table = pa.table({"a": [1]})
    pq_path = tmp_path / "output.parquet"

    write_table(table, str(pq_path), None)
    assert pq.read_table(pq_path).to_pydict() == {"a": [1]}


def test_write_table_uses_format_metadata(tmp_path: Path) -> None:
    pq_path = tmp_path / "input.parquet"
    pq.write_table(pa.table({"a": [1]}), pq_path)
    table = read_table(str(pq_path), None)
    out = tmp_path / "output"
    write_table(table, str(out), None)
    assert pq.read_table(out).to_pydict() == {"a": [1]}


def test_write_table_to_stdout_uses_format_metadata(monkeypatch, tmp_path: Path) -> None:
    pq_path = tmp_path / "input.parquet"
    pq.write_table(pa.table({"a": [1]}), pq_path)
    table = read_table(str(pq_path), None)

    buf = io.BytesIO()

    class Dummy:
        def __init__(self, b: io.BytesIO) -> None:
            self.buffer = b

    monkeypatch.setattr(sys, "stdout", Dummy(buf))
    write_table(table, None, None)
    assert buf.getvalue().startswith(b"PAR1")


def test_read_table_unsupported_format() -> None:
    with pytest.raises(UnsupportedFormatError):
        read_table("dummy", "json")


def test_write_table_unsupported_format() -> None:
    table = pa.table({"a": [1]})
    with pytest.raises(UnsupportedFormatError):
        write_table(table, "dummy", "json")
