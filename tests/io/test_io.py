from __future__ import annotations

from pathlib import Path
import io
import sys

import pyarrow as pa
import pytest

from barrow.errors import UnsupportedFormatError
from barrow.io import read_table, write_table


def test_read_table_from_path(tmp_path: Path) -> None:
    data = "a,b\n1,2\n"
    csv_path = tmp_path / "input.csv"
    csv_path.write_text(data)

    table = read_table(str(csv_path), "csv")
    assert table.to_pylist() == [{"a": 1, "b": 2}]


def test_read_table_from_stdin(monkeypatch) -> None:
    data = b"a,b\n1,2\n"
    stdin = io.BytesIO(data)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(stdin, encoding="utf-8"))

    table = read_table(None, "csv")
    assert table.to_pylist() == [{"a": 1, "b": 2}]


def test_write_table_to_stdout(capsys) -> None:
    table = pa.table({"a": [1]})
    write_table(table, None, "csv")
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == '"a"'
    assert out[1] == "1"


def test_write_table_to_parquet(tmp_path: Path) -> None:
    table = pa.table({"a": [1]})
    pq_path = tmp_path / "output.parquet"

    write_table(table, str(pq_path), "parquet")
    assert pq_path.exists()


def test_read_table_unsupported_format() -> None:
    with pytest.raises(UnsupportedFormatError):
        read_table("dummy", "json")


def test_write_table_unsupported_format() -> None:
    table = pa.table({"a": [1]})
    with pytest.raises(UnsupportedFormatError):
        write_table(table, "dummy", "json")

