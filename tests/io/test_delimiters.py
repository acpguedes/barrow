from __future__ import annotations

import pyarrow as pa
import pytest

from barrow.io import read_table, write_table


def _csv_content(delimiter: str) -> str:
    return f"a{delimiter}b\n1{delimiter}2\n"


@pytest.mark.parametrize("delimiter", [";", "\t"])
def test_read_table_sniffs_custom_delimiter(tmp_path, delimiter: str) -> None:
    path = tmp_path / "in.csv"
    path.write_text(_csv_content(delimiter))
    table = read_table(str(path), None, None)
    assert table.to_pydict() == {"a": [1], "b": [2]}


@pytest.mark.parametrize("delimiter", [";", "\t"])
def test_write_table_custom_delimiter(tmp_path, delimiter: str) -> None:
    table = pa.table({"a": [1], "b": [2]})
    path = tmp_path / "out.csv"
    write_table(table, str(path), "csv", delimiter)
    first_line = path.read_text().splitlines()[0]
    assert first_line == f'"a"{delimiter}"b"'
