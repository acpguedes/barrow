from __future__ import annotations

import pyarrow as pa

from barrow.io import read_table, write_table
from barrow.operations import groupby


def test_csv_roundtrip_preserves_grouping(tmp_path) -> None:
    table = pa.table({"a": [1, 2], "b": [3, 4]})
    grouped = groupby(table, ["a"])
    path = tmp_path / "out.csv"
    write_table(grouped, str(path), None)
    lines = path.read_text().splitlines()
    assert lines[0] == "# grouped_by: a"
    result = read_table(str(path), None)
    assert result.schema.metadata.get(b"grouped_by") == b"a"
    assert result.to_pylist() == table.to_pylist()
