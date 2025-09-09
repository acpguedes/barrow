from __future__ import annotations

import pyarrow.csv as csv
import pyarrow.feather as feather
import pyarrow.parquet as pq
import pytest

from barrow.cli import main


@pytest.mark.parametrize(
    "flag, ext, reader",
    [
        ("--csv", ".csv", csv.read_csv),
        ("--parquet", ".parquet", pq.read_table),
        ("--feather", ".feather", feather.read_table),
    ],
)
def test_cli_output_format_flags(tmp_path, sample_csv, sample_table, flag, ext, reader) -> None:
    dst = tmp_path / f"out{ext}"
    rc = main([
        "select",
        "a,b,grp",
        "--input",
        sample_csv,
        flag,
        "--output",
        str(dst),
    ])
    assert rc == 0
    table = reader(dst)
    assert table.to_pydict() == sample_table.to_pydict()


def test_cli_csv_out_delimiter(tmp_path, sample_csv) -> None:
    dst = tmp_path / "out.csv"
    rc = main([
        "select",
        "a,b",
        "--input",
        sample_csv,
        "--csv",
        "--csv-out-delimiter",
        ";",
        "--output",
        str(dst),
    ])
    assert rc == 0
    first_line = dst.read_text().splitlines()[0]
    assert first_line == '"a";"b"'
