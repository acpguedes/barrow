import subprocess
import sys

import pyarrow.csv as csv
import pyarrow.parquet as pq
import pytest

from barrow.cli import main
from barrow.errors import InvalidExpressionError


def test_cli_returns_error_on_exception(monkeypatch, capsys) -> None:
    def fake_read_table(path, fmt):
        raise InvalidExpressionError("bad format")

    monkeypatch.setattr("barrow.cli.read_table", fake_read_table)

    rc = main(["--input", "in.csv", "--output", "out.parquet"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "bad format" in err


def test_filter_and_select(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    rc = main([
        "--input",
        sample_csv,
        "--output",
        str(dst),
        "filter",
        "a > 1",
        "select",
        "b,grp",
    ])
    assert rc == 0
    table = pq.read_table(dst)
    assert table.column_names == ["b", "grp"]
    assert table.to_pydict() == {"b": [5, 6], "grp": ["x", "y"]}


def test_cli_subprocess(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"
    cmd = [
        sys.executable,
        "-m",
        "barrow.cli",
        "--input",
        sample_csv,
        "--output",
        str(dst),
        "filter",
        "a > 1",
        "select",
        "b,grp",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    table = pq.read_table(dst)
    assert table.column_names == ["b", "grp"]
    assert table.to_pydict() == {"b": [5, 6], "grp": ["x", "y"]}


def test_mutate(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    rc = main([
        "--input",
        sample_csv,
        "--output",
        str(dst),
        "mutate",
        "c=a+b",
    ])
    assert rc == 0
    table = pq.read_table(dst)
    assert table.column_names == ["a", "b", "grp", "c"]
    assert table.to_pydict()["c"] == [5, 7, 9]


def test_groupby_summary(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    rc = main([
        "--input",
        sample_csv,
        "--output",
        str(dst),
        "mutate",
        "c=a+b",
        "groupby",
        "grp",
        "summary",
        "c=sum",
    ])
    assert rc == 0
    table = pq.read_table(dst)
    assert table.column_names == ["grp", "c_sum"]
    assert table.to_pydict() == {"grp": ["x", "y"], "c_sum": [12, 9]}


@pytest.mark.parametrize(
    "input_fixture,input_fmt",
    [("sample_csv", "csv"), ("sample_parquet", "parquet")],
)
@pytest.mark.parametrize(
    "output_fmt,reader,ext",
    [("csv", csv.read_csv, ".csv"), ("parquet", pq.read_table, ".parquet")],
)
def test_format_combinations(
    tmp_path, request, sample_table, input_fixture, input_fmt, output_fmt, reader, ext
) -> None:
    src = request.getfixturevalue(input_fixture)
    dst = tmp_path / f"out{ext}"
    rc = main(
        [
            "--input",
            src,
            "--input-format",
            input_fmt,
            "--output",
            str(dst),
            "--output-format",
            output_fmt,
        ]
    )
    assert rc == 0
    table = reader(dst)
    assert table.to_pydict() == sample_table.to_pydict()


def test_window(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    rc = main([
        "--input",
        sample_csv,
        "--output",
        str(dst),
        "window",
        "by=grp",
        "order_by=a",
        "rn=row_number()",
    ])
    assert rc == 0
    table = pq.read_table(dst)
    assert table["rn"].to_pylist() == [1, 2, 1]


def test_window_missing_expression(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"
    rc = main([
        "--input",
        sample_csv,
        "--output",
        str(dst),
        "window",
    ])
    assert rc == 1
