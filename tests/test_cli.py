import subprocess
import sys

from barrow.cli import main
from barrow.errors import InvalidExpressionError
import pyarrow.parquet as pq


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
