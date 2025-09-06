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


def test_filter_and_select(tmp_path) -> None:
    data = b"name,age\nAlice,25\nBob,35\n"
    src = tmp_path / "in.csv"
    src.write_bytes(data)
    dst = tmp_path / "out.parquet"

    rc = main([
        "--input",
        str(src),
        "--output",
        str(dst),
        "filter",
        "age > 30",
        "select",
        "name,age",
    ])
    assert rc == 0
    table = pq.read_table(dst)
    assert table.column_names == ["name", "age"]
    assert table.to_pydict() == {"name": ["Bob"], "age": [35]}

