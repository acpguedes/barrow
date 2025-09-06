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

