import subprocess
import sys

import pyarrow.csv as csv
import pyarrow.parquet as pq

from barrow.cli import main
from barrow.errors import InvalidExpressionError


def test_cli_returns_error_on_exception(monkeypatch, capsys) -> None:
    def fake_read_table(path, fmt):
        raise InvalidExpressionError("bad format")

    monkeypatch.setattr("barrow.cli.read_table", fake_read_table)

    rc = main(["filter", "a > 1", "--input", "in.csv", "--output", "out.parquet"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "bad format" in err


def test_filter_and_select_pipeline(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.csv"

    cmd_filter = [
        sys.executable,
        "-m",
        "barrow.cli",
        "filter",
        "a > 1",
        "--input",
        sample_csv,
    ]
    cmd_select = [
        sys.executable,
        "-m",
        "barrow.cli",
        "select",
        "b,grp",
        "--output",
        str(dst),
    ]
    p1 = subprocess.Popen(cmd_filter, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd_select, stdin=p1.stdout)
    p1.stdout.close()
    p2.communicate()

    assert p2.returncode == 0
    table = csv.read_csv(dst)
    assert table.column_names == ["b", "grp"]
    assert table.to_pydict() == {"b": [5, 6], "grp": ["x", "y"]}


def test_mutate_from_stdin(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    cmd = [
        sys.executable,
        "-m",
        "barrow.cli",
        "mutate",
        "c=a+b",
        "--output",
        str(dst),
        "--output-format",
        "parquet",
    ]
    with open(sample_csv, "rb") as f:
        result = subprocess.run(cmd, stdin=f, capture_output=True)
    assert result.returncode == 0, result.stderr

    table = pq.read_table(dst)
    assert table.column_names == ["a", "b", "grp", "c"]
    assert table.to_pydict()["c"] == [5, 7, 9]


def test_groupby_summary_pipeline(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    cmd_mutate = [
        sys.executable,
        "-m",
        "barrow.cli",
        "mutate",
        "c=a+b",
        "--input",
        sample_csv,
        "--output-format",
        "parquet",
    ]
    cmd_groupby = [
        sys.executable,
        "-m",
        "barrow.cli",
        "groupby",
        "grp",
        "--input-format",
        "parquet",
        "--output-format",
        "parquet",
    ]
    cmd_summary = [
        sys.executable,
        "-m",
        "barrow.cli",
        "summary",
        "c=sum",
        "--input-format",
        "parquet",
        "--output",
        str(dst),
        "--output-format",
        "parquet",
    ]
    p1 = subprocess.Popen(cmd_mutate, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd_groupby, stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    p3 = subprocess.Popen(cmd_summary, stdin=p2.stdout)
    p2.stdout.close()
    p3.communicate()

    assert p3.returncode == 0
    table = pq.read_table(dst)
    assert table.column_names == ["grp", "c_sum"]
    assert table.to_pydict() == {"grp": ["x", "y"], "c_sum": [12, 9]}


def test_format_combinations(
    tmp_path, request, sample_table, sample_csv, sample_parquet
) -> None:
    src_csv = sample_csv
    src_parquet = sample_parquet
    cases = [
        (src_csv, "csv", "csv", csv.read_csv, ".csv"),
        (src_parquet, "parquet", "parquet", pq.read_table, ".parquet"),
    ]
    for src, input_fmt, output_fmt, reader, ext in cases:
        dst = tmp_path / f"out{ext}"
        rc = main(
            [
                "select",
                "a,b,grp",
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


def test_ungroup_removes_metadata(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.parquet"

    cmd_groupby = [
        sys.executable,
        "-m",
        "barrow.cli",
        "groupby",
        "grp",
        "--input",
        sample_csv,
        "--output-format",
        "parquet",
    ]
    cmd_ungroup = [
        sys.executable,
        "-m",
        "barrow.cli",
        "ungroup",
        "--input-format",
        "parquet",
        "--output",
        str(dst),
        "--output-format",
        "parquet",
    ]
    p1 = subprocess.Popen(cmd_groupby, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd_ungroup, stdin=p1.stdout)
    p1.stdout.close()
    p2.communicate()

    assert p2.returncode == 0
    table = pq.read_table(dst)
    assert (table.schema.metadata or {}).get(b"grouped_by") is None

