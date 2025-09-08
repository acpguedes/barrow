import subprocess
import sys

import pyarrow as pa
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
    assert p1.stdout is not None
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
        "--output-format",
        "parquet",
    ]
    cmd_summary = [
        sys.executable,
        "-m",
        "barrow.cli",
        "summary",
        "c=sum",
        "--output",
        str(dst),
    ]
    p1 = subprocess.Popen(cmd_mutate, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd_groupby, stdin=p1.stdout, stdout=subprocess.PIPE)
    assert p1.stdout is not None
    p1.stdout.close()
    p3 = subprocess.Popen(cmd_summary, stdin=p2.stdout)
    assert p2.stdout is not None
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
        (src_csv, csv.read_csv, ".csv"),
        (src_parquet, pq.read_table, ".parquet"),
    ]
    for src, reader, ext in cases:
        dst = tmp_path / f"out{ext}"
        rc = main(
            [
                "select",
                "a,b,grp",
                "--input",
                src,
                "--output",
                str(dst),
            ]
        )
        assert rc == 0
        table = reader(dst)
        assert table.to_pydict() == sample_table.to_pydict()


def test_join_cli_infers_formats(tmp_path) -> None:
    left = pa.table({"id": [1, 2], "left_val": ["a", "b"]})
    right = pa.table({"id": [1], "right_val": ["x"]})
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.parquet"
    out_path = tmp_path / "out.csv"
    csv.write_csv(left, left_path)
    pq.write_table(right, right_path)

    cmd = [
        sys.executable,
        "-m",
        "barrow.cli",
        "join",
        "id",
        "id",
        "--input",
        str(left_path),
        "--right",
        str(right_path),
        "--output",
        str(out_path),
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, result.stderr
    table = csv.read_csv(out_path)
    assert table.to_pylist() == [{"id": 1, "left_val": "a", "right_val": "x"}]


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
        "--output",
        str(dst),
    ]
    p1 = subprocess.Popen(cmd_groupby, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd_ungroup, stdin=p1.stdout)
    assert p1.stdout is not None
    p1.stdout.close()
    p2.communicate()

    assert p2.returncode == 0
    table = pq.read_table(dst)
    assert (table.schema.metadata or {}).get(b"grouped_by") is None

