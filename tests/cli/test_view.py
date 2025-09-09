import io
import subprocess
import sys

import pyarrow.csv as csv
import pytest


@pytest.mark.parametrize(
    "fixture, fmt",
    [
        ("sample_parquet", "parquet"),
        ("sample_orc", "orc"),
    ],
)
def test_view_reads_binary_and_writes_csv(fixture, fmt, request, sample_table) -> None:
    src = request.getfixturevalue(fixture)
    cmd = [
        sys.executable,
        "-m",
        "barrow.cli",
        "view",
        "--input",
        src,
        "--output-format",
        fmt,
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, result.stderr
    out = csv.read_csv(io.BytesIO(result.stdout))
    assert out.to_pydict() == sample_table.to_pydict()
