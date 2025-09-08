import io
import subprocess
import sys

import pyarrow.csv as csv


def test_view_reads_parquet_and_writes_csv(sample_parquet, sample_table) -> None:
    cmd = [
        sys.executable,
        "-m",
        "barrow.cli",
        "view",
        "--input",
        sample_parquet,
        "--output-format",
        "parquet",
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, result.stderr
    out = csv.read_csv(io.BytesIO(result.stdout))
    assert out.to_pydict() == sample_table.to_pydict()
