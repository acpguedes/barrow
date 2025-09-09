from __future__ import annotations

import subprocess
import sys

import pyarrow.feather as feather


def test_tmp_pipeline_feather(sample_csv, tmp_path) -> None:
    dst = tmp_path / "out.feather"

    cmd_filter = [
        sys.executable,
        "-m",
        "barrow.cli",
        "filter",
        "a > 1",
        "--input",
        sample_csv,
        "-t",
    ]
    cmd_select = [
        sys.executable,
        "-m",
        "barrow.cli",
        "select",
        "b,grp",
        "-t",
        "--output",
        str(dst),
    ]
    p1 = subprocess.Popen(cmd_filter, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd_select, stdin=p1.stdout)
    assert p1.stdout is not None
    p1.stdout.close()
    p2.communicate()
    assert p2.returncode == 0

    table = feather.read_table(dst)
    assert table.column_names == ["b", "grp"]
    assert table.to_pydict() == {"b": [5, 6], "grp": ["x", "y"]}
