import pyarrow.parquet as pq
import pytest

from barrow.cli import main


def test_filter_inherits_input_format(sample_parquet, tmp_path) -> None:
    out = tmp_path / "out"
    rc = main([
        "filter",
        "a > 1",
        "--input",
        sample_parquet,
        "--output",
        str(out),
    ])
    assert rc == 0
    table = pq.read_table(out)
    assert table.to_pydict()["a"] == [2, 3]


@pytest.mark.parametrize(
    "command, args",
    [
        ("select", ["a,b,grp"]),
        ("mutate", ["c=a+b"]),
        ("groupby", ["grp"]),
    ],
)
def test_other_commands_inherit_format(command, args, sample_parquet, tmp_path) -> None:
    out = tmp_path / "out"
    rc = main([command, *args, "--input", sample_parquet, "--output", str(out)])
    assert rc == 0
    pq.read_table(out)
