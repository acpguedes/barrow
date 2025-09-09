import pyarrow.parquet as pq
import pyarrow.orc as orc
import pytest

from barrow.cli import main


@pytest.mark.parametrize(
    "src_fixture, reader",
    [
        ("sample_parquet", pq.read_table),
        ("sample_orc", orc.read_table),
    ],
)
def test_filter_inherits_input_format(src_fixture, reader, request, tmp_path) -> None:
    sample = request.getfixturevalue(src_fixture)
    out = tmp_path / "out"
    rc = main(
        [
            "filter",
            "a > 1",
            "--input",
            sample,
            "--output",
            str(out),
        ]
    )
    assert rc == 0
    table = reader(out)
    assert table.to_pydict()["a"] == [2, 3]


@pytest.mark.parametrize(
    "command, args",
    [
        ("select", ["a,b,grp"]),
        ("mutate", ["c=a+b"]),
        ("groupby", ["grp"]),
    ],
)
@pytest.mark.parametrize(
    "src_fixture, reader",
    [
        ("sample_parquet", pq.read_table),
        ("sample_orc", orc.read_table),
    ],
)
def test_other_commands_inherit_format(command, args, src_fixture, reader, request, tmp_path) -> None:
    sample = request.getfixturevalue(src_fixture)
    out = tmp_path / "out"
    rc = main([command, *args, "--input", sample, "--output", str(out)])
    assert rc == 0
    reader(out)
