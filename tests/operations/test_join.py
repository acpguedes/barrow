import pyarrow as pa
import pytest

from barrow.operations import join


def test_inner_join() -> None:
    left = pa.table({"id": [1, 2], "val": [10, 20]})
    right = pa.table({"key": [2, 3], "other": [200, 300]})
    result = join(left, right, "id", "key")
    assert result.column_names == ["id", "val", "other"]
    assert result.to_pydict() == {"id": [2], "val": [20], "other": [200]}


def test_left_outer_join() -> None:
    left = pa.table({"id": [1, 2], "val": [10, 20]})
    right = pa.table({"key": [2, 3], "other": [200, 300]})
    result = join(left, right, "id", "key", "left outer").sort_by("id")
    assert result.to_pydict() == {
        "id": [1, 2],
        "val": [10, 20],
        "other": [None, 200],
    }


def test_missing_left_key() -> None:
    left = pa.table({"id": [1]})
    right = pa.table({"key": [1]})
    with pytest.raises(KeyError):
        join(left, right, "missing", "key")


def test_missing_right_key() -> None:
    left = pa.table({"id": [1]})
    right = pa.table({"key": [1]})
    with pytest.raises(KeyError):
        join(left, right, "id", "missing")


def test_suffixes() -> None:
    left = pa.table({"id": [1], "val": [10]})
    right = pa.table({"key": [1], "val": [20]})
    result = join(left, right, "id", "key")
    assert result.column_names == ["id", "val", "val_right"]
    assert result["val"].to_pylist() == [10]
    assert result["val_right"].to_pylist() == [20]
