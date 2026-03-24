import numpy as np

from barrow.operations._env import build_env


def test_build_env(sample_table):
    env = build_env(sample_table)
    assert set(sample_table.column_names).issubset(env)
    assert env["sqrt"] is np.sqrt
    assert env["a"].tolist() == [1, 2, 3]


def test_build_env_selective_columns(sample_table):
    """Only requested columns are eagerly converted; others are lazy."""
    env = build_env(sample_table, columns={"a"})
    # Eagerly converted
    assert env["a"].tolist() == [1, 2, 3]
    # Lazily resolved from table on access
    assert env["b"].tolist() == [4, 5, 6]
    # NumPy functions still accessible
    assert env["sqrt"] is np.sqrt


def test_build_env_lazy_numpy(sample_table):
    """NumPy functions are resolved lazily, not eagerly populated."""
    env = build_env(sample_table)
    # Should resolve on access without being pre-populated
    assert callable(env["sin"])
    assert env["pi"] == np.pi
    # Non-existent keys raise KeyError
    import pytest

    with pytest.raises(KeyError):
        env["nonexistent_xyz_42"]
