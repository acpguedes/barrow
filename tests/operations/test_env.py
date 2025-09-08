import numpy as np

from barrow.operations._env import build_env


def test_build_env(sample_table):
    env = build_env(sample_table)
    assert set(sample_table.column_names).issubset(env)
    assert env["sqrt"] is np.sqrt
    assert env["a"].tolist() == [1, 2, 3]

