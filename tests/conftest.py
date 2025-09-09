import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.orc as orc
import pytest


@pytest.fixture
def sample_table() -> pa.Table:
    """Small example table used in tests."""
    return pa.table({"a": [1, 2, 3], "b": [4, 5, 6], "grp": ["x", "x", "y"]})


@pytest.fixture
def sample_csv(tmp_path, sample_table) -> str:
    """Path to a CSV file with ``sample_table`` data."""
    path = tmp_path / "data.csv"
    csv.write_csv(sample_table, path)
    return str(path)


@pytest.fixture
def sample_parquet(tmp_path, sample_table) -> str:
    """Path to a Parquet file with ``sample_table`` data."""
    path = tmp_path / "data.parquet"
    pq.write_table(sample_table, path)
    return str(path)


@pytest.fixture
def sample_orc(tmp_path, sample_table) -> str:
    """Path to an ORC file with ``sample_table`` data."""
    path = tmp_path / "data.orc"
    orc.write_table(sample_table, path)
    return str(path)


@pytest.fixture
def parquet_table(sample_parquet) -> pa.Table:
    """Table loaded from the ``sample_parquet`` fixture."""
    return pq.read_table(sample_parquet)


@pytest.fixture
def orc_table(sample_orc) -> pa.Table:
    """Table loaded from the ``sample_orc`` fixture."""
    return orc.read_table(sample_orc)
