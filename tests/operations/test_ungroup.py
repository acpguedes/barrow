import logging

from barrow.operations import groupby, ungroup


def test_ungroup_removes_metadata(sample_table, caplog):
    with caplog.at_level(logging.DEBUG):
        gb = groupby(sample_table, "grp")
        assert (gb.schema.metadata or {}).get(b"grouped_by") == b"grp"
        result = ungroup(gb)
    assert (result.schema.metadata or {}).get(b"grouped_by") is None
    assert "Ungrouping table" in caplog.text

