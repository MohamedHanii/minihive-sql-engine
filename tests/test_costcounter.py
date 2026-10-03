"""Tests for the cost metric (bytes of intermediate JSON written to *.tmp files)."""
import json

import pytest

import costcounter


def test_no_tmp_files_costs_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert costcounter.compute_hdfs_costs() == 0


def test_cost_is_sum_of_serialized_tuple_lengths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    t1 = {"Person.name": "Amy"}
    t2 = {"Person.name": "Ben", "Person.age": 21}
    (tmp_path / "tmp1.tmp").write_text(f"Person\t{json.dumps(t1)}\nPerson\t{json.dumps(t2)}\n")
    (tmp_path / "tmp2.tmp").write_text(f"Person\t{json.dumps(t1)}\n")
    (tmp_path / "Person.json").write_text(f"Person\t{json.dumps(t2)}\n")  # input data is free

    expected = 2 * len(json.dumps(t1)) + len(json.dumps(t2))
    assert costcounter.compute_hdfs_costs() == expected


def test_malformed_tuple_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "tmp1.tmp").write_text("Person\tnot json\n")
    with pytest.raises(json.JSONDecodeError):
        costcounter.compute_hdfs_costs()
