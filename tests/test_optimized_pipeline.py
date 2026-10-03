"""Regression tests for milestone 4: the optimized pipeline must return exactly the same rows
as the unoptimized one while writing less intermediate data.

Runs every benchmark query from benchmarks/queries.sql against a small TPC-H sample
(tests/fixtures/tpch_sample) using the LOCAL execution environment.
"""
import json
import shutil
from pathlib import Path

import pytest

import costcounter
import minihive
import ra2mr

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = Path(__file__).resolve().parent / "fixtures" / "tpch_sample"
QUERIES = [q.strip() for q in (ROOT / "benchmarks" / "queries.sql").read_text().splitlines() if q.strip()]


@pytest.fixture
def tpch_dir(tmp_path, monkeypatch):
    for f in SAMPLE.glob("*.json"):
        shutil.copy(f, tmp_path)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def run(query, optimize):
    minihive.clear_local_tmpfiles()
    task = minihive.evaluate(ra2mr.ExecEnv.LOCAL, query, minihive.TPCH_SCHEMA, optimize)
    with task.output().open("r") as f:
        rows = {json.dumps(json.loads(line.split("\t")[1]), sort_keys=True) for line in f}
    return rows, costcounter.compute_hdfs_costs()


@pytest.mark.parametrize("query", QUERIES, ids=[f"q{i + 1}" for i in range(len(QUERIES))])
def test_optimized_matches_unoptimized_and_is_cheaper(tpch_dir, query):
    plain_rows, plain_cost = run(query, optimize=False)
    opt_rows, opt_cost = run(query, optimize=True)

    assert plain_rows, "sample data should produce a non-empty result"
    assert opt_rows == plain_rows
    assert opt_cost < plain_cost


def test_point_lookup_cost_matches_benchmark(tpch_dir):
    # Customer 42 is in the sample, so this query's cost equals the full-dataset benchmark numbers.
    query = "select distinct C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42"
    assert run(query, optimize=False)[1] == 448
    assert run(query, optimize=True)[1] == 77
