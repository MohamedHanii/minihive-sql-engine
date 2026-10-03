# miniHive SQL Engine

[![tests](https://github.com/MohamedHanii/minihive-sql-engine/actions/workflows/tests.yml/badge.svg)](https://github.com/MohamedHanii/minihive-sql-engine/actions/workflows/tests.yml)

A small SQL-on-MapReduce query engine written in Python. miniHive takes a SQL query, translates it into relational algebra, rewrites it with logical optimization rules, and compiles the result into a chain of MapReduce jobs that run on Hadoop (or locally) through [Luigi](https://github.com/spotify/luigi).

```
SQL ──► sql2ra ──► raopt ──► ra2mr ──► Luigi / Hadoop MapReduce
        (parse)    (optimize)  (compile to jobs)
```

## Features

| Stage | Module | What it does |
|-------|--------|--------------|
| SQL → RA | [`src/sql2ra.py`](src/sql2ra.py) | Canonical translation of `SELECT DISTINCT … FROM … WHERE …` into projection, selection, renaming and cross product (using [radb](https://pypi.org/project/radb/) ASTs). |
| Logical optimization | [`src/raopt.py`](src/raopt.py) | Break up conjunctive selections, push selections down, merge selections, turn selection-over-cross-product into joins, and push projections down. |
| RA → MapReduce | [`src/ra2mr.py`](src/ra2mr.py) | One Luigi `JobTask` per operator (select, project, rename, join), plus optimized tasks that fuse several operators into one job. |
| Cost metric | [`src/costcounter.py`](src/costcounter.py) | Measures the bytes of intermediate data written between jobs. |
| CLI | [`src/minihive.py`](src/minihive.py) | Runs the whole pipeline over the TPC-H schema. |

### Physical optimizations (`--O`)

- **Projection push-down**: unused columns are dropped right at the base tables, so less data moves between jobs.
- **`MapperChainTask`**: a chain of selections, projections and renames runs as a single map-only job, with no temporary file after every operator.
- **`EnhancedJoinTask`**: the mapper walks the query-plan branch each input tuple belongs to and applies that branch's selections, renames and projections in memory before the shuffle. Several RA operators are folded into the join's single MapReduce job.

## Benchmark

Bytes of intermediate data written for each query in [`benchmarks/queries.sql`](benchmarks/queries.sql), on TPC-H scale factor 0.01 (raw numbers in [`benchmarks/results.csv`](benchmarks/results.csv)):

| # | Query (abridged) | Unoptimized | Optimized | Reduction |
|---|------------------|------------:|----------:|----------:|
| 1 | `C_NAME, C_ADDRESS FROM CUSTOMER WHERE C_CUSTKEY=42` | 448 | 77 | 83% |
| 2 | `C.C_NAME, C.C_ADDRESS FROM CUSTOMER C WHERE C.C_NATIONKEY=7` | 467,940 | 4,392 | 99% |
| 3 | `* FROM CUSTOMER ⋈ NATION WHERE N_NAME='GERMANY'` | 29,683 | 29,523 | 1% |
| 4 | `C_CUSTKEY FROM CUSTOMER ⋈ NATION WHERE N_NAME='GERMANY'` | 31,240 | 7,674 | 75% |
| 5 | `C_CUSTKEY FROM CUSTOMER ⋈ NATION WHERE C_CUSTKEY=42` | 902 | 104 | 88% |
| 6 | `C_CUSTKEY FROM CUSTOMER ⋈ NATION ⋈ REGION` | 1,849,921 | 394,783 | 79% |
| 7 | `C_CUSTKEY FROM REGION ⋈ NATION ⋈ CUSTOMER` | 1,061,077 | 238,478 | 78% |
| 8 | `* FROM ORDERS ⋈ CUSTOMER WHERE O_ORDERPRIORITY='1-URGENT'` | 3,055,418 | 2,062,404 | 33% |
| 9 | `* FROM CUSTOMER ⋈ ORDERS ⋈ LINEITEM WHERE AIR / HOUSEHOLD` | 8,513,358 | 3,911,209 | 54% |
| 10 | `* FROM LINEITEM ⋈ ORDERS ⋈ CUSTOMER WHERE AIR / HOUSEHOLD` | 13,912,617 | 9,310,468 | 33% |

## Getting started

Requires Python 3.10+.

```bash
git clone https://github.com/MohamedHanii/minihive-sql-engine.git
cd minihive-sql-engine

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Extract the TPC-H sample data (scale factor 0.01) into data/
unzip data/data_0_01.zip -d data
```

> Luigi is pinned below 3.7, because later releases removed a module that `luigi.contrib.hadoop` still imports.

### Run a query locally

The `LOCAL` environment reads `<TABLE>.json` from the current directory and writes intermediate results to `tmp*.tmp` files there, so run it from `data/`:

```bash
cd data
python ../src/minihive.py --env LOCAL "select distinct C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42"
python ../src/minihive.py --env LOCAL --O "select distinct C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42"
```

The command prints the final relational algebra plan and the Luigi log. The last line is the cost (bytes of intermediate data). The final result is in `tmp1.tmp`.

| Flag | Meaning |
|------|---------|
| `--O` | Enable projection push-down and the fused MapReduce tasks |
| `--env {HDFS,LOCAL}` | Execution environment (default `HDFS`) |
| `--SF <float>` | TPC-H scale factor (informational) |

### Run on Hadoop

With `--env HDFS` (the default), tables are read from HDFS and the jobs are submitted to a Hadoop cluster through Luigi. This needs a working Hadoop installation, for example the course's `minihive-docker` image:

```bash
docker run -d --name minihive -v "$(pwd)":/home/minihive/minihive minihive-docker
docker exec -it minihive bash
```

### Reproduce the benchmark

```bash
benchmarks/run_benchmark.sh   # writes benchmarks/results.csv
```

## Supported SQL

```sql
SELECT DISTINCT <* | col, Table.col, Alias.col, ...>
FROM   Table [Alias], ...
[WHERE a = b AND c = 'literal' AND d = 42 ...]
```

Only conjunctions of equality comparisons are supported in `WHERE`. String literals must use single quotes.

## Tests

```bash
pytest
```

The suite also runs on GitHub Actions (Python 3.10 and 3.12) on every push.

| File | Covers |
|------|--------|
| `test_sql2ra.py` | SQL → RA translation, including parsing edge cases |
| `test_raopt.py`, `test_raopt_extended.py` | Selection break-up, push-down, merge, and join introduction |
| `test_push_down_projections.py` | Projection push-down rule |
| `test_mapper_helper.py` | Tuple-level selection, rename, projection, and join-key logic |
| `test_ra2mr.py` | MapReduce evaluation of RA queries on Luigi's in-memory file system |
| `test_end_to_end.py` | SQL → results on the pizza example database, with and without `--O` |
| `test_optimized_pipeline.py` | Every benchmark query on a TPC-H sample: optimized results must equal unoptimized results, at a lower cost |
| `test_costcounter.py` | Cost metric |

## Project layout

```
.
├── src/                    # miniHive source (sql2ra, raopt, ra2mr, costcounter, CLI)
├── tests/                  # pytest suite
│   └── fixtures/tpch_sample/   # ~200 KB TPC-H subset used by the tests
├── benchmarks/             # benchmark queries, script, and results
├── .github/workflows/      # CI: runs pytest on push
├── data/data_0_01.zip      # TPC-H data, scale factor 0.01
├── requirements.txt
└── pytest.ini
```

## Acknowledgements

Built as a university databases course project, in four milestones (SQL → RA, logical optimization, RA → MapReduce, query optimization). The course provided the project skeleton, the cost counter, the base test suites and the TPC-H data. The relational algebra parser is [radb](https://github.com/junyang/radb) by Jun Yang.

## Author

Mohamed Osman
