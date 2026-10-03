#!/usr/bin/env bash
# Compare the cost (bytes of intermediate data written) of unoptimized vs. optimized
# plans for every query in benchmarks/queries.sql, using the LOCAL execution environment.
#
# Usage (from anywhere, after `unzip data/data_0_01.zip -d data`):
#   benchmarks/run_benchmark.sh
# Optional overrides: DATA_DIR=<dir with TPC-H *.json>  OUT=<results csv>
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
OUT="${OUT:-$ROOT/benchmarks/results.csv}"
QUERIES="$ROOT/benchmarks/queries.sql"

if [ ! -f "$DATA_DIR/CUSTOMER.json" ]; then
    echo "TPC-H data not found in $DATA_DIR. Run: unzip data/data_0_01.zip -d data" >&2
    exit 1
fi

# miniHive reads <TABLE>.json and writes tmp*.tmp in the current directory.
cd "$DATA_DIR"

echo "Query,Unoptimized,Optimized,Reduction%" > "$OUT"

while IFS= read -r query; do
    [ -z "${query// }" ] && continue

    unopt_cost=$(python3 "$ROOT/src/minihive.py" --env LOCAL "$query" 2>/dev/null | tail -1)
    opt_cost=$(python3 "$ROOT/src/minihive.py" --O --env LOCAL "$query" 2>/dev/null | tail -1)
    reduction=$(awk -v u="$unopt_cost" -v o="$opt_cost" \
        'BEGIN { if (u > 0) printf "%.0f%%", (u - o) / u * 100; else print "N/A" }')

    echo "\"$query\",$unopt_cost,$opt_cost,$reduction" >> "$OUT"
    printf '%s\n  unoptimized: %s\n  optimized:   %s\n  reduction:   %s\n\n' \
        "$query" "$unopt_cost" "$opt_cost" "$reduction"
done < "$QUERIES"

rm -f ./*.tmp
echo "Results written to $OUT"
