#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/benchmark.sh [options]

Benchmark common barrow operations against generated datasets.
The script creates small, medium, and large CSV fixtures automatically and
measures individual commands plus pipeline variants with and without --tmp,
including SQL alternatives where they are useful.

Options:
  --workspace DIR     Directory for generated data and outputs.
                      Default: .benchmarks
  --sizes SPEC        Dataset sizes as small:medium:large row counts.
                      Default: 1000:50000:200000
  --iterations N      Number of measured runs per benchmark. Default: 3
  --warmup N          Number of warmup runs per benchmark. Default: 1
  --barrow-cmd CMD    Command used to invoke barrow.
                      Default: barrow, or 'python -m barrow.cli' if unavailable
  --datasets LIST     Comma-separated dataset labels to run: small,medium,large
                      Default: small,medium,large
  --only LIST         Comma-separated benchmark groups to run.
                      Available: filter,select,mutate,groupby,summary,ungroup,
                      sort,window,sql,join,pipeline
  --cleanup           Remove generated CSVs and output artifacts after the run.
  --help              Show this help.

Examples:
  scripts/benchmark.sh
  scripts/benchmark.sh --iterations 5 --datasets medium,large
  scripts/benchmark.sh --only filter,sql,pipeline --workspace /tmp/barrow-bench
USAGE
}

workspace=".benchmarks"
sizes_spec="1000:50000:200000"
iterations=3
warmup=1
barrow_cmd=""
datasets="small,medium,large"
only=""
cleanup=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace)
      workspace="$2"
      shift 2
      ;;
    --sizes)
      sizes_spec="$2"
      shift 2
      ;;
    --iterations)
      iterations="$2"
      shift 2
      ;;
    --warmup)
      warmup="$2"
      shift 2
      ;;
    --barrow-cmd)
      barrow_cmd="$2"
      shift 2
      ;;
    --datasets)
      datasets="$2"
      shift 2
      ;;
    --only)
      only="$2"
      shift 2
      ;;
    --cleanup)
      cleanup=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$barrow_cmd" ]]; then
  if command -v barrow >/dev/null 2>&1; then
    barrow_cmd="barrow"
  else
    barrow_cmd="python -m barrow.cli"
  fi
fi

if [[ "$sizes_spec" != *:*:* ]]; then
  echo "--sizes must use small:medium:large" >&2
  exit 1
fi
IFS=':' read -r rows_small rows_medium rows_large <<< "$sizes_spec"

for value in "$iterations" "$warmup" "$rows_small" "$rows_medium" "$rows_large"; do
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "Numeric option expected, got: $value" >&2
    exit 1
  fi
done

allowed_datasets=(small medium large)
allowed_benchmarks=(filter select mutate groupby summary ungroup sort window sql join pipeline)

contains_csv_item() {
  local needle="$1"
  local haystack="$2"
  local item
  IFS=',' read -r -a __items <<< "$haystack"
  for item in "${__items[@]}"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

for dataset in ${datasets//,/ }; do
  if ! contains_csv_item "$dataset" "small,medium,large"; then
    echo "Invalid dataset label: $dataset" >&2
    exit 1
  fi
done

if [[ -n "$only" ]]; then
  for bench in ${only//,/ }; do
    if ! contains_csv_item "$bench" "filter,select,mutate,groupby,summary,ungroup,sort,window,sql,join,pipeline"; then
      echo "Invalid benchmark group: $bench" >&2
      exit 1
    fi
  done
fi

mkdir -p "$workspace"
workspace="$(cd "$workspace" && pwd)"
results_file="$workspace/results.tsv"
: > "$results_file"
printf 'dataset	rows	benchmark	variant	seconds\n' >> "$results_file"

echo "==> Workspace: $workspace"
echo "==> Command: $barrow_cmd"
echo "==> Sizes: small=$rows_small medium=$rows_medium large=$rows_large"
echo "==> Iterations: $iterations (warmup=$warmup)"

python - <<PY "$workspace" "$rows_small" "$rows_medium" "$rows_large"
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys

workspace = pathlib.Path(sys.argv[1])
row_counts = {
    "small": int(sys.argv[2]),
    "medium": int(sys.argv[3]),
    "large": int(sys.argv[4]),
}

for label, rows in row_counts.items():
    data_path = workspace / f"{label}.csv"
    join_path = workspace / f"{label}_right.csv"
    with data_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "grp", "a", "b", "value", "flag", "category", "ts"])
        base = dt.date(2024, 1, 1)
        for i in range(rows):
            grp = f"g{i % 10}"
            a = (i * 7) % 997
            b = (i * 11) % 389
            value = ((i * 17) % 1000) / 10
            flag = "1" if i % 3 == 0 else "0"
            category = f"cat{i % 5}"
            ts = (base + dt.timedelta(days=i % 365)).isoformat()
            writer.writerow([i, grp, a, b, f"{value:.1f}", flag, category, ts])
    with join_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "segment", "weight"])
        for i in range(rows):
            writer.writerow([i, f"seg{i % 4}", (i * 13) % 101])
PY

run_cmd() {
  local command="$1"
  bash -lc "$command"
}

measure() {
  local dataset="$1"
  local rows="$2"
  local benchmark="$3"
  local variant="$4"
  local command="$5"
  local elapsed
  local run

  for ((run = 0; run < warmup; run++)); do
    run_cmd "$command" >/dev/null 2>&1
  done

  for ((run = 1; run <= iterations; run++)); do
    elapsed=$(python - <<'PY' "$command"
from __future__ import annotations
import subprocess
import sys
import time

command = sys.argv[1]
start = time.perf_counter()
subprocess.run(["bash", "-lc", command], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
end = time.perf_counter()
print(f"{end - start:.6f}")
PY
)
    printf '%s\t%s\t%s\t%s\t%s\n' "$dataset" "$rows" "$benchmark" "$variant" "$elapsed" >> "$results_file"
    printf '[%s] %-8s %-12s %-16s run=%d/%d %ss\n' "$dataset" "$rows" "$benchmark" "$variant" "$run" "$iterations" "$elapsed"
  done
}

should_run() {
  local benchmark="$1"
  [[ -z "$only" ]] && return 0
  contains_csv_item "$benchmark" "$only"
}

benchmark_dataset() {
  local dataset="$1"
  local rows="$2"
  local input="$workspace/${dataset}.csv"
  local right_input="$workspace/${dataset}_right.csv"
  local outdir="$workspace/out/$dataset"
  mkdir -p "$outdir"

  if should_run filter; then
    measure "$dataset" "$rows" filter direct \
      "$barrow_cmd filter 'a > 400' -i '$input' -o '$outdir/filter.csv'"
    measure "$dataset" "$rows" filter sql_equivalent \
      "$barrow_cmd sql \"SELECT * FROM tbl WHERE a > 400\" -i '$input' -o '$outdir/filter_sql.csv'"
  fi

  if should_run select; then
    measure "$dataset" "$rows" select direct \
      "$barrow_cmd select 'id,grp,a,value' -i '$input' -o '$outdir/select.csv'"
    measure "$dataset" "$rows" select sql_equivalent \
      "$barrow_cmd sql \"SELECT id, grp, a, value FROM tbl\" -i '$input' -o '$outdir/select_sql.csv'"
  fi

  if should_run mutate; then
    measure "$dataset" "$rows" mutate direct \
      "$barrow_cmd mutate 'total=a+b,scaled=value*2,tag=grp' -i '$input' -o '$outdir/mutate.csv'"
  fi

  if should_run groupby; then
    measure "$dataset" "$rows" groupby direct \
      "$barrow_cmd groupby 'grp,category' -i '$input' -o '$outdir/groupby.parquet' --parquet"
  fi

  if should_run summary; then
    measure "$dataset" "$rows" summary pipeline \
      "$barrow_cmd groupby 'grp,category' -i '$input' --tmp | $barrow_cmd summary 'a=sum,b=mean,id=count' --parquet -o '$outdir/summary.parquet'"
    measure "$dataset" "$rows" summary sql_equivalent \
      "$barrow_cmd sql \"SELECT grp, category, SUM(a) AS sum_a, AVG(b) AS avg_b, COUNT(id) AS rows FROM tbl GROUP BY grp, category\" -i '$input' --parquet -o '$outdir/summary_sql.parquet'"
  fi

  if should_run ungroup; then
    measure "$dataset" "$rows" ungroup grouped_roundtrip \
      "$barrow_cmd groupby 'grp' -i '$input' --parquet -o '$outdir/grouped_for_ungroup.parquet' && $barrow_cmd ungroup -i '$outdir/grouped_for_ungroup.parquet' --parquet -o '$outdir/ungroup.parquet'"
  fi

  if should_run sort; then
    measure "$dataset" "$rows" sort ascending \
      "$barrow_cmd sort 'grp,a' -i '$input' -o '$outdir/sort.csv'"
    measure "$dataset" "$rows" sort descending \
      "$barrow_cmd sort 'value' --desc -i '$input' -o '$outdir/sort_desc.csv'"
  fi

  if should_run window; then
    measure "$dataset" "$rows" window partitioned \
      "$barrow_cmd window 'rn=row_number()' --by grp --order-by ts -i '$input' --parquet -o '$outdir/window.parquet'"
  fi

  if should_run sql; then
    measure "$dataset" "$rows" sql analytic \
      "$barrow_cmd sql \"SELECT id, grp, a, value, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn FROM tbl WHERE value >= 30\" -i '$input' --parquet -o '$outdir/sql.parquet'"
  fi

  if should_run join; then
    measure "$dataset" "$rows" join inner \
      "$barrow_cmd join id id --right '$right_input' --right-format csv -i '$input' --parquet -o '$outdir/join.parquet'"
  fi

  if should_run pipeline; then
    measure "$dataset" "$rows" pipeline standard_pipe \
      "$barrow_cmd filter 'a > 200' -i '$input' | $barrow_cmd mutate 'total=a+b' | $barrow_cmd select 'id,grp,total,category' | $barrow_cmd sort 'grp,total' --parquet -o '$outdir/pipeline.parquet'"
    measure "$dataset" "$rows" pipeline tmp_pipe \
      "$barrow_cmd filter 'a > 200' -i '$input' --tmp | $barrow_cmd mutate 'total=a+b' --tmp | $barrow_cmd select 'id,grp,total,category' --tmp | $barrow_cmd sort 'grp,total' --parquet -o '$outdir/pipeline_tmp.parquet'"
    measure "$dataset" "$rows" pipeline sql_vs_commands \
      "$barrow_cmd sql \"SELECT id, grp, a + b AS total, category FROM tbl WHERE a > 200 ORDER BY grp, total\" -i '$input' --parquet -o '$outdir/pipeline_sql.parquet'"
  fi
}

for dataset in ${datasets//,/ }; do
  case "$dataset" in
    small) benchmark_dataset small "$rows_small" ;;
    medium) benchmark_dataset medium "$rows_medium" ;;
    large) benchmark_dataset large "$rows_large" ;;
  esac
done

python - <<'PY' "$results_file"
from __future__ import annotations

import collections
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
rows = path.read_text().strip().splitlines()
header, *data = rows
summary = collections.defaultdict(list)
for row in data:
    dataset, nrows, benchmark, variant, seconds = row.split("\t")
    summary[(dataset, benchmark, variant)].append(float(seconds))

print("\n==> Summary (avg seconds)")
for (dataset, benchmark, variant) in sorted(summary):
    values = summary[(dataset, benchmark, variant)]
    avg = sum(values) / len(values)
    print(f"{dataset:>6} | {benchmark:<10} | {variant:<16} | {avg:.6f}s")
PY

printf '\nResults written to: %s\n' "$results_file"

if [[ "$cleanup" -eq 1 ]]; then
  rm -rf "$workspace/out" "$workspace/small.csv" "$workspace/medium.csv" "$workspace/large.csv" \
         "$workspace/small_right.csv" "$workspace/medium_right.csv" "$workspace/large_right.csv"
  echo "Generated fixtures and outputs were removed; results were kept in $results_file"
else
  echo "Generated fixtures and outputs were kept in $workspace for inspection."
fi
