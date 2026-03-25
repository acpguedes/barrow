#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/benchmark.sh [options]

Benchmark common barrow operations against generated datasets.
The script creates deterministic CSV fixtures automatically for the requested
dataset sizes and measures individual commands plus pipeline variants with explicit cold,
warmup, and hot phases. It also adds SQL equivalents for the basic operations
where they make sense, records wall time plus resource metrics, and writes
detailed machine-readable results plus a final summary to the selected workspace.

Options:
  --workspace DIR     Directory for generated data and outputs.
                      Default: .benchmarks
  --sizes SPEC        Dataset sizes as either small:medium:large or
                      tiny:small:medium:large:xlarge row counts.
                      Default core sizes: 1000:50000:200000
                      Optional dataset defaults: tiny=100 xlarge=1000000
  --iterations N      Number of runs per enabled phase per benchmark.
                      Default: 3
  --warmup N          Enable warmup when N > 0. The warmup phase uses the
                      same run count as --iterations. Default: 1
  --cold-runs N       Enable cold when N > 0. The cold phase uses the same
                      run count as --iterations. Default: 1
  --barrow-cmd CMD    Command used to invoke barrow.
                      Default: barrow, or 'python -m barrow.cli' if unavailable
  --datasets LIST     Comma-separated dataset labels to run:
                      tiny,small,medium,large,xlarge
                      Default: small,medium,large
  --only LIST         Comma-separated benchmark groups to run.
                      Available: filter,select,mutate,groupby,summary,ungroup,
                      sort,window,sql,join,pipeline
  --cleanup           Remove generated CSVs and output artifacts after the run.
  --help              Show this help.

Phases:
  cold    First measured runs on a cleaned output directory
  warmup  Preparatory runs executed before hot measurements
  hot     Repeated measured runs after warmup, best for steady-state analysis

Examples:
  scripts/benchmark.sh
  scripts/benchmark.sh --iterations 5 --warmup 1 --datasets medium,large
  scripts/benchmark.sh --only filter,mutate,sql,pipeline --workspace /tmp/barrow-bench
USAGE
}

workspace=".benchmarks"
sizes_spec="1000:50000:200000"
iterations=3
warmup=1
cold_runs=1
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
    --cold-runs)
      cold_runs="$2"
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

IFS=':' read -r -a size_parts <<< "$sizes_spec"
case "${#size_parts[@]}" in
  3)
    rows_tiny=100
    rows_small="${size_parts[0]}"
    rows_medium="${size_parts[1]}"
    rows_large="${size_parts[2]}"
    rows_xlarge=1000000
    ;;
  5)
    rows_tiny="${size_parts[0]}"
    rows_small="${size_parts[1]}"
    rows_medium="${size_parts[2]}"
    rows_large="${size_parts[3]}"
    rows_xlarge="${size_parts[4]}"
    ;;
  *)
    echo "--sizes must use small:medium:large or tiny:small:medium:large:xlarge" >&2
    exit 1
    ;;
esac

for value in "$iterations" "$warmup" "$cold_runs" "$rows_tiny" "$rows_small" "$rows_medium" "$rows_large" "$rows_xlarge"; do
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "Numeric option expected, got: $value" >&2
    exit 1
  fi
done

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
  if ! contains_csv_item "$dataset" "tiny,small,medium,large,xlarge"; then
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
summary_file="$workspace/summary.md"
summary_json="$workspace/summary.json"
config_file="$workspace/config.txt"
: > "$results_file"
printf 'dataset\trows\tbenchmark\tvariant\tphase\trun\tseconds\tpeak_rss_kb\tcpu_user_s\tcpu_sys_s\tcpu_pct\texit_code\tcommand\n' >> "$results_file"

cat > "$config_file" <<CONFIG
workspace=$workspace
barrow_cmd=$barrow_cmd
sizes=$sizes_spec
rows_tiny=$rows_tiny
rows_small=$rows_small
rows_medium=$rows_medium
rows_large=$rows_large
rows_xlarge=$rows_xlarge
datasets=$datasets
only=${only:-all}
warmup=$warmup
cold_runs=$cold_runs
iterations=$iterations
CONFIG

echo "==> Workspace: $workspace"
echo "==> Command: $barrow_cmd"
echo "==> Sizes: tiny=$rows_tiny small=$rows_small medium=$rows_medium large=$rows_large xlarge=$rows_xlarge"
echo "==> Runs per enabled phase: $iterations"
echo "==> Enabled phases: cold=$([[ $cold_runs -gt 0 ]] && echo yes || echo no) warmup=$([[ $warmup -gt 0 ]] && echo yes || echo no) hot=yes"

echo "==> Generating fixtures"
python - <<PY "$workspace" "$rows_tiny" "$rows_small" "$rows_medium" "$rows_large" "$rows_xlarge"
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys

workspace = pathlib.Path(sys.argv[1])
row_counts = {
    "tiny": int(sys.argv[2]),
    "small": int(sys.argv[3]),
    "medium": int(sys.argv[4]),
    "large": int(sys.argv[5]),
    "xlarge": int(sys.argv[6]),
}

for label, rows in row_counts.items():
    data_path = workspace / f"{label}.csv"
    join_path = workspace / f"{label}_right.csv"
    with data_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "id",
            "grp",
            "subgrp",
            "region",
            "a",
            "b",
            "value",
            "flag",
            "category",
            "priority",
            "score",
            "ts",
        ])
        base = dt.date(2024, 1, 1)
        for i in range(rows):
            grp = f"g{i % 10}"
            subgrp = f"sg{i % 25}"
            region = ("north", "south", "east", "west")[i % 4]
            a = (i * 7) % 997
            b = (i * 11) % 389
            value = ((i * 17) % 1000) / 10
            flag = "1" if i % 3 == 0 else "0"
            category = f"cat{i % 5}"
            priority = (i * 5) % 9
            score = round(((i * 19) % 10000) / 137.0, 4)
            ts = (base + dt.timedelta(days=i % 365)).isoformat()
            writer.writerow([
                i,
                grp,
                subgrp,
                region,
                a,
                b,
                f"{value:.1f}",
                flag,
                category,
                priority,
                f"{score:.4f}",
                ts,
            ])
    with join_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "segment", "weight", "status"])
        for i in range(rows):
            writer.writerow([i, f"seg{i % 4}", (i * 13) % 101, ("new", "active", "hold")[i % 3]])
PY

run_cmd() {
  local command="$1"
  bash -lc "$command"
}

cleanup_outputs() {
  local outdir="$1"
  if [[ -d "$outdir" ]]; then
    find "$outdir" -mindepth 1 -maxdepth 1 -type f -delete
  fi
}

record_timing() {
  local dataset="$1"
  local rows="$2"
  local benchmark="$3"
  local variant="$4"
  local phase="$5"
  local run_id="$6"
  local elapsed="$7"
  local peak_rss_kb="$8"
  local cpu_user_s="$9"
  local cpu_sys_s="${10}"
  local cpu_pct="${11}"
  local exit_code="${12}"
  local command="${13}"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$dataset" "$rows" "$benchmark" "$variant" "$phase" "$run_id" "$elapsed" "$peak_rss_kb" "$cpu_user_s" "$cpu_sys_s" "$cpu_pct" "$exit_code" "$command" >> "$results_file"
}

measure_phase() {
  local dataset="$1"
  local rows="$2"
  local benchmark="$3"
  local variant="$4"
  local phase="$5"
  local count="$6"
  local outdir="$7"
  local command="$8"
  local elapsed
  local peak_rss_kb
  local cpu_user_s
  local cpu_sys_s
  local cpu_pct
  local exit_code
  local run

  [[ "$count" -eq 0 ]] && return 0

  for ((run = 1; run <= count; run++)); do
    cleanup_outputs "$outdir"
    IFS=$'\t' read -r elapsed peak_rss_kb cpu_user_s cpu_sys_s cpu_pct exit_code <<< "$(python - <<'PY' "$command"
from __future__ import annotations
import os
import pathlib
import resource
import shutil
import subprocess
import sys
import tempfile
import time

command = sys.argv[1]

# Detect GNU time: check /usr/bin/time, gtime (macOS Homebrew), /usr/local/bin/gtime
gnu_time_bin = None
for candidate in ["/usr/bin/time", "gtime", "/usr/local/bin/gtime"]:
    resolved = shutil.which(candidate)
    if resolved:
        try:
            result = subprocess.run(
                [resolved, "--version"],
                capture_output=True, text=True, timeout=5,
            )
            if "GNU" in (result.stdout + result.stderr).upper():
                gnu_time_bin = resolved
                break
        except (subprocess.TimeoutExpired, OSError):
            continue

peak_rss_kb = 0
cpu_user_s = 0.0
cpu_sys_s = 0.0
cpu_pct = 0

if gnu_time_bin:
    # Use GNU time with format strings (more robust than -v + regex parsing)
    # %e = wall clock (seconds), %M = max RSS (KB), %U = user CPU (seconds),
    # %S = system CPU (seconds), %P = CPU percentage, %x = exit code
    metrics_fd, metrics_path = tempfile.mkstemp(prefix="barrow_time_")
    os.close(metrics_fd)
    try:
        start = time.perf_counter()
        proc = subprocess.run(
            [gnu_time_bin, "-f", "%e\t%M\t%U\t%S\t%P\t%x",
             "-o", metrics_path,
             "bash", "-lc", command],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        end = time.perf_counter()
        elapsed = end - start

        metrics_text = pathlib.Path(metrics_path).read_text().strip()
        if metrics_text:
            parts = metrics_text.split("\t")
            if len(parts) >= 6:
                # Use Python perf_counter for wall time (higher precision)
                peak_rss_kb = int(parts[1]) if parts[1] else 0
                cpu_user_s = float(parts[2]) if parts[2] else 0.0
                cpu_sys_s = float(parts[3]) if parts[3] else 0.0
                # cpu_pct may have trailing '%' or be '?'
                pct_raw = parts[4].rstrip("%")
                cpu_pct = int(pct_raw) if pct_raw.isdigit() else 0
                exit_code = int(parts[5]) if parts[5].isdigit() else proc.returncode
            else:
                exit_code = proc.returncode
        else:
            exit_code = proc.returncode
    finally:
        pathlib.Path(metrics_path).unlink(missing_ok=True)
else:
    # Fallback: resource.getrusage for CPU/memory of child processes
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.perf_counter()
    proc = subprocess.run(
        ["bash", "-lc", command],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    end = time.perf_counter()
    elapsed = end - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    peak_rss_kb = int(after.ru_maxrss)
    cpu_user_s = max(0.0, after.ru_utime - before.ru_utime)
    cpu_sys_s = max(0.0, after.ru_stime - before.ru_stime)
    cpu_total = cpu_user_s + cpu_sys_s
    cpu_pct = int(cpu_total / elapsed * 100) if elapsed > 0 else 0
    exit_code = proc.returncode

print(f"{elapsed:.6f}\t{peak_rss_kb}\t{cpu_user_s:.6f}\t{cpu_sys_s:.6f}\t{cpu_pct}\t{exit_code}")
PY
)"
    record_timing "$dataset" "$rows" "$benchmark" "$variant" "$phase" "$run" \
      "$elapsed" "$peak_rss_kb" "$cpu_user_s" "$cpu_sys_s" "$cpu_pct" "$exit_code" "$command"
    printf '[%s] rows=%-8s bench=%-10s variant=%-18s phase=%-6s run=%d/%d %ss rss=%skB user=%ss sys=%ss cpu=%s%%\n' \
      "$dataset" "$rows" "$benchmark" "$variant" "$phase" "$run" "$count" "$elapsed" "$peak_rss_kb" "$cpu_user_s" "$cpu_sys_s" "$cpu_pct"
  done
}

measure() {
  local dataset="$1"
  local rows="$2"
  local benchmark="$3"
  local variant="$4"
  local outdir="$5"
  local command="$6"

  local cold_count=0
  local warmup_count=0

  if [[ "$cold_runs" -gt 0 ]]; then
    cold_count="$iterations"
  fi

  if [[ "$warmup" -gt 0 ]]; then
    warmup_count="$iterations"
  fi

  measure_phase "$dataset" "$rows" "$benchmark" "$variant" cold "$cold_count" "$outdir" "$command"
  measure_phase "$dataset" "$rows" "$benchmark" "$variant" warmup "$warmup_count" "$outdir" "$command"
  measure_phase "$dataset" "$rows" "$benchmark" "$variant" hot "$iterations" "$outdir" "$command"
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

  echo "==> Benchmarking dataset=$dataset rows=$rows"

  if should_run filter; then
    measure "$dataset" "$rows" filter direct "$outdir" \
      "$barrow_cmd filter 'a > 400' -i '$input' -o '$outdir/filter.csv'"
    measure "$dataset" "$rows" filter sql_equivalent_filter "$outdir" \
      "$barrow_cmd sql \"SELECT * FROM tbl WHERE a > 400\" -i '$input' -o '$outdir/filter_sql.csv'"
  fi

  if should_run select; then
    measure "$dataset" "$rows" select direct "$outdir" \
      "$barrow_cmd select 'id,grp,subgrp,region,a,value,score' -i '$input' -o '$outdir/select.csv'"
    measure "$dataset" "$rows" select sql_equivalent_select "$outdir" \
      "$barrow_cmd sql \"SELECT id, grp, subgrp, region, a, value, score FROM tbl\" -i '$input' -o '$outdir/select_sql.csv'"
  fi

  if should_run mutate; then
    measure "$dataset" "$rows" mutate direct "$outdir" \
      "$barrow_cmd mutate 'total=a+b,scaled=value*2,priority_band=priority+10,tag=grp' -i '$input' -o '$outdir/mutate.csv'"
    measure "$dataset" "$rows" mutate sql_equivalent_mutate "$outdir" \
      "$barrow_cmd sql \"SELECT *, a + b AS total, value * 2 AS scaled, priority + 10 AS priority_band, grp AS tag FROM tbl\" -i '$input' -o '$outdir/mutate_sql.csv'"
  fi

  if should_run groupby; then
    measure "$dataset" "$rows" groupby direct "$outdir" \
      "$barrow_cmd groupby 'grp,category,region' -i '$input' --parquet -o '$outdir/groupby.parquet'"
    measure "$dataset" "$rows" groupby sql_grouped_projection "$outdir" \
      "$barrow_cmd sql \"SELECT grp, category, region, COUNT(*) AS rows_in_group FROM tbl GROUP BY grp, category, region\" -i '$input' --parquet -o '$outdir/groupby_sql.parquet'"
  fi

  if should_run summary; then
    measure "$dataset" "$rows" summary pipeline "$outdir" \
      "$barrow_cmd groupby 'grp,category,region' -i '$input' --tmp | $barrow_cmd summary 'a=sum,b=mean,id=count,score=max' --parquet -o '$outdir/summary.parquet'"
    measure "$dataset" "$rows" summary sql_equivalent_summary "$outdir" \
      "$barrow_cmd sql \"SELECT grp, category, region, SUM(a) AS sum_a, AVG(b) AS avg_b, COUNT(id) AS rows, MAX(score) AS max_score FROM tbl GROUP BY grp, category, region\" -i '$input' --parquet -o '$outdir/summary_sql.parquet'"
  fi

  if should_run ungroup; then
    measure "$dataset" "$rows" ungroup grouped_roundtrip "$outdir" \
      "$barrow_cmd groupby 'grp' -i '$input' --parquet -o '$outdir/grouped_for_ungroup.parquet' && $barrow_cmd ungroup -i '$outdir/grouped_for_ungroup.parquet' --parquet -o '$outdir/ungroup.parquet'"
  fi

  if should_run sort; then
    measure "$dataset" "$rows" sort ascending "$outdir" \
      "$barrow_cmd sort 'grp,a,score' -i '$input' -o '$outdir/sort.csv'"
    measure "$dataset" "$rows" sort descending "$outdir" \
      "$barrow_cmd sort 'value' --desc -i '$input' -o '$outdir/sort_desc.csv'"
    measure "$dataset" "$rows" sort sql_equivalent_asc "$outdir" \
      "$barrow_cmd sql \"SELECT * FROM tbl ORDER BY grp, a, score\" -i '$input' -o '$outdir/sort_sql.csv'"
    measure "$dataset" "$rows" sort sql_equivalent_desc "$outdir" \
      "$barrow_cmd sql \"SELECT * FROM tbl ORDER BY value DESC\" -i '$input' -o '$outdir/sort_sql_desc.csv'"
  fi

  if should_run window; then
    measure "$dataset" "$rows" window partitioned "$outdir" \
      "$barrow_cmd window 'rn=row_number()' --by grp --order-by ts -i '$input' --parquet -o '$outdir/window.parquet'"
    measure "$dataset" "$rows" window sql_equivalent_partitioned "$outdir" \
      "$barrow_cmd sql \"SELECT *, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn FROM tbl\" -i '$input' --parquet -o '$outdir/window_sql.parquet'"
  fi

  if should_run sql; then
    measure "$dataset" "$rows" sql analytic "$outdir" \
      "$barrow_cmd sql \"SELECT id, grp, region, a, value, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn FROM tbl WHERE value >= 30 ORDER BY grp, rn\" -i '$input' --parquet -o '$outdir/sql.parquet'"
  fi

  if should_run join; then
    measure "$dataset" "$rows" join inner "$outdir" \
      "$barrow_cmd join id id --right '$right_input' --right-format csv -i '$input' --parquet -o '$outdir/join.parquet'"
    measure "$dataset" "$rows" join sql_equivalent_inner "$outdir" \
      "$barrow_cmd sql \"SELECT l.*, r.segment, r.weight, r.status FROM tbl AS l INNER JOIN read_csv_auto('$right_input') AS r ON l.id = r.id\" -i '$input' --parquet -o '$outdir/join_sql.parquet'"
  fi

  if should_run pipeline; then
    measure "$dataset" "$rows" pipeline standard_pipe "$outdir" \
      "$barrow_cmd filter 'a > 200' -i '$input' | $barrow_cmd mutate 'total=a+b,weighted=score+value' | $barrow_cmd select 'id,grp,region,total,weighted,category' | $barrow_cmd sort 'grp,total' --parquet -o '$outdir/pipeline.parquet'"
    measure "$dataset" "$rows" pipeline tmp_pipe "$outdir" \
      "$barrow_cmd filter 'a > 200' -i '$input' --tmp | $barrow_cmd mutate 'total=a+b,weighted=score+value' --tmp | $barrow_cmd select 'id,grp,region,total,weighted,category' --tmp | $barrow_cmd sort 'grp,total' --parquet -o '$outdir/pipeline_tmp.parquet'"
    measure "$dataset" "$rows" pipeline sql_vs_commands "$outdir" \
      "$barrow_cmd sql \"SELECT id, grp, region, a + b AS total, score + value AS weighted, category FROM tbl WHERE a > 200 ORDER BY grp, total\" -i '$input' --parquet -o '$outdir/pipeline_sql.parquet'"

    # --- Composite scenarios ---
    # ETL: filter → mutate → groupby → summary
    measure "$dataset" "$rows" pipeline etl_pipe "$outdir" \
      "$barrow_cmd filter 'a > 200' -i '$input' | $barrow_cmd mutate 'total=a+b' | $barrow_cmd groupby 'grp,category' --tmp | $barrow_cmd summary 'total=sum,a=count' --parquet -o '$outdir/etl_pipe.parquet'"
    measure "$dataset" "$rows" pipeline etl_tmp "$outdir" \
      "$barrow_cmd filter 'a > 200' -i '$input' --tmp | $barrow_cmd mutate 'total=a+b' --tmp | $barrow_cmd groupby 'grp,category' --tmp | $barrow_cmd summary 'total=sum,a=count' --parquet -o '$outdir/etl_tmp.parquet'"
    measure "$dataset" "$rows" pipeline etl_sql "$outdir" \
      "$barrow_cmd sql \"SELECT grp, category, SUM(a + b) AS sum_total, COUNT(a) AS count_a FROM tbl WHERE a > 200 GROUP BY grp, category\" -i '$input' --parquet -o '$outdir/etl_sql.parquet'"

    # Analytics: join → window → sort → select
    measure "$dataset" "$rows" pipeline analytics_pipe "$outdir" \
      "$barrow_cmd join id id --right '$right_input' --right-format csv -i '$input' | $barrow_cmd window 'rn=row_number()' --by grp --order-by ts --tmp | $barrow_cmd sort 'grp,rn' --tmp | $barrow_cmd select 'id,grp,region,segment,rn' --parquet -o '$outdir/analytics_pipe.parquet'"
    measure "$dataset" "$rows" pipeline analytics_sql "$outdir" \
      "$barrow_cmd sql \"SELECT l.id, l.grp, l.region, r.segment, ROW_NUMBER() OVER (PARTITION BY l.grp ORDER BY l.ts) AS rn FROM tbl AS l INNER JOIN read_csv_auto('$right_input') AS r ON l.id = r.id ORDER BY l.grp, rn\" -i '$input' --parquet -o '$outdir/analytics_sql.parquet'"
  fi
}

for dataset in ${datasets//,/ }; do
  case "$dataset" in
    tiny) benchmark_dataset tiny "$rows_tiny" ;;
    small) benchmark_dataset small "$rows_small" ;;
    medium) benchmark_dataset medium "$rows_medium" ;;
    large) benchmark_dataset large "$rows_large" ;;
    xlarge) benchmark_dataset xlarge "$rows_xlarge" ;;
  esac
done

python - <<'PY' "$results_file" "$summary_file" "$summary_json" "$config_file"
from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
import pathlib
import statistics
import sys
from collections import defaultdict

results_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
summary_json_path = pathlib.Path(sys.argv[3])
config_path = pathlib.Path(sys.argv[4])

with results_path.open(newline="") as fh:
    reader = csv.DictReader(fh, delimiter="\t")
    rows = list(reader)

if not rows:
    summary_path.write_text("# Benchmark summary\n\nNo benchmark rows were generated.\n")
    summary_json_path.write_text(json.dumps({"results": []}, indent=2))
    print("\n==> Summary: no benchmark rows were generated")
    raise SystemExit(0)

config = {}
for line in config_path.read_text().splitlines():
    if not line.strip() or "=" not in line:
        continue
    key, value = line.split("=", 1)
    config[key] = value

groups = defaultdict(list)
for row in rows:
    key = (row["dataset"], row["rows"], row["benchmark"], row["variant"], row["phase"])
    groups[key].append(row)

def stats(raw_values: list[float], prefix: str) -> dict[str, float | int]:
    values = sorted(v for v in raw_values if not math.isnan(v))
    if not values:
        return {
            "runs": 0,
            f"avg_{prefix}": 0.0,
            f"median_{prefix}": 0.0,
            f"min_{prefix}": 0.0,
            f"max_{prefix}": 0.0,
            f"stdev_{prefix}": 0.0,
        }
    mean = statistics.fmean(values)
    median = statistics.median(values)
    minimum = values[0]
    maximum = values[-1]
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "runs": len(values),
        f"avg_{prefix}": mean,
        f"median_{prefix}": median,
        f"min_{prefix}": minimum,
        f"max_{prefix}": maximum,
        f"stdev_{prefix}": stdev,
    }

def safe_int(v, default=0):
    try:
        return int(v)
    except (ValueError, TypeError):
        return default

def safe_float(v, default=float('nan')):
    try:
        return float(v)
    except (ValueError, TypeError):
        return default

summary_rows = []
for key in sorted(groups):
    dataset, nrows, benchmark, variant, phase = key
    records = groups[key]
    item = {
        "dataset": dataset,
        "rows": safe_int(nrows),
        "benchmark": benchmark,
        "variant": variant,
        "phase": phase,
        "exit_codes": sorted({safe_int(record["exit_code"], -1) for record in records}),
    }
    item.update(stats([safe_float(record["seconds"]) for record in records], "seconds"))
    item.update(stats([safe_float(record["peak_rss_kb"]) for record in records], "peak_rss_kb"))
    item.update(stats([safe_float(record["cpu_user_s"]) for record in records], "cpu_user_s"))
    item.update(stats([safe_float(record["cpu_sys_s"]) for record in records], "cpu_sys_s"))
    if "cpu_pct" in records[0]:
        item.update(stats([safe_float(record["cpu_pct"]) for record in records], "cpu_pct"))
    summary_rows.append(item)

hot_by_benchmark = defaultdict(list)
for item in summary_rows:
    if item["phase"] != "hot":
        continue
    hot_by_benchmark[(item["dataset"], item["rows"], item["benchmark"])].append(item)

rankings = []
for key, items in sorted(hot_by_benchmark.items()):
    ordered = sorted(items, key=lambda item: item["avg_seconds"])
    baseline = ordered[0]["avg_seconds"]
    for position, item in enumerate(ordered, start=1):
        rankings.append({
            "dataset": key[0],
            "rows": key[1],
            "benchmark": key[2],
            "rank": position,
            "variant": item["variant"],
            "avg_seconds": item["avg_seconds"],
            "delta_vs_best_seconds": item["avg_seconds"] - baseline,
            "slowdown_vs_best": (item["avg_seconds"] / baseline) if baseline else math.inf,
        })

phase_overview = defaultdict(list)
for item in summary_rows:
    phase_overview[item["phase"]].append(item)

def collect_environment_metadata(workspace: pathlib.Path) -> dict[str, object]:
    metadata: dict[str, object] = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "uname": " ".join(platform.uname()),
    }

    cpuinfo = pathlib.Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if ":" not in line:
                continue
            key, value = [part.strip() for part in line.split(":", 1)]
            if key == "model name":
                metadata["cpu_model"] = value
                break

    meminfo = pathlib.Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                metadata["mem_total_kb"] = int(line.split()[1])
                break

    fixture_hashes = []
    for fixture_path in sorted(workspace.glob("*.csv")):
        if not fixture_path.is_file():
            continue
        digest = hashlib.sha256(fixture_path.read_bytes()).hexdigest()
        fixture_hashes.append(
            {
                "path": fixture_path.name,
                "bytes": fixture_path.stat().st_size,
                "sha256": digest,
            }
        )
    metadata["fixtures"] = fixture_hashes
    return metadata

environment = collect_environment_metadata(pathlib.Path(config["workspace"]))

sql_pairs = []
for key, items in sorted(hot_by_benchmark.items()):
    direct_like = None
    sql_like = None
    for item in items:
        variant = item["variant"]
        if variant.startswith("sql") or "sql_" in variant:
            sql_like = item if sql_like is None else min(sql_like, item, key=lambda x: x["avg_seconds"])
        else:
            direct_like = item if direct_like is None else min(direct_like, item, key=lambda x: x["avg_seconds"])
    if direct_like and sql_like:
        sql_pairs.append({
            "dataset": key[0],
            "rows": key[1],
            "benchmark": key[2],
            "direct_variant": direct_like["variant"],
            "direct_avg_seconds": direct_like["avg_seconds"],
            "sql_variant": sql_like["variant"],
            "sql_avg_seconds": sql_like["avg_seconds"],
            "sql_vs_direct_ratio": sql_like["avg_seconds"] / direct_like["avg_seconds"] if direct_like["avg_seconds"] else math.inf,
            "direct_minus_sql_seconds": direct_like["avg_seconds"] - sql_like["avg_seconds"],
        })

lines = []
lines.append("# Benchmark summary")
lines.append("")
lines.append("## Configuration")
for key in ("workspace", "barrow_cmd", "sizes", "rows_tiny", "rows_small", "rows_medium", "rows_large", "rows_xlarge", "datasets", "only", "cold_runs", "warmup", "iterations"):
    if key in config:
        lines.append(f"- **{key}**: `{config[key]}`")
lines.append("")
lines.append("## Environment")
for key in ("platform", "machine", "cpu_model", "mem_total_kb", "python"):
    if key in environment:
        lines.append(f"- **{key}**: `{environment[key]}`")
if environment.get("fixtures"):
    lines.append("- **fixtures**:")
    for fixture in environment["fixtures"]:
        lines.append(
            f"  - `{fixture['path']}` ({fixture['bytes']} bytes, sha256 `{fixture['sha256'][:16]}...`)"
        )
lines.append("")
lines.append("## Phase overview")
for phase in ("cold", "warmup", "hot"):
    items = phase_overview.get(phase, [])
    if not items:
        continue
    phase_stats = stats([item["avg_seconds"] for item in items], "seconds")
    rss_stats = stats([item["avg_peak_rss_kb"] for item in items], "peak_rss_kb")
    lines.append(
        f"- **{phase}**: {phase_stats['runs']} aggregate rows, avg={phase_stats['avg_seconds']:.6f}s, "
        f"median={phase_stats['median_seconds']:.6f}s, min={phase_stats['min_seconds']:.6f}s, "
        f"max={phase_stats['max_seconds']:.6f}s, avg peak RSS={rss_stats['avg_peak_rss_kb']:.0f}kB"
    )
lines.append("")
lines.append("## Hot rankings")
for key in sorted(hot_by_benchmark):
    dataset, rows_count, benchmark = key
    lines.append("")
    lines.append(f"### {dataset} / {benchmark} ({rows_count} rows)")
    lines.append("")
    lines.append("| Rank | Variant | Avg (s) | Median (s) | Min (s) | Max (s) | Stdev (s) | Avg RSS (kB) | Avg user (s) | Avg sys (s) | Avg CPU% |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    ordered = sorted(hot_by_benchmark[key], key=lambda item: item["avg_seconds"])
    for position, item in enumerate(ordered, start=1):
        cpu_pct_str = f"{item['avg_cpu_pct']:.0f}" if "avg_cpu_pct" in item else "N/A"
        lines.append(
            f"| {position} | {item['variant']} | {item['avg_seconds']:.6f} | {item['median_seconds']:.6f} | "
            f"{item['min_seconds']:.6f} | {item['max_seconds']:.6f} | {item['stdev_seconds']:.6f} | "
            f"{item['avg_peak_rss_kb']:.0f} | {item['avg_cpu_user_s']:.6f} | {item['avg_cpu_sys_s']:.6f} | {cpu_pct_str} |"
        )
lines.append("")
lines.append("## SQL vs command comparisons")
if sql_pairs:
    lines.append("")
    lines.append("| Dataset | Benchmark | Direct variant | Direct avg (s) | SQL variant | SQL avg (s) | SQL/direct |")
    lines.append("| --- | --- | --- | ---: | --- | ---: | ---: |")
    for item in sql_pairs:
        lines.append(
            f"| {item['dataset']} | {item['benchmark']} | {item['direct_variant']} | {item['direct_avg_seconds']:.6f} | "
            f"{item['sql_variant']} | {item['sql_avg_seconds']:.6f} | {item['sql_vs_direct_ratio']:.3f}x |"
        )
else:
    lines.append("")
    lines.append("No SQL/direct pairs were available in the hot-phase results.")
lines.append("")
lines.append("## Raw aggregates")
lines.append("")
lines.append("| Dataset | Rows | Benchmark | Variant | Phase | Runs | Avg (s) | Median (s) | Min (s) | Max (s) | Stdev (s) | Avg RSS (kB) | Avg user (s) | Avg sys (s) | Avg CPU% | Exit codes |")
lines.append("| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
for item in summary_rows:
    cpu_pct_str = f"{item['avg_cpu_pct']:.0f}" if "avg_cpu_pct" in item else "N/A"
    lines.append(
        f"| {item['dataset']} | {item['rows']} | {item['benchmark']} | {item['variant']} | {item['phase']} | {item['runs']} | "
        f"{item['avg_seconds']:.6f} | {item['median_seconds']:.6f} | {item['min_seconds']:.6f} | {item['max_seconds']:.6f} | {item['stdev_seconds']:.6f} | "
        f"{item['avg_peak_rss_kb']:.0f} | {item['avg_cpu_user_s']:.6f} | {item['avg_cpu_sys_s']:.6f} | {cpu_pct_str} | "
        f"{','.join(str(code) for code in item['exit_codes'])} |"
    )
summary_path.write_text("\n".join(lines) + "\n")
summary_json_path.write_text(
    json.dumps(
        {
            "config": config,
            "environment": environment,
            "summary_rows": summary_rows,
            "rankings": rankings,
            "sql_pairs": sql_pairs,
        },
        indent=2,
    )
)

print("\n==> Summary (hot averages)")
for key in sorted(hot_by_benchmark):
    dataset, rows_count, benchmark = key
    ordered = sorted(hot_by_benchmark[key], key=lambda item: item["avg_seconds"])
    winner = ordered[0]
    print(f"{dataset:>6} | {benchmark:<10} | best={winner['variant']:<18} avg={winner['avg_seconds']:.6f}s")
print(f"\nDetailed summary written to: {summary_path}")
print(f"JSON summary written to: {summary_json_path}")
PY

printf '\nDetailed results written to: %s\n' "$results_file"

if [[ "$cleanup" -eq 1 ]]; then
  rm -rf "$workspace/out" "$workspace/tiny.csv" "$workspace/small.csv" "$workspace/medium.csv" "$workspace/large.csv" "$workspace/xlarge.csv" \
         "$workspace/tiny_right.csv" "$workspace/small_right.csv" "$workspace/medium_right.csv" "$workspace/large_right.csv" "$workspace/xlarge_right.csv"
  echo "Generated fixtures and outputs were removed; results and summaries were kept in $workspace"
else
  echo "Generated fixtures, outputs, and summaries were kept in $workspace for inspection."
fi
