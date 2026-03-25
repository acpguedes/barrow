#!/usr/bin/env bash
# resource_wrapper.sh — Drop-in measurement wrapper for barrow benchmark
#
# Replaces plain `time` with GNU /usr/bin/time to capture:
#   - Wall clock time (seconds)
#   - Peak RSS (KB)
#   - User CPU time (seconds)
#   - System CPU time (seconds)
#   - CPU utilization (%)
#   - Exit code
#
# Usage:
#   source scripts/resource_wrapper.sh
#   run_measured "barrow filter 'a > 400' -i data.csv -o out.csv" \
#       dataset rows benchmark variant phase run results_file
#
# Or standalone:
#   ./scripts/resource_wrapper.sh "barrow select 'a,b' -i data.csv"
#
# Requirements:
#   - GNU time (/usr/bin/time), NOT the bash builtin
#   - Install: apt-get install time (Debian/Ubuntu) or brew install gnu-time (macOS)
set -euo pipefail

# ─── Detect GNU time ────────────────────────────────────────────────────────

detect_gnu_time() {
    local candidates=("/usr/bin/time" "gtime" "/usr/local/bin/gtime")
    for cmd in "${candidates[@]}"; do
        if command -v "$cmd" &>/dev/null; then
            if "$cmd" --version 2>&1 | grep -qi "GNU"; then
                echo "$cmd"
                return 0
            fi
        fi
    done
    echo ""
    return 1
}

GNU_TIME=$(detect_gnu_time) || true

if [[ -z "$GNU_TIME" ]]; then
    echo "WARNING: GNU time not found. Falling back to bash time (no memory/CPU metrics)." >&2
    echo "Install with: apt-get install time (Linux) or brew install gnu-time (macOS)" >&2
    FALLBACK_MODE=1
else
    FALLBACK_MODE=0
fi

# ─── TSV header ─────────────────────────────────────────────────────────────

RESOURCE_TSV_HEADER="dataset\trows\tbenchmark\tvariant\tphase\trun\tseconds\tpeak_rss_kb\tcpu_user_s\tcpu_sys_s\tcpu_pct\texit_code\tcommand"

emit_header() {
    local outfile="${1:--}"
    if [[ "$outfile" == "-" ]]; then
        printf '%s\n' "$RESOURCE_TSV_HEADER"
    else
        printf '%s\n' "$RESOURCE_TSV_HEADER" > "$outfile"
    fi
}

# ─── Core measurement function ──────────────────────────────────────────────
#
# Runs a command and captures resource usage.
# Outputs a TSV row to the specified results file.
#
# Arguments:
#   $1 - command string to execute (passed to bash -c)
#   $2 - dataset name (e.g., "small", "medium", "large")
#   $3 - row count (e.g., 1000, 50000, 200000)
#   $4 - benchmark name (e.g., "filter", "select")
#   $5 - variant name (e.g., "direct", "sql_equivalent")
#   $6 - phase (e.g., "cold", "warmup", "hot")
#   $7 - run number (1, 2, 3, ...)
#   $8 - output file path (append mode)
#
run_measured() {
    local cmd="$1"
    local dataset="${2:-unknown}"
    local rows="${3:-0}"
    local benchmark="${4:-unknown}"
    local variant="${5:-unknown}"
    local phase="${6:-unknown}"
    local run_num="${7:-0}"
    local outfile="${8:--}"

    local wall_s peak_rss_kb cpu_user cpu_sys cpu_pct exit_code
    local tmpfile

    if [[ "$FALLBACK_MODE" -eq 1 ]]; then
        # ─── Fallback: bash time only ───────────────────────────────────
        local start_ns end_ns
        start_ns=$(date +%s%N)

        eval "$cmd" >/dev/null 2>&1
        exit_code=$?

        end_ns=$(date +%s%N)
        wall_s=$(awk "BEGIN{printf \"%.6f\", ($end_ns - $start_ns) / 1000000000}")
        peak_rss_kb="NA"
        cpu_user="NA"
        cpu_sys="NA"
        cpu_pct="NA"
    else
        # ─── GNU time: full resource capture ────────────────────────────
        tmpfile=$(mktemp /tmp/barrow_time.XXXXXX)

        # GNU time format string:
        # %e = wall clock (seconds)
        # %M = max RSS (KB)
        # %U = user CPU (seconds)
        # %S = system CPU (seconds)
        # %P = CPU percentage
        # %x = exit code
        "$GNU_TIME" -f '%e\t%M\t%U\t%S\t%P\t%x' \
            -o "$tmpfile" \
            bash -c "$cmd" >/dev/null 2>&1 || true

        if [[ -s "$tmpfile" ]]; then
            IFS=$'\t' read -r wall_s peak_rss_kb cpu_user cpu_sys cpu_pct_raw exit_code < "$tmpfile"

            # Clean CPU% (remove trailing %)
            cpu_pct="${cpu_pct_raw//%/}"

            # Wall time: GNU time rounds to 0.01s; use it as-is
            wall_s="${wall_s:-0}"
        else
            # time failed somehow
            wall_s="0"
            peak_rss_kb="NA"
            cpu_user="NA"
            cpu_sys="NA"
            cpu_pct="NA"
            exit_code="1"
        fi

        rm -f "$tmpfile"
    fi

    # ─── Emit TSV row ───────────────────────────────────────────────────
    local row
    row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
        "$dataset" "$rows" "$benchmark" "$variant" "$phase" "$run_num" \
        "$wall_s" "$peak_rss_kb" "$cpu_user" "$cpu_sys" "$cpu_pct" "$exit_code" \
        "$cmd")

    if [[ "$outfile" == "-" ]]; then
        echo "$row"
    else
        echo "$row" >> "$outfile"
    fi
}

# ─── Convenience: measure with input/output file size ────────────────────────
#
# Same as run_measured but also captures input/output file sizes.
# Reports sizes to stderr for debugging.
#
run_measured_with_io() {
    local cmd="$1"
    local dataset="${2:-unknown}"
    local rows="${3:-0}"
    local benchmark="${4:-unknown}"
    local variant="${5:-unknown}"
    local phase="${6:-unknown}"
    local run_num="${7:-0}"
    local outfile="${8:--}"
    local input_file="${9:-}"
    local output_file="${10:-}"

    # Get input file size before
    local input_bytes=0
    if [[ -n "$input_file" && -f "$input_file" ]]; then
        input_bytes=$(stat -c%s "$input_file" 2>/dev/null || stat -f%z "$input_file" 2>/dev/null || echo 0)
    fi

    # Run the command with measurement
    run_measured "$cmd" "$dataset" "$rows" "$benchmark" "$variant" "$phase" "$run_num" "$outfile"

    # Get output file size after
    local output_bytes=0
    if [[ -n "$output_file" && -f "$output_file" ]]; then
        output_bytes=$(stat -c%s "$output_file" 2>/dev/null || stat -f%z "$output_file" 2>/dev/null || echo 0)
    fi

    if [[ "$input_bytes" -gt 0 || "$output_bytes" -gt 0 ]]; then
        echo "IO_STATS: input_bytes=$input_bytes output_bytes=$output_bytes" >&2
    fi
}

# ─── System info capture ────────────────────────────────────────────────────

capture_system_info() {
    local outfile="${1:-/dev/stdout}"
    {
        echo "=== System Info ==="
        echo "date: $(date -Iseconds)"
        echo "hostname: $(hostname)"
        echo "kernel: $(uname -srm)"
        echo "cpu_model: $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'unknown')"
        echo "cpu_cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 'unknown')"
        echo "ram_total_mb: $(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f", $1/1048576}' || echo 'unknown')"
        echo "gnu_time: ${GNU_TIME:-not_found}"
        echo "==================="
    } > "$outfile"
}

# ─── Standalone mode ────────────────────────────────────────────────────────

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Running directly (not sourced)
    if [[ $# -lt 1 ]]; then
        echo "Usage: $0 <command> [dataset rows benchmark variant phase run outfile]"
        echo ""
        echo "Examples:"
        echo "  $0 \"barrow filter 'a > 400' -i data.csv -o out.csv\""
        echo "  $0 \"barrow select 'a,b' -i data.csv\" small 1000 select direct hot 1 results.tsv"
        echo ""
        echo "System check:"
        if [[ "$FALLBACK_MODE" -eq 0 ]]; then
            echo "  GNU time: $GNU_TIME"
        else
            echo "  GNU time: NOT FOUND (install with: apt-get install time)"
        fi
        exit 0
    fi

    cmd="$1"
    dataset="${2:-test}"
    rows="${3:-0}"
    benchmark="${4:-manual}"
    variant="${5:-default}"
    phase="${6:-hot}"
    run_num="${7:-1}"
    outfile="${8:--}"

    if [[ "$outfile" != "-" && ! -f "$outfile" ]]; then
        emit_header "$outfile"
    fi

    run_measured "$cmd" "$dataset" "$rows" "$benchmark" "$variant" "$phase" "$run_num" "$outfile"
fi
