#!/usr/bin/env python3
"""
analyze_bench.py — Comprehensive benchmark analysis for Barrow.

Handles both legacy format (without cpu_pct) and enriched format (with cpu_pct).
Supports peak_rss_kb (benchmark.sh) and peak_rss_mb (resource_wrapper) schemas.
Generates: statistical analysis markdown + structured JSON.

Usage:
    python3 scripts/analyze_bench.py results.tsv [--output-dir ./report]
    python3 scripts/analyze_bench.py results.tsv --summary-only
    python3 scripts/analyze_bench.py results.tsv --json-only
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from scipy import stats as sp_stats

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


# ─── Schema detection and loading ───────────────────────────────────────────


def load_results(path: str) -> tuple[pd.DataFrame, bool]:
    """Load results TSV, auto-detecting schema. Returns (df, has_resources)."""
    df = pd.read_csv(path, sep="\t")

    # Normalize: if peak_rss_mb exists (resource_wrapper original), convert to kb
    if "peak_rss_mb" in df.columns and "peak_rss_kb" not in df.columns:
        df["peak_rss_kb"] = pd.to_numeric(df["peak_rss_mb"], errors="coerce") * 1024
        df.drop(columns=["peak_rss_mb"], inplace=True)

    # Detect whether resource data is actually populated (not all zeros/NA)
    has_resources = False
    for col in ["peak_rss_kb", "cpu_user_s", "cpu_sys_s"]:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            df[col] = numeric
            if numeric.notna().any() and (numeric > 0).any():
                has_resources = True

    if "cpu_pct" in df.columns:
        df["cpu_pct"] = pd.to_numeric(df["cpu_pct"], errors="coerce")

    # Ensure numeric types
    df["seconds"] = pd.to_numeric(df["seconds"], errors="coerce")
    df["rows"] = pd.to_numeric(df["rows"], errors="coerce")

    return df, has_resources


# ─── Statistical analysis ───────────────────────────────────────────────────


def analyze_distribution(series: pd.Series, name: str = "") -> dict:
    """Full distribution stats for a numeric series."""
    if len(series) == 0:
        return {"name": name, "n": 0}
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    return {
        "name": name,
        "n": len(series),
        "mean": series.mean(),
        "median": series.median(),
        "std": series.std(),
        "cv_pct": (series.std() / series.mean() * 100) if series.mean() > 0 else 0,
        "min": series.min(),
        "max": series.max(),
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "p5": series.quantile(0.05),
        "p95": series.quantile(0.95),
        "skewness": series.skew(),
        "kurtosis": series.kurtosis(),
        "outliers_low": int((series < q1 - 1.5 * iqr).sum()),
        "outliers_high": int((series > q3 + 1.5 * iqr).sum()),
    }


def normality_test(series: pd.Series) -> dict:
    """Shapiro-Wilk normality test."""
    if not HAS_SCIPY:
        return {
            "test": "shapiro",
            "W": None,
            "p": None,
            "normal": None,
            "note": "scipy not installed",
        }
    if len(series) < 3 or len(series) > 5000:
        return {"test": "shapiro", "W": None, "p": None, "normal": None}
    w, p = sp_stats.shapiro(series)
    return {"test": "shapiro", "W": round(w, 4), "p": round(p, 6), "normal": p > 0.05}


def compare_variants(group_a: pd.Series, group_b: pd.Series) -> dict:
    """Mann-Whitney U + Cohen's d between two groups."""
    if not HAS_SCIPY:
        return {
            "u_stat": None,
            "p_value": None,
            "cohens_d": None,
            "significant": None,
            "note": "scipy not installed",
        }
    if len(group_a) < 3 or len(group_b) < 3:
        return {"u_stat": None, "p_value": None, "cohens_d": None, "significant": None}
    u, p = sp_stats.mannwhitneyu(group_a, group_b, alternative="two-sided")
    pooled_std = np.sqrt((group_a.std() ** 2 + group_b.std() ** 2) / 2)
    d = (group_a.mean() - group_b.mean()) / pooled_std if pooled_std > 0 else 0
    return {
        "u_stat": round(float(u), 1),
        "p_value": round(float(p), 6),
        "cohens_d": round(float(d), 3),
        "effect_size": (
            "large" if abs(d) >= 0.8 else "medium" if abs(d) >= 0.5 else "small"
        ),
        "significant": bool(p < 0.05),
    }


# ─── Core analysis pipeline ─────────────────────────────────────────────────


def run_analysis(df: pd.DataFrame, has_resources: bool) -> dict:
    """Run the full analysis pipeline. Returns structured results dict."""
    results: dict = {}

    hot = df[df["phase"] == "hot"].copy()
    cold = df[df["phase"] == "cold"].copy()

    # 1. Global distribution
    results["global"] = analyze_distribution(hot["seconds"], "hot_seconds")
    results["global"]["normality"] = normality_test(hot["seconds"])

    # 2. Phase comparison
    phase_stats = {}
    for phase_name in ["cold", "warmup", "hot"]:
        subset = df[df["phase"] == phase_name]["seconds"]
        if len(subset) > 0:
            phase_stats[phase_name] = analyze_distribution(subset, phase_name)
    results["phases"] = phase_stats

    cold_mean = cold["seconds"].mean() if len(cold) > 0 else 0
    hot_mean = hot["seconds"].mean() if len(hot) > 0 else 0
    results["warmup_effect_pct"] = (
        round((cold_mean - hot_mean) / cold_mean * 100, 2) if cold_mean > 0 else 0
    )

    # 3. Per-benchmark aggregation
    agg = (
        hot.groupby(["dataset", "benchmark", "variant"])["seconds"]
        .agg(["mean", "median", "std", "min", "max", "count"])
        .reset_index()
    )
    agg["cv_pct"] = (agg["std"] / agg["mean"] * 100).round(2)
    results["aggregated"] = agg.round(6).to_dict("records")

    # 4. Scaling analysis
    scaling = []
    datasets = sorted(hot["dataset"].unique())
    for bench in hot["benchmark"].unique():
        bench_data = hot[hot["benchmark"] == bench]
        sizes = []
        for ds in datasets:
            ds_data = bench_data[bench_data["dataset"] == ds]
            if len(ds_data) > 0:
                sizes.append(
                    {
                        "dataset": ds,
                        "mean_s": round(ds_data["seconds"].mean(), 4),
                        "rows": int(ds_data["rows"].iloc[0]) if pd.notna(ds_data["rows"].iloc[0]) else 0,
                    }
                )
        if len(sizes) >= 2:
            first, last = sizes[0], sizes[-1]
            factor = (
                round(last["mean_s"] / first["mean_s"], 2) if first["mean_s"] > 0 else 0
            )
            scaling.append(
                {
                    "benchmark": bench,
                    "smallest": first["dataset"],
                    "smallest_mean": first["mean_s"],
                    "largest": last["dataset"],
                    "largest_mean": last["mean_s"],
                    "factor": factor,
                    "all_sizes": sizes,
                }
            )
    results["scaling"] = sorted(scaling, key=lambda x: x["factor"], reverse=True)

    # 5. SQL vs Direct comparisons
    sql_vs = []
    for bench in hot["benchmark"].unique():
        for ds in hot["dataset"].unique():
            sql_data = hot[
                (hot["benchmark"] == bench)
                & (hot["variant"].str.contains("sql", case=False))
                & (hot["dataset"] == ds)
            ]["seconds"]
            dir_data = hot[
                (hot["benchmark"] == bench)
                & (~hot["variant"].str.contains("sql", case=False))
                & (hot["dataset"] == ds)
            ]["seconds"]
            if len(sql_data) >= 3 and len(dir_data) >= 3:
                comp = compare_variants(sql_data, dir_data)
                sql_vs.append(
                    {
                        "dataset": ds,
                        "benchmark": bench,
                        "sql_mean": round(float(sql_data.mean()), 4),
                        "direct_mean": round(float(dir_data.mean()), 4),
                        "diff_pct": round(
                            float(
                                (sql_data.mean() - dir_data.mean())
                                / dir_data.mean()
                                * 100
                            ),
                            2,
                        ),
                        **comp,
                    }
                )
    results["sql_vs_direct"] = sql_vs

    # 6. Best variant per benchmark+dataset
    if len(agg) > 0:
        best = agg.loc[agg.groupby(["dataset", "benchmark"])["mean"].idxmin()]
        results["best_variants"] = best[
            ["dataset", "benchmark", "variant", "mean"]
        ].to_dict("records")
    else:
        results["best_variants"] = []

    # 7. Normality per benchmark
    normality = {}
    for bench in hot["benchmark"].unique():
        data = hot[hot["benchmark"] == bench]["seconds"]
        if len(data) >= 8:
            normality[bench] = normality_test(data)
    results["normality_by_benchmark"] = normality

    # 8. Kruskal-Wallis: dataset size effect
    if HAS_SCIPY:
        groups = [g["seconds"].values for _, g in hot.groupby("dataset")]
        if len(groups) >= 2 and all(len(g) >= 3 for g in groups):
            h, p = sp_stats.kruskal(*groups)
            results["kruskal_dataset_size"] = {
                "H": round(float(h), 4),
                "p": round(float(p), 8),
                "significant": bool(p < 0.05),
            }

    # 9. Cold penalty per benchmark
    cold_penalty = []
    for bench in hot["benchmark"].unique():
        hot_m = hot[hot["benchmark"] == bench]["seconds"].mean()
        cold_bench = cold[cold["benchmark"] == bench]
        cold_m = cold_bench["seconds"].mean() if len(cold_bench) > 0 else hot_m
        penalty = (cold_m - hot_m) / hot_m * 100 if hot_m > 0 else 0
        cold_penalty.append(
            {"benchmark": bench, "cold_penalty_pct": round(float(penalty), 1)}
        )
    results["cold_penalty"] = sorted(
        cold_penalty, key=lambda x: x["cold_penalty_pct"], reverse=True
    )

    # 10. Floor analysis (minimum overhead proxy)
    floor = []
    for ds in sorted(hot["dataset"].unique()):
        subset = hot[hot["dataset"] == ds]
        if len(subset) > 0:
            min_idx = subset["seconds"].idxmin()
            min_row = subset.loc[min_idx]
            floor.append(
                {
                    "dataset": ds,
                    "floor_s": round(float(min_row["seconds"]), 4),
                    "benchmark": min_row["benchmark"],
                    "variant": min_row["variant"],
                }
            )
    results["floor_analysis"] = floor

    # 11. Resource analysis (if available)
    if has_resources:
        resource_stats = _analyze_resources(hot)
        results["resources"] = resource_stats

    return results


def _analyze_resources(hot: pd.DataFrame) -> dict:
    """Analyze memory and CPU metrics from enriched data."""
    res: dict = {}

    # Peak RSS by benchmark+dataset
    if (
        "peak_rss_kb" in hot.columns
        and hot["peak_rss_kb"].notna().any()
        and (hot["peak_rss_kb"] > 0).any()
    ):
        rss_mb = hot["peak_rss_kb"].dropna() / 1024  # Convert to MB for display

        rss_agg = hot.copy()
        rss_agg["peak_rss_mb"] = rss_agg["peak_rss_kb"] / 1024
        rss_by_op = (
            rss_agg.groupby(["dataset", "benchmark", "variant"])["peak_rss_mb"]
            .agg(["mean", "median", "max"])
            .reset_index()
            .round(1)
        )
        res["rss_by_operation"] = rss_by_op.to_dict("records")

        # Memory scaling
        mem_scaling = []
        datasets = sorted(hot["dataset"].unique())
        for bench in hot["benchmark"].unique():
            bench_data = hot[hot["benchmark"] == bench]
            sizes = []
            for ds in datasets:
                ds_rss = bench_data[bench_data["dataset"] == ds]["peak_rss_kb"].mean()
                if not np.isnan(ds_rss) and ds_rss > 0:
                    sizes.append({"dataset": ds, "rss_mb": round(ds_rss / 1024, 1)})
            if len(sizes) >= 2:
                first, last = sizes[0], sizes[-1]
                if first["rss_mb"] > 0:
                    mem_scaling.append(
                        {
                            "benchmark": bench,
                            "smallest": first["dataset"],
                            "smallest_rss_mb": first["rss_mb"],
                            "largest": last["dataset"],
                            "largest_rss_mb": last["rss_mb"],
                            "factor": round(last["rss_mb"] / first["rss_mb"], 2),
                        }
                    )
        res["memory_scaling"] = sorted(
            mem_scaling, key=lambda x: x["factor"], reverse=True
        )

        # Global memory stats (in MB)
        res["rss_global"] = analyze_distribution(rss_mb, "peak_rss_mb")

    # CPU utilization
    cpu_cols = ["cpu_user_s", "cpu_sys_s"]
    if all(c in hot.columns for c in cpu_cols):
        hot_valid = hot.dropna(subset=cpu_cols).copy()
        hot_valid = hot_valid[
            (hot_valid["cpu_user_s"] > 0) | (hot_valid["cpu_sys_s"] > 0)
        ]
        if len(hot_valid) > 0:
            hot_valid["cpu_total_s"] = hot_valid["cpu_user_s"] + hot_valid["cpu_sys_s"]
            hot_valid["cpu_efficiency"] = (
                hot_valid["cpu_total_s"] / hot_valid["seconds"] * 100
            ).clip(0, 200)

            res["cpu_efficiency"] = {
                "mean_pct": round(float(hot_valid["cpu_efficiency"].mean()), 1),
                "median_pct": round(float(hot_valid["cpu_efficiency"].median()), 1),
                "interpretation": (
                    "CPU-bound"
                    if hot_valid["cpu_efficiency"].median() > 90
                    else (
                        "Mixed I/O + CPU"
                        if hot_valid["cpu_efficiency"].median() > 50
                        else "I/O-bound"
                    )
                ),
            }

            # Per-benchmark CPU breakdown
            cpu_by_bench = (
                hot_valid.groupby(["dataset", "benchmark"])
                .agg(
                    wall_mean=("seconds", "mean"),
                    cpu_user_mean=("cpu_user_s", "mean"),
                    cpu_sys_mean=("cpu_sys_s", "mean"),
                    cpu_eff_mean=("cpu_efficiency", "mean"),
                )
                .reset_index()
                .round(3)
            )
            res["cpu_by_benchmark"] = cpu_by_bench.to_dict("records")

            # Compute vs I/O split estimate
            hot_valid["io_wait_estimate_s"] = (
                hot_valid["seconds"] - hot_valid["cpu_total_s"]
            ).clip(0)
            io_split = (
                hot_valid.groupby("benchmark")
                .agg(
                    avg_wall=("seconds", "mean"),
                    avg_cpu=("cpu_total_s", "mean"),
                    avg_io_wait=("io_wait_estimate_s", "mean"),
                )
                .reset_index()
            )
            io_split["io_pct"] = (
                io_split["avg_io_wait"] / io_split["avg_wall"] * 100
            ).round(1)
            res["io_vs_compute"] = io_split.round(4).to_dict("records")

    # Memory efficiency: rows/s per MB
    if "peak_rss_kb" in hot.columns:
        hot_mem = hot.dropna(subset=["peak_rss_kb"]).copy()
        hot_mem = hot_mem[hot_mem["peak_rss_kb"] > 0]
        if len(hot_mem) > 0:
            hot_mem["peak_rss_mb"] = hot_mem["peak_rss_kb"] / 1024
            hot_mem["rows_per_sec"] = hot_mem["rows"] / hot_mem["seconds"]
            hot_mem["efficiency"] = hot_mem["rows_per_sec"] / hot_mem["peak_rss_mb"]
            eff_by_bench = (
                hot_mem.groupby(["dataset", "benchmark", "variant"])["efficiency"]
                .mean()
                .reset_index()
                .round(1)
            )
            res["throughput_efficiency"] = eff_by_bench.to_dict("records")

    return res


# ─── Output generators ──────────────────────────────────────────────────────


def generate_summary_md(results: dict, has_resources: bool) -> str:
    """Generate markdown summary from analysis results."""
    lines = ["# Barrow Benchmark — Comprehensive Statistical Analysis\n"]

    # Global
    g = results["global"]
    if g.get("n", 0) == 0:
        lines.append("No hot-phase data available for analysis.\n")
        return "\n".join(lines)

    lines.append("## Global Distribution (Hot Phase)\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| N | {g['n']} |")
    lines.append(f"| Mean | {g['mean']:.4f}s |")
    lines.append(f"| Median | {g['median']:.4f}s |")
    lines.append(f"| Std | {g['std']:.4f}s |")
    lines.append(f"| CV | {g['cv_pct']:.1f}% |")
    lines.append(f"| Min / Max | {g['min']:.4f} / {g['max']:.4f}s |")
    lines.append(f"| P5 / P95 | {g['p5']:.4f} / {g['p95']:.4f}s |")
    lines.append(f"| Skewness | {g['skewness']:.2f} |")
    lines.append(f"| Kurtosis | {g['kurtosis']:.2f} |")
    lines.append(
        f"| Outliers (IQR) | low={g['outliers_low']}, high={g['outliers_high']} |"
    )
    norm = g.get("normality", {})
    if norm.get("W"):
        lines.append(
            f"| Shapiro-Wilk | W={norm['W']}, p={norm['p']} "
            f"({'normal' if norm['normal'] else '**NOT normal**'}) |"
        )
    lines.append("")

    # Warmup effect
    lines.append(
        f"## Warmup Effect: **{results['warmup_effect_pct']}%** reduction cold to hot\n"
    )

    # Kruskal-Wallis
    if "kruskal_dataset_size" in results:
        k = results["kruskal_dataset_size"]
        sig_str = "**significant**" if k["significant"] else "not significant"
        lines.append("## Dataset Size Effect")
        lines.append(f"Kruskal-Wallis: H={k['H']}, p={k['p']} ({sig_str})\n")

    # Scaling
    if results.get("scaling"):
        lines.append("## Scaling Analysis\n")
        lines.append("| Benchmark | Smallest (s) | Largest (s) | Factor | Note |")
        lines.append("|-----------|--------------|-------------|--------|------|")
        for s in results["scaling"]:
            note = (
                "high" if s["factor"] > 2.0 else "good" if s["factor"] < 1.5 else "ok"
            )
            lines.append(
                f"| {s['benchmark']} | {s['smallest_mean']} ({s['smallest']}) "
                f"| {s['largest_mean']} ({s['largest']}) | {s['factor']}x | {note} |"
            )
        lines.append("")

    # Cold penalty
    if results.get("cold_penalty"):
        lines.append("## Cold Penalty\n")
        lines.append("| Benchmark | Penalty (%) |")
        lines.append("|-----------|-------------|")
        for cp in results["cold_penalty"][:10]:
            flag = " (high)" if cp["cold_penalty_pct"] > 10 else ""
            lines.append(f"| {cp['benchmark']} | {cp['cold_penalty_pct']}%{flag} |")
        lines.append("")

    # SQL vs Direct
    if results.get("sql_vs_direct"):
        lines.append("## SQL vs Direct\n")
        lines.append(
            "| Dataset | Benchmark | SQL (s) | Direct (s) | Diff% | p-value | Sig? | Effect |"
        )
        lines.append(
            "|---------|-----------|---------|------------|-------|---------|------|--------|"
        )
        for c in sorted(results["sql_vs_direct"], key=lambda x: x["diff_pct"]):
            sig = "Y" if c.get("significant") else "N"
            p_val = c.get("p_value", "N/A")
            effect = c.get("effect_size", "N/A")
            lines.append(
                f"| {c['dataset']} | {c['benchmark']} | {c['sql_mean']} | {c['direct_mean']} | "
                f"{c['diff_pct']:+.1f}% | {p_val} | {sig} | {effect} |"
            )
        lines.append("")

    # Best variants
    if results.get("best_variants"):
        lines.append("## Best Variant per Operation (Hot)\n")
        lines.append("| Dataset | Benchmark | Winner | Avg (s) |")
        lines.append("|---------|-----------|--------|---------|")
        for b in results["best_variants"]:
            lines.append(
                f"| {b['dataset']} | {b['benchmark']} | **{b['variant']}** | {b['mean']:.4f} |"
            )
        lines.append("")

    # Resource analysis
    if has_resources and "resources" in results:
        res = results["resources"]
        lines.append("## Resource Analysis (Memory & CPU)\n")

        if "rss_global" in res and res["rss_global"].get("n", 0) > 0:
            rg = res["rss_global"]
            lines.append("### Memory (Peak RSS)")
            lines.append(
                f"- Mean: **{rg['mean']:.1f} MB**, Median: {rg['median']:.1f} MB"
            )
            lines.append(f"- Range: {rg['min']:.1f} - {rg['max']:.1f} MB")
            lines.append(f"- P95: {rg['p95']:.1f} MB\n")

        if "memory_scaling" in res and res["memory_scaling"]:
            lines.append("### Memory Scaling\n")
            lines.append("| Benchmark | Smallest (MB) | Largest (MB) | Factor |")
            lines.append("|-----------|---------------|--------------|--------|")
            for ms in res["memory_scaling"][:10]:
                lines.append(
                    f"| {ms['benchmark']} | {ms['smallest_rss_mb']} ({ms['smallest']}) "
                    f"| {ms['largest_rss_mb']} ({ms['largest']}) | {ms['factor']}x |"
                )
            lines.append("")

        if "cpu_efficiency" in res:
            ce = res["cpu_efficiency"]
            lines.append("### CPU Efficiency")
            lines.append(f"- Mean: **{ce['mean_pct']}%**, Median: {ce['median_pct']}%")
            lines.append(f"- Classification: **{ce['interpretation']}**\n")

        if "io_vs_compute" in res and res["io_vs_compute"]:
            lines.append("### I/O vs Compute Split\n")
            lines.append("| Benchmark | Wall (s) | CPU (s) | I/O Wait (s) | I/O % |")
            lines.append("|-----------|----------|---------|--------------|-------|")
            for io in sorted(
                res["io_vs_compute"], key=lambda x: x["io_pct"], reverse=True
            ):
                lines.append(
                    f"| {io['benchmark']} | {io['avg_wall']} | {io['avg_cpu']} | "
                    f"{io['avg_io_wait']} | {io['io_pct']}% |"
                )
            lines.append("")

    # Floor
    if results.get("floor_analysis"):
        lines.append("## Floor Analysis (startup overhead proxy)\n")
        lines.append("| Dataset | Floor (s) | Operation |")
        lines.append("|---------|-----------|----------|")
        for f in results["floor_analysis"]:
            lines.append(
                f"| {f['dataset']} | {f['floor_s']}s | {f['benchmark']}/{f['variant']} |"
            )
        lines.append("")

    return "\n".join(lines)


def generate_json(results: dict, path: str) -> None:
    """Dump full results as JSON."""

    def convert(obj: object) -> object:
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        raise TypeError(f"Not serializable: {type(obj)}")

    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=convert)


# ─── CLI ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Barrow benchmark analysis")
    parser.add_argument("results", help="Path to results.tsv")
    parser.add_argument(
        "--output-dir",
        "-o",
        default=None,
        help="Output directory (default: same as results)",
    )
    parser.add_argument(
        "--summary-only", action="store_true", help="Only generate markdown summary"
    )
    parser.add_argument("--json-only", action="store_true", help="Only generate JSON")
    args = parser.parse_args()

    results_path = Path(args.results)
    if not results_path.exists():
        print(f"Error: {results_path} not found", file=sys.stderr)
        sys.exit(1)

    # Load
    df, has_resources = load_results(str(results_path))
    print(f"Loaded {len(df)} rows, resources_available={has_resources}")
    print(f"  Datasets: {sorted(df['dataset'].unique())}")
    print(f"  Benchmarks: {sorted(df['benchmark'].unique())}")
    print(f"  Phases: {sorted(df['phase'].unique())}")
    if has_resources:
        rss_valid = (df.get("peak_rss_kb", pd.Series(dtype=float)) > 0).sum()
        print(f"  Resource data: {rss_valid}/{len(df)} rows with valid RSS")
    if not HAS_SCIPY:
        print("  Note: scipy not installed, skipping normality and comparison tests")

    # Analyze
    results = run_analysis(df, has_resources)

    # Output
    outdir = Path(args.output_dir) if args.output_dir else results_path.parent
    outdir.mkdir(parents=True, exist_ok=True)

    if not args.json_only:
        md_path = outdir / "analysis.md"
        md_content = generate_summary_md(results, has_resources)
        md_path.write_text(md_content)
        print(f"\nMarkdown: {md_path}")

    if not args.summary_only:
        json_path = outdir / "analysis.json"
        generate_json(results, str(json_path))
        print(f"JSON: {json_path}")

    # Quick summary to stdout
    g = results["global"]
    if g.get("n", 0) > 0:
        print(f"\n{'=' * 60}")
        print(
            f"HOT PHASE: mean={g['mean']:.4f}s, median={g['median']:.4f}s, n={g['n']}"
        )
        print(f"Distribution: skew={g['skewness']:.2f}, kurt={g['kurtosis']:.2f}")
        print(f"Warmup effect: {results['warmup_effect_pct']}%")
        if "kruskal_dataset_size" in results:
            k = results["kruskal_dataset_size"]
            print(f"Dataset size effect: H={k['H']}, p={k['p']}")
        if has_resources and "resources" in results:
            res = results["resources"]
            if "rss_global" in res and res["rss_global"].get("n", 0) > 0:
                print(
                    f"Peak RSS: mean={res['rss_global']['mean']:.1f}MB, max={res['rss_global']['max']:.1f}MB"
                )
            if "cpu_efficiency" in res:
                print(
                    f"CPU efficiency: {res['cpu_efficiency']['mean_pct']}% ({res['cpu_efficiency']['interpretation']})"
                )
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
