"""Benchmark harness measuring real throughput, latency, compaction, and data quality metrics."""

from __future__ import annotations

import json
import logging
import shutil
import sys
import time
from pathlib import Path

# Ensure project root is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src.ingestion.opensky_client import OpenSkyClient
from src.lakehouse.compactor import LakehouseCompactor
from src.lakehouse.storage import LakehouseStorage
from src.quality.assertions import DataQualityChecker
from src.streaming.anomaly_detector import AnomalyDetector
from src.streaming.window_aggregations import WindowAggregator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
BENCHMARK_LAKEHOUSE = Path("temp_benchmark_lakehouse")


def run_comprehensive_benchmark():
    logger.info("Initializing benchmark harness...")
    client = OpenSkyClient()
    records = client.fetch_telemetry_batch()
    total_records = len(records)
    logger.info("Loaded %d real OpenSky telemetry records for benchmarking.", total_records)

    # 1. Measure Streaming Processing Throughput & Latency across Batch Sizes
    batch_sizes = [500, 1000, 2500, 5000, 10000]
    # Filter to batch sizes that can be formed from available records or repeat records
    test_records = records
    while len(test_records) < 10000:
        test_records = test_records + records

    throughput_results = []
    latencies_per_batch = {}

    aggregator = WindowAggregator(window_minutes=5)
    detector = AnomalyDetector()

    for b_size in batch_sizes:
        sub_batch = test_records[:b_size]
        batch_latencies = []
        # Run 5 iterations per batch size to get p50, p95, p99
        for _ in range(5):
            t0 = time.perf_counter()
            _ = aggregator.compute_windowed_aggregations(sub_batch)
            _ = detector.process_batch(sub_batch)
            elapsed_sec = time.perf_counter() - t0
            batch_latencies.append(elapsed_sec * 1000.0)  # ms

        avg_lat_ms = float(np.mean(batch_latencies))
        p50_ms = float(np.percentile(batch_latencies, 50))
        p95_ms = float(np.percentile(batch_latencies, 95))
        p99_ms = float(np.percentile(batch_latencies, 99))
        throughput_eps = float(b_size / (avg_lat_ms / 1000.0))

        throughput_results.append({
            "batch_size": b_size,
            "throughput_events_per_sec": round(throughput_eps, 1),
            "latency_p50_ms": round(p50_ms, 2),
            "latency_p95_ms": round(p95_ms, 2),
            "latency_p99_ms": round(p99_ms, 2),
        })
        latencies_per_batch[b_size] = {
            "p50": p50_ms,
            "p95": p95_ms,
            "p99": p99_ms,
        }
        logger.info(
            "Batch Size %5d -> Throughput: %8.1f events/s | Latency p50: %6.2f ms | p95: %6.2f ms",
            b_size,
            throughput_eps,
            p50_ms,
            p95_ms,
        )

    # 2. Measure Lakehouse Partition Compaction Performance
    if BENCHMARK_LAKEHOUSE.exists():
        shutil.rmtree(BENCHMARK_LAKEHOUSE)
    storage = LakehouseStorage(base_dir=BENCHMARK_LAKEHOUSE)

    # Generate 15 streaming micro-batches of 500 records each
    num_micro_batches = 15
    for i in range(num_micro_batches):
        mb = test_records[i * 200 : (i + 1) * 200]
        storage.write_silver_batch(mb, batch_id=f"bench_mb_{i+1}")

    compactor = LakehouseCompactor(base_dir=BENCHMARK_LAKEHOUSE)
    t_comp_start = time.perf_counter()
    comp_stats = compactor.run_compaction()
    comp_duration_sec = time.perf_counter() - t_comp_start

    compaction_metric = {
        "partitions_processed": comp_stats.partitions_processed,
        "files_merged": comp_stats.files_merged,
        "records_compacted": comp_stats.records_compacted,
        "bytes_before": comp_stats.bytes_before,
        "bytes_after": comp_stats.bytes_after,
        "compression_ratio": comp_stats.compression_ratio,
        "savings_percent": comp_stats.savings_percent,
        "compaction_duration_seconds": round(comp_duration_sec, 3),
    }
    logger.info("Compaction benchmark: %s", compaction_metric)

    # 3. Measure DuckDB Data Quality Assertion Latency
    checker = DataQualityChecker(lakehouse_dir=BENCHMARK_LAKEHOUSE)
    t_dq_start = time.perf_counter()
    dq_report = checker.run_checks()
    dq_duration_sec = time.perf_counter() - t_dq_start

    dq_metric = {
        "overall_passed": dq_report.overall_passed,
        "total_rules": dq_report.total_rules,
        "passed_rules": dq_report.passed_rules,
        "total_records_evaluated": dq_report.total_records_evaluated,
        "execution_time_ms": round(dq_duration_sec * 1000.0, 2),
    }
    logger.info("Data Quality benchmark: %s", dq_metric)

    # 4. Generate Real 5-Minute Window Aggregation Sample Table
    window_metrics = aggregator.compute_windowed_aggregations(records)
    if window_metrics:
        agg_df = pd.DataFrame([m.to_dict() for m in window_metrics])
        csv_path = RESULTS_DIR / "flight_window_aggregates_sample.csv"
        agg_df.head(25).to_csv(csv_path, index=False)
        logger.info("Saved sample window aggregates to %s", csv_path)

    # 5. Compile and Save Benchmark JSON
    benchmark_payload = {
        "benchmark_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_source": "OpenSky Network Live ADS-B State Vectors (Public API)",
        "dataset_records_count": total_records,
        "dataset_license": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
        "streaming_throughput_benchmarks": throughput_results,
        "compaction_benchmark": compaction_metric,
        "data_quality_benchmark": dq_metric,
    }

    metrics_json_path = RESULTS_DIR / "benchmark_metrics.json"
    with open(metrics_json_path, "w", encoding="utf-8") as f:
        json.dump(benchmark_payload, f, indent=2)
    logger.info("Saved benchmark metrics to %s", metrics_json_path)

    # 6. Generate Publication-Quality Plots
    generate_benchmark_plots(throughput_results, compaction_metric, dq_metric)

    # Cleanup temp lakehouse
    if BENCHMARK_LAKEHOUSE.exists():
        shutil.rmtree(BENCHMARK_LAKEHOUSE)


def generate_benchmark_plots(throughput_data, comp_metric, dq_metric):
    """Generate professional plots for the README."""
    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.patch.set_facecolor("#0e1117")

    for row in axes:
        for ax in row:
            ax.set_facecolor("#161b22")
            ax.tick_params(colors="#c9d1d9")
            ax.xaxis.label.set_color("#c9d1d9")
            ax.yaxis.label.set_color("#c9d1d9")
            ax.title.set_color("#f0f6fc")

    df_tp = pd.DataFrame(throughput_data)

    # Subplot 1: Throughput (Events/sec) vs Batch Size
    ax1 = axes[0, 0]
    ax1.plot(
        df_tp["batch_size"],
        df_tp["throughput_events_per_sec"],
        marker="o",
        linewidth=2.5,
        color="#38bdf8",
        label="Measured Throughput",
    )
    ax1.set_title("Streaming Throughput vs. Micro-Batch Size", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Batch Size (Aircraft Telemetry Records)")
    ax1.set_ylabel("Throughput (Events / sec)")
    for _, row in df_tp.iterrows():
        ax1.annotate(
            f"{int(row['throughput_events_per_sec']):,} e/s",
            (row["batch_size"], row["throughput_events_per_sec"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=9,
            color="#38bdf8",
        )

    # Subplot 2: Latency Percentiles (p50, p95, p99)
    ax2 = axes[0, 1]
    width = 0.25
    x = np.arange(len(df_tp))
    ax2.bar(x - width, df_tp["latency_p50_ms"], width, label="p50 Latency", color="#34d399")
    ax2.bar(x, df_tp["latency_p95_ms"], width, label="p95 Latency", color="#fbbf24")
    ax2.bar(x + width, df_tp["latency_p99_ms"], width, label="p99 Latency", color="#f87171")
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(b) for b in df_tp["batch_size"]])
    ax2.set_title("End-to-End Processing Latency (ms)", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Batch Size (Records)")
    ax2.set_ylabel("Latency (ms)")
    ax2.legend(facecolor="#161b22", edgecolor="#30363d", labelcolor="#c9d1d9")

    # Subplot 3: Lakehouse Storage Compaction Footprint
    ax3 = axes[1, 0]
    sizes_kb = [comp_metric["bytes_before"] / 1024.0, comp_metric["bytes_after"] / 1024.0]
    bars = ax3.bar(["Before Compaction\n(15 micro-batches)", "After Compaction\n(1 consolidated partition)"], sizes_kb, color=["#f43f5e", "#10b981"], width=0.5)
    ax3.set_title(f"Storage Footprint Optimization ({comp_metric['savings_percent']}% Reduction)", fontsize=12, fontweight="bold")
    ax3.set_ylabel("Storage Size (KB)")
    for bar in bars:
        h = bar.get_height()
        ax3.annotate(f"{h:.1f} KB", (bar.get_x() + bar.get_width() / 2, h), textcoords="offset points", xytext=(0, 5), ha="center", color="#f0f6fc", fontsize=10)

    # Subplot 4: Data Quality Assertion Verification
    ax4 = axes[1, 1]
    bars_dq = ax4.bar(
        ["Total Rules Passed", "Execution Time (ms)"],
        [dq_metric["passed_rules"], dq_metric["execution_time_ms"]],
        color=["#6366f1", "#a855f7"],
        width=0.45,
    )
    ax4.set_title("DuckDB In-Memory Data Quality Audit", fontsize=12, fontweight="bold")
    ax4.set_ylabel("Value")
    for bar in bars_dq:
        h = bar.get_height()
        ax4.annotate(f"{h:.1f}", (bar.get_x() + bar.get_width() / 2, h), textcoords="offset points", xytext=(0, 5), ha="center", color="#f0f6fc", fontsize=10)

    plt.tight_layout()
    plot_path = RESULTS_DIR / "benchmark_throughput_latency.png"
    plt.savefig(plot_path, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    logger.info("Saved benchmark plot to %s", plot_path)


if __name__ == "__main__":
    run_comprehensive_benchmark()
