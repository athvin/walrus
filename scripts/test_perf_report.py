#!/usr/bin/env python3
"""Unit tests for the dependency-free local performance report helper."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("perf_report.py")
SPEC = importlib.util.spec_from_file_location("perf_report", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
perf_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(perf_report)


class PerfReportTest(unittest.TestCase):
    def test_cpu_time_parses_macos_and_linux_ps_shapes(self) -> None:
        self.assertEqual(perf_report.parse_cpu_time("01:02"), 62)
        self.assertEqual(perf_report.parse_cpu_time("02:03:04"), 7384)
        self.assertEqual(perf_report.parse_cpu_time("1-02:03:04.5"), 93784.5)

    def test_prometheus_sums_label_sets_and_ignores_prefixes(self) -> None:
        text = """
# TYPE walrus_loader_raw_append_lag_bytes gauge
walrus_loader_raw_append_lag_bytes{table="a"} 3
walrus_loader_raw_append_lag_bytes{table="b"} 4.5
walrus_loader_raw_append_lag_bytes_extra 100
"""
        self.assertEqual(
            perf_report.parse_prometheus(text, "walrus_loader_raw_append_lag_bytes"), 7.5
        )

    def test_finish_writes_efficiency_and_peaks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            metadata = {
                "schema_version": 1,
                "run_id": "run",
                "status": "running",
                "mode": "measure",
                "diagnostic_target": None,
                "build_profile": "release",
                "comparable": True,
                "scenario": "mixed",
                "workload": {},
                "host": {},
                "toolchain": {},
            }
            perf_report.write_json(run_dir / "metadata.json", metadata)
            with (run_dir / "samples.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=perf_report.CSV_COLUMNS)
                writer.writeheader()
                base = {column: 0 for column in perf_report.CSV_COLUMNS}
                writer.writerow(base | {"sink_cpu_seconds": 10, "loader_cpu_seconds": 20})
                writer.writerow(
                    base
                    | {
                        "t_seconds": 5,
                        "sink_cpu_seconds": 12,
                        "loader_cpu_seconds": 24,
                        "sink_rss_bytes": 100,
                        "loader_rss_bytes": 200,
                        "raw_append_lag_bytes": 300,
                    }
                )
            args = argparse.Namespace(
                run_dir=str(run_dir),
                elapsed=5,
                rows_start=100,
                rows_end=2100,
                flush_sum_start=1,
                flush_sum_end=3,
                flush_count_start=2,
                flush_count_end=4,
                spill_start=0,
                spill_end=2,
                failure_reason=[],
            )
            self.assertEqual(perf_report.finish_run(args), 0)
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["throughput"]["rows_per_second"], 400)
            self.assertEqual(summary["cpu"]["total_seconds_per_1000_rows"], 3)
            self.assertEqual(summary["memory"]["loader_peak_rss_bytes"], 200)
            self.assertEqual(summary["pipeline"]["raw_append_lag_peak_bytes"], 300)

    def test_comparison_rejects_workload_and_host_drift(self) -> None:
        base = {
            "status": "success",
            "comparable": True,
            "mode": "measure",
            "diagnostic_target": None,
            "build_profile": "release",
            "scenario": "mixed",
            "workload": {
                "duration_seconds": 30,
                "clients": 4,
                "max_fill": "2s",
                "max_rows": 5000,
                "max_bytes": 2_000_000,
                "max_inflight_bytes": 4_000_000,
                "poll_interval": "1s",
                "sample_interval_seconds": 1,
            },
            "host": {"os": "Darwin", "architecture": "arm64", "cpu_model": "M2"},
            "toolchain": {"rustc": "rustc 1.95.0"},
        }
        candidate = json.loads(json.dumps(base))
        candidate["workload"]["clients"] = 8
        candidate["host"]["cpu_model"] = "M3"
        self.assertEqual(
            perf_report.comparison_mismatches(base, candidate),
            ["workload.clients", "host.cpu_model"],
        )

    def test_resolve_bench_requires_one_matching_executable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            messages = Path(temporary) / "messages.json"
            messages.write_text(
                json.dumps(
                    {
                        "reason": "compiler-artifact",
                        "target": {"name": "decode", "kind": ["bench"]},
                        "executable": "/tmp/decode-123",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            args = argparse.Namespace(cargo_json=str(messages), bench="decode")
            self.assertEqual(perf_report.resolve_bench(args), 0)


if __name__ == "__main__":
    unittest.main()
