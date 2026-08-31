#!/usr/bin/env python3
"""Local performance run metadata, sampling, summaries, and comparisons for Walrus."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import platform
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
METRIC_COLUMNS = (
    "sink_rows",
    "raw_append_lag_bytes",
    "transform_lag_bytes",
    "files_ready",
    "raw_rows",
    "sink_inflight_bytes",
    "sink_spill_total",
    "replication_lag_bytes",
)
CSV_COLUMNS = (
    "t_seconds",
    *METRIC_COLUMNS,
    "sink_cpu_seconds",
    "loader_cpu_seconds",
    "sink_rss_bytes",
    "loader_rss_bytes",
)
PROMETHEUS_NAMES = {
    "sink_rows": "walrus_sink_parquet_rows_written_total",
    "raw_append_lag_bytes": "walrus_loader_raw_append_lag_bytes",
    "transform_lag_bytes": "walrus_loader_transform_lag_bytes",
    "files_ready": "walrus_loader_files_ready",
    "raw_rows": "walrus_loader_raw_row_count",
    "sink_inflight_bytes": "walrus_sink_inflight_bytes",
    "sink_spill_total": "walrus_sink_spill_total",
    "replication_lag_bytes": "walrus_sink_replication_lag_bytes",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def command_output(*command: str) -> str | None:
    try:
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    value = result.stdout.strip()
    return value or None


def cpu_model() -> str | None:
    value = command_output("sysctl", "-n", "machdep.cpu.brand_string")
    if value:
        return value
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith(("model name", "hardware")):
                return line.split(":", 1)[-1].strip()
    except OSError:
        pass
    return platform.processor() or None


def tool_version(tool: str) -> str | None:
    value = command_output(tool, "--version")
    return value.splitlines()[0] if value else None


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path_or_dir: str) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "summary.json"
    return json.loads(path.read_text(encoding="utf-8"))


def start_run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=False)
    commit = command_output("git", "rev-parse", "HEAD")
    dirty_output = command_output("git", "status", "--porcelain")
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_dir.name,
        "status": "running",
        "started_at": utc_now(),
        "ended_at": None,
        "mode": args.mode,
        "diagnostic_target": args.target,
        "build_profile": args.profile,
        "comparable": args.mode == "measure" and args.profile == "release",
        "scenario": args.scenario,
        "workload": {
            "duration_seconds": args.duration,
            "clients": args.clients,
            "max_fill": args.max_fill,
            "max_rows": args.max_rows,
            "max_bytes": args.max_bytes,
            "max_inflight_bytes": args.max_inflight,
            "poll_interval": args.poll_interval,
            "sample_interval_seconds": args.sample_interval,
        },
        "source": {"git_commit": commit, "git_dirty": bool(dirty_output)},
        "host": {
            "os": platform.system(),
            "kernel": platform.release(),
            "architecture": platform.machine(),
            "cpu_model": cpu_model(),
        },
        "toolchain": {
            "rustc": tool_version("rustc"),
            "cargo": tool_version("cargo"),
            "python": platform.python_version(),
            "samply": tool_version("samply") if args.mode == "cpu" else None,
            "tokio_console": tool_version("tokio-console") if args.mode == "async" else None,
            "console_subscriber": "0.5.0" if args.mode == "async" else None,
            "dhat": "0.3.3" if args.mode == "heap" else None,
        },
    }
    write_json(run_dir / "metadata.json", metadata)
    return 0


def parse_prometheus(text: str, name: str) -> float:
    total = 0.0
    matched = False
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        metric, separator, value = line.rpartition(" ")
        if not separator:
            continue
        bare_name = metric.split("{", 1)[0]
        if bare_name != name:
            continue
        try:
            total += float(value)
            matched = True
        except ValueError:
            continue
    return total if matched else 0.0


def scrape(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.read().decode("utf-8", errors="replace")
    except (OSError, TimeoutError):
        return ""


def parse_cpu_time(value: str) -> float:
    value = value.strip()
    days = 0
    if "-" in value:
        day_text, value = value.split("-", 1)
        days = int(day_text)
    parts = value.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = parts
    elif len(parts) == 2:
        hours = "0"
        minutes, seconds = parts
    else:
        raise ValueError(f"unsupported CPU time: {value!r}")
    return days * 86400 + int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def process_usage(pid: int) -> tuple[float | None, int | None]:
    output = command_output("ps", "-o", "time=", "-o", "rss=", "-p", str(pid))
    if not output:
        return None, None
    fields = output.split()
    if len(fields) < 2:
        return None, None
    try:
        return parse_cpu_time(fields[-2]), int(fields[-1]) * 1024
    except ValueError:
        return None, None


def sample_run(args: argparse.Namespace) -> int:
    keep_running = True

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal keep_running
        keep_running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    output = Path(args.output)
    started = time.monotonic()
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        while keep_running:
            sink_metrics = scrape(args.sink_url)
            loader_metrics = scrape(args.loader_url)
            sink_cpu, sink_rss = process_usage(args.sink_pid)
            loader_cpu, loader_rss = process_usage(args.loader_pid)
            merged = sink_metrics + "\n" + loader_metrics
            row: dict[str, Any] = {"t_seconds": round(time.monotonic() - started, 3)}
            for column in METRIC_COLUMNS:
                row[column] = parse_prometheus(merged, PROMETHEUS_NAMES[column])
            row.update(
                {
                    "sink_cpu_seconds": sink_cpu,
                    "loader_cpu_seconds": loader_cpu,
                    "sink_rss_bytes": sink_rss,
                    "loader_rss_bytes": loader_rss,
                }
            )
            writer.writerow(row)
            handle.flush()
            deadline = time.monotonic() + args.interval
            while keep_running and time.monotonic() < deadline:
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    return 0


def numeric_rows(path: Path) -> list[dict[str, float | None]]:
    rows: list[dict[str, float | None]] = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            parsed: dict[str, float | None] = {}
            for key, value in raw.items():
                try:
                    parsed[key] = float(value) if value not in (None, "") else None
                except ValueError:
                    parsed[key] = None
            rows.append(parsed)
    return rows


def first_last_delta(rows: list[dict[str, float | None]], key: str) -> float | None:
    values = [row[key] for row in rows if row.get(key) is not None]
    if len(values) < 2:
        return None
    return max(0.0, float(values[-1]) - float(values[0]))


def peak(rows: list[dict[str, float | None]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return max(values) if values else None


def per_thousand(cpu_seconds: float | None, rows: int) -> float | None:
    if cpu_seconds is None or rows <= 0:
        return None
    return cpu_seconds * 1000 / rows


def finish_run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    samples = numeric_rows(run_dir / "samples.csv")
    row_count = max(0, round(args.rows_end - args.rows_start))
    sink_cpu = first_last_delta(samples, "sink_cpu_seconds")
    loader_cpu = first_last_delta(samples, "loader_cpu_seconds")
    total_cpu = None if sink_cpu is None or loader_cpu is None else sink_cpu + loader_cpu
    flush_count = max(0, round(args.flush_count_end - args.flush_count_start))
    flush_latency = None
    if flush_count > 0:
        flush_latency = (args.flush_sum_end - args.flush_sum_start) / flush_count
    spill_count = max(0, round(args.spill_end - args.spill_start))
    reasons = [reason for reason in args.failure_reason if reason]
    status = "success" if not reasons else "failed"
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": metadata["run_id"],
        "status": status,
        "failure_reasons": reasons,
        "comparable": metadata["comparable"] and status == "success",
        "mode": metadata["mode"],
        "diagnostic_target": metadata.get("diagnostic_target"),
        "build_profile": metadata["build_profile"],
        "scenario": metadata["scenario"],
        "workload": metadata["workload"],
        "host": metadata["host"],
        "toolchain": metadata["toolchain"],
        "throughput": {
            "rows": row_count,
            "elapsed_seconds": args.elapsed,
            "rows_per_second": row_count / args.elapsed if args.elapsed > 0 else None,
        },
        "cpu": {
            "sink_seconds": sink_cpu,
            "loader_seconds": loader_cpu,
            "total_seconds": total_cpu,
            "sink_seconds_per_1000_rows": per_thousand(sink_cpu, row_count),
            "loader_seconds_per_1000_rows": per_thousand(loader_cpu, row_count),
            "total_seconds_per_1000_rows": per_thousand(total_cpu, row_count),
        },
        "memory": {
            "sink_peak_rss_bytes": peak(samples, "sink_rss_bytes"),
            "loader_peak_rss_bytes": peak(samples, "loader_rss_bytes"),
        },
        "pipeline": {
            "mean_flush_latency_seconds": flush_latency,
            "flush_count": flush_count,
            "spill_count": spill_count,
            "sink_inflight_peak_bytes": peak(samples, "sink_inflight_bytes"),
            "raw_append_lag_peak_bytes": peak(samples, "raw_append_lag_bytes"),
            "transform_lag_peak_bytes": peak(samples, "transform_lag_bytes"),
            "files_ready_peak": peak(samples, "files_ready"),
            "raw_rows_peak": peak(samples, "raw_rows"),
            "replication_lag_peak_bytes": peak(samples, "replication_lag_bytes"),
        },
        "artifacts": {
            "metadata": "metadata.json",
            "samples": "samples.csv",
            "sink_log": "sink.log",
            "loader_log": "loader.log",
            "diagnostics": sorted(
                path.name
                for path in run_dir.iterdir()
                if path.is_file()
                and any(
                    marker in path.name
                    for marker in ("samply.json", "dhat-heap", "tokio-console")
                )
            ),
        },
    }
    metadata["status"] = status
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    write_json(run_dir / "summary.json", summary)
    print_summary(summary, run_dir)
    return 0 if status == "success" else 1


def fail_run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        return 0
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata.get("status") != "running":
        return 0
    metadata["status"] = "failed"
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": metadata["run_id"],
        "status": "failed",
        "failure_reasons": [args.reason],
        "comparable": False,
        "mode": metadata["mode"],
        "diagnostic_target": metadata.get("diagnostic_target"),
        "build_profile": metadata["build_profile"],
        "scenario": metadata["scenario"],
        "workload": metadata["workload"],
        "host": metadata["host"],
        "toolchain": metadata["toolchain"],
    }
    write_json(run_dir / "summary.json", summary)
    return 0


def resolve_bench(args: argparse.Namespace) -> int:
    matches: list[str] = []
    with Path(args.cargo_json).open(encoding="utf-8") as handle:
        for line in handle:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            target = message.get("target", {})
            if (
                message.get("reason") == "compiler-artifact"
                and target.get("name") == args.bench
                and "bench" in target.get("kind", [])
                and message.get("executable")
            ):
                matches.append(str(message["executable"]))
    unique = sorted(set(matches))
    if len(unique) != 1:
        print(
            f"expected one executable for benchmark {args.bench!r}, found {unique}",
            file=sys.stderr,
        )
        return 2
    print(unique[0])
    return 0


def complete_artifact(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    artifact = run_dir / args.artifact
    status = "success" if artifact.is_file() and artifact.stat().st_size > 0 else "failed"
    reasons = [] if status == "success" else [f"missing or empty artifact: {args.artifact}"]
    metadata["status"] = status
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    write_json(
        run_dir / "summary.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": metadata["run_id"],
            "status": status,
            "failure_reasons": reasons,
            "comparable": False,
            "mode": metadata["mode"],
            "diagnostic_target": metadata.get("diagnostic_target"),
            "build_profile": metadata["build_profile"],
            "scenario": metadata["scenario"],
            "workload": metadata["workload"],
            "host": metadata["host"],
            "toolchain": metadata["toolchain"],
            "artifacts": {"profile": args.artifact, "cargo_messages": "cargo-artifacts.json"},
        },
    )
    print(f"profile bundle: {run_dir}")
    return 0 if status == "success" else 1


def format_number(value: Any, unit: str = "") -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if unit == "bytes":
        for suffix in ("B", "KiB", "MiB", "GiB"):
            if abs(number) < 1024 or suffix == "GiB":
                return f"{number:.2f} {suffix}"
            number /= 1024
    return f"{number:.4f}{unit}"


def print_summary(summary: dict[str, Any], run_dir: Path) -> None:
    throughput = summary["throughput"]
    cpu = summary["cpu"]
    memory = summary["memory"]
    pipeline = summary["pipeline"]
    print("\n==========================================================================")
    print(f"  perf SUMMARY — scenario={summary['scenario']} mode={summary['mode']}")
    print(f"  status={summary['status']} profile={summary['build_profile']}")
    print("--------------------------------------------------------------------------")
    print(
        f"  rows={throughput['rows']}  rows/s={format_number(throughput['rows_per_second'])}  "
        f"elapsed={format_number(throughput['elapsed_seconds'], 's')}"
    )
    print(
        "  CPU seconds / 1k rows: "
        f"sink={format_number(cpu['sink_seconds_per_1000_rows'])}  "
        f"loader={format_number(cpu['loader_seconds_per_1000_rows'])}  "
        f"total={format_number(cpu['total_seconds_per_1000_rows'])}"
    )
    print(
        "  peak RSS: "
        f"sink={format_number(memory['sink_peak_rss_bytes'], 'bytes')}  "
        f"loader={format_number(memory['loader_peak_rss_bytes'], 'bytes')}"
    )
    print(
        "  peak lag: "
        f"raw={format_number(pipeline['raw_append_lag_peak_bytes'], 'bytes')}  "
        f"transform={format_number(pipeline['transform_lag_peak_bytes'], 'bytes')}  "
        f"inflight={format_number(pipeline['sink_inflight_peak_bytes'], 'bytes')}"
    )
    if summary["failure_reasons"]:
        print(f"  failures: {'; '.join(summary['failure_reasons'])}")
    print(f"  run bundle: {run_dir}")
    print("==========================================================================\n")


def nested(value: dict[str, Any], path: str) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def comparison_mismatches(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
    paths = (
        "mode",
        "diagnostic_target",
        "build_profile",
        "scenario",
        "workload.duration_seconds",
        "workload.clients",
        "workload.max_fill",
        "workload.max_rows",
        "workload.max_bytes",
        "workload.max_inflight_bytes",
        "workload.poll_interval",
        "workload.sample_interval_seconds",
        "host.os",
        "host.architecture",
        "host.cpu_model",
        "toolchain.rustc",
    )
    mismatches = [path for path in paths if nested(left, path) != nested(right, path)]
    if not left.get("comparable") or not right.get("comparable"):
        mismatches.append("run.comparable")
    if left.get("status") != "success" or right.get("status") != "success":
        mismatches.append("run.status")
    return mismatches


def compare_runs(args: argparse.Namespace) -> int:
    baseline = read_json(args.baseline)
    candidate = read_json(args.candidate)
    mismatches = comparison_mismatches(baseline, candidate)
    if mismatches and not args.allow_mismatch:
        print("incompatible performance runs:", file=sys.stderr)
        for path in mismatches:
            print(
                f"  {path}: {nested(baseline, path)!r} != {nested(candidate, path)!r}",
                file=sys.stderr,
            )
        print("use --allow-mismatch only for an explicitly non-authoritative comparison", file=sys.stderr)
        return 2
    if mismatches:
        print("WARNING: comparing mismatched runs: " + ", ".join(mismatches))
    metrics = (
        ("rows/s", "throughput.rows_per_second", True),
        ("elapsed seconds", "throughput.elapsed_seconds", False),
        ("total CPU s/1k rows", "cpu.total_seconds_per_1000_rows", False),
        ("sink CPU s/1k rows", "cpu.sink_seconds_per_1000_rows", False),
        ("loader CPU s/1k rows", "cpu.loader_seconds_per_1000_rows", False),
        ("sink peak RSS", "memory.sink_peak_rss_bytes", False),
        ("loader peak RSS", "memory.loader_peak_rss_bytes", False),
        ("raw lag peak", "pipeline.raw_append_lag_peak_bytes", False),
        ("transform lag peak", "pipeline.transform_lag_peak_bytes", False),
        ("sink inflight peak", "pipeline.sink_inflight_peak_bytes", False),
        ("mean flush latency", "pipeline.mean_flush_latency_seconds", False),
        ("spill count", "pipeline.spill_count", False),
    )
    print(f"baseline:  {baseline['run_id']}")
    print(f"candidate: {candidate['run_id']}")
    print(f"{'metric':27} {'baseline':>14} {'candidate':>14} {'change':>12}  result")
    for label, path, higher_is_better in metrics:
        before = nested(baseline, path)
        after = nested(candidate, path)
        if before is None or after is None:
            print(f"{label:27} {'n/a':>14} {'n/a':>14} {'n/a':>12}  unknown")
            continue
        if float(before) == 0:
            change = None
        else:
            change = (float(after) - float(before)) / float(before) * 100
        if change is None:
            change_text = "n/a"
            result = "unknown"
        else:
            improvement = change if higher_is_better else -change
            result = "better" if improvement > 0 else "worse" if improvement < 0 else "same"
            change_text = f"{change:+.2f}%"
        print(f"{label:27} {float(before):14.4f} {float(after):14.4f} {change_text:>12}  {result}")
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start")
    start.add_argument("--run-dir", required=True)
    start.add_argument("--mode", choices=("measure", "cpu", "heap", "async"), required=True)
    start.add_argument("--target", choices=("sink", "loader"))
    start.add_argument("--profile", required=True)
    start.add_argument("--scenario", required=True)
    start.add_argument("--duration", type=int, required=True)
    start.add_argument("--clients", type=int, required=True)
    start.add_argument("--max-fill", required=True)
    start.add_argument("--max-rows", type=int, required=True)
    start.add_argument("--max-bytes", type=int, required=True)
    start.add_argument("--max-inflight", type=int, required=True)
    start.add_argument("--poll-interval", required=True)
    start.add_argument("--sample-interval", type=float, required=True)
    start.set_defaults(function=start_run)

    sample = commands.add_parser("sample")
    sample.add_argument("--sink-pid", type=int, required=True)
    sample.add_argument("--loader-pid", type=int, required=True)
    sample.add_argument("--sink-url", required=True)
    sample.add_argument("--loader-url", required=True)
    sample.add_argument("--output", required=True)
    sample.add_argument("--interval", type=float, default=1.0)
    sample.set_defaults(function=sample_run)

    finish = commands.add_parser("finish")
    finish.add_argument("--run-dir", required=True)
    finish.add_argument("--elapsed", type=float, required=True)
    finish.add_argument("--rows-start", type=float, required=True)
    finish.add_argument("--rows-end", type=float, required=True)
    finish.add_argument("--flush-sum-start", type=float, required=True)
    finish.add_argument("--flush-sum-end", type=float, required=True)
    finish.add_argument("--flush-count-start", type=float, required=True)
    finish.add_argument("--flush-count-end", type=float, required=True)
    finish.add_argument("--spill-start", type=float, required=True)
    finish.add_argument("--spill-end", type=float, required=True)
    finish.add_argument("--failure-reason", action="append", default=[])
    finish.set_defaults(function=finish_run)

    fail = commands.add_parser("fail")
    fail.add_argument("--run-dir", required=True)
    fail.add_argument("--reason", required=True)
    fail.set_defaults(function=fail_run)

    resolve = commands.add_parser("resolve-bench")
    resolve.add_argument("--cargo-json", required=True)
    resolve.add_argument("--bench", required=True)
    resolve.set_defaults(function=resolve_bench)

    complete = commands.add_parser("complete-artifact")
    complete.add_argument("--run-dir", required=True)
    complete.add_argument("--artifact", required=True)
    complete.set_defaults(function=complete_artifact)

    compare = commands.add_parser("compare")
    compare.add_argument("baseline")
    compare.add_argument("candidate")
    compare.add_argument("--allow-mismatch", action="store_true")
    compare.set_defaults(function=compare_runs)
    return root


def main() -> int:
    args = parser().parse_args()
    return int(args.function(args))


if __name__ == "__main__":
    raise SystemExit(main())
