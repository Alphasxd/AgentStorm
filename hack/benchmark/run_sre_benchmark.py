#!/usr/bin/env python3
"""Run the no-cost canonical-shape SRE benchmark and produce public artifacts."""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import hashlib
import io
import json
import math
import platform
import sys
from dataclasses import asdict, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "worker/src"))

from agentstorm_worker.benchmarks.sre import SREFakeAdapter
from agentstorm_worker.config import (
    FaultRuleConfig,
    FaultScenarioConfig,
    ReliabilityConfig,
    RetryConfig,
    load_dataset,
    load_run_config,
)
from agentstorm_worker.runner import WorkloadRunner

CAPACITY_ORDERS = ((1, 4, 16, 32), (32, 16, 4, 1), (4, 1, 32, 16))


def prepare_output(output_dir: Path) -> None:
    """Create an empty artifact directory without overwriting prior evidence."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"benchmark output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] if ordered else 0.0


def scenario() -> FaultScenarioConfig:
    rules = (
        FaultRuleConfig(
            name="first-rate-limit",
            fault="rate_limit",
            probability=1,
            case_ids=("INC-001",),
            attempts=(1,),
        ),
        FaultRuleConfig(
            name="timeout-no-retry",
            fault="timeout",
            probability=1,
            case_ids=("INC-002",),
            attempts=(1,),
            delay_ms=1,
        ),
        FaultRuleConfig(
            name="logs-tool-error",
            fault="tool_error",
            probability=1,
            case_ids=("INC-003",),
            attempts=(1,),
            tool_name="search_logs",
        ),
    )
    document = {
        "apiVersion": "agentstorm.io/v1alpha1",
        "kind": "FaultScenario",
        "rules": [asdict(rule) for rule in rules],
    }
    digest = hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return FaultScenarioConfig(
        source_name="sre-reliability",
        source_key="scenario.json",
        digest=f"sha256:{digest}",
        rules=rules,
    )


async def execute_fake(output_dir: Path, image_digest: str) -> None:
    cases = load_dataset(REPO_ROOT / "examples/datasets/sre-incidents.jsonl")
    base = load_run_config(REPO_ROOT / "examples/run.sre.local.json")
    prepare_output(output_dir)
    runs: list[dict[str, Any]] = []
    case_records: list[dict[str, Any]] = []

    for round_index, order in enumerate(CAPACITY_ORDERS, start=1):
        for sequence, workers in enumerate(order, start=1):
            run_id = f"fake-capacity-r{round_index}-s{sequence}-w{workers}"
            config = replace(
                base,
                run_id=run_id,
                workload=replace(base.workload, concurrency=workers, iterations=1),
            )
            results, summary = await WorkloadRunner(config, SREFakeAdapter()).execute(
                cases
            )
            runs.append(
                run_record(run_id, "capacity", workers, round_index, results, summary)
            )
            case_records.extend(case_record(run_id, item) for item in results)

    reliability = ReliabilityConfig(
        seed=42,
        retry=RetryConfig(max_attempts=2, jitter_ratio=0),
        scenario=scenario(),
    )
    run_id = "fake-reliability-w16"
    config = replace(
        base,
        run_id=run_id,
        workload=replace(base.workload, concurrency=16, iterations=1),
        reliability=reliability,
    )
    results, summary = await WorkloadRunner(config, SREFakeAdapter()).execute(cases)
    runs.append(run_record(run_id, "reliability", 16, 1, results, summary))
    case_records.extend(case_record(run_id, item) for item in results)

    complete = (
        len(runs) == 13
        and sum(run["total"] for run in runs if run["kind"] == "capacity") == 384
        and len(case_records) == 416
    )
    manifest = {
        "schema_version": "agentstorm-benchmark/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "fake",
        "canonical": False,
        "complete": complete,
        "model": "fake-sre-lifecycle",
        "adapter_entrypoint": "agentstorm_worker.benchmarks.sre:create_adapter",
        "dataset_cases": len(cases),
        "capacity_orders": CAPACITY_ORDERS,
        "capacity_executions": 384,
        "reliability_executions": 32,
        "worker_image_digest": image_digest or None,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "kubernetes": None,
            "keda": None,
        },
        "limitations": [
            "Fake mode validates lifecycle, assertions, aggregation, and artifact generation only.",
            "It is not a model performance result and contains no KEDA scaling measurements.",
        ],
    }
    write_json(output_dir / "manifest.json", manifest)
    write_json(output_dir / "runs.json", runs)
    write_cases(output_dir / "cases.jsonl.gz", case_records)
    write_summary(output_dir / "summary.csv", runs)
    (output_dir / "REPORT.md").write_text(report(manifest, runs), encoding="utf-8")
    write_checksums(output_dir)
    if not complete:
        raise RuntimeError(
            "fake benchmark did not complete the canonical execution shape"
        )


def run_record(
    run_id: str,
    kind: str,
    workers: int,
    round_index: int,
    results: list[Any],
    summary: Any,
) -> dict[str, Any]:
    latencies = [item.latency_ms for item in results]
    quality = sum(item.failure_category == "evaluation" for item in results)
    infrastructure = sum(
        item.failure_category in {"provider", "tool", "harness"} for item in results
    )
    retries = sum(max(0, len(item.attempts) - 1) for item in results)
    retry_successes = sum(item.success and len(item.attempts) > 1 for item in results)
    injected = sum(
        bool(attempt.injected_fault) for item in results for attempt in item.attempts
    )
    rate_limited = sum(
        attempt.error_code == "rate_limited"
        for item in results
        for attempt in item.attempts
    )
    timeouts = sum(
        attempt.error_code == "timeout" for item in results for attempt in item.attempts
    )
    tool_errors = sum(item.failure_category == "tool" for item in results)
    return {
        "run_id": run_id,
        "kind": kind,
        "round": round_index,
        "max_workers": workers,
        "total": summary.total,
        "succeeded": summary.succeeded,
        "failed": summary.failed,
        "success_rate": summary.succeeded / summary.total if summary.total else 0,
        "duration_ms": summary.duration_ms,
        "throughput_agents_per_second": (
            summary.total / (summary.duration_ms / 1000)
            if summary.duration_ms
            else None
        ),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "quality_failures": quality,
        "quality_failure_rate": quality / summary.total if summary.total else 0,
        "infrastructure_failures": infrastructure,
        "infrastructure_failure_rate": (
            infrastructure / summary.total if summary.total else 0
        ),
        "attempt_count": sum(len(item.attempts) for item in results),
        "retry_count": retries,
        "retry_successes": retry_successes,
        "injected_faults": injected,
        "rate_limited_attempts": rate_limited,
        "timeout_attempts": timeouts,
        "tool_error_cases": tool_errors,
        "model_call_count": summary.model_call_count,
        "tool_call_count": summary.tool_call_count,
        "model_calls_per_successful_agent": summary.model_calls_per_successful_agent,
        "tool_calls_per_successful_agent": summary.tool_calls_per_successful_agent,
        "input_tokens": summary.input_tokens,
        "output_tokens": summary.output_tokens,
        "cost_usd": summary.cost_usd,
        "cost_per_successful_agent_usd": (
            str(Decimal(summary.cost_usd) / summary.succeeded)
            if summary.cost_usd is not None and summary.succeeded
            else None
        ),
        "scale_from_zero_ms": None,
        "peak_workers": None,
        "scale_to_zero_ms": None,
    }


def case_record(run_id: str, result: Any) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "case_id": result.case_id,
        "iteration": result.iteration,
        "success": result.success,
        "latency_ms": result.latency_ms,
        "failure_category": result.failure_category or None,
        "error_code": result.error_code or None,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "model_call_count": result.model_call_count,
        "tool_call_count": result.tool_call_count,
        "tool_path": result.tool_path,
        "attempts": [asdict(attempt) for attempt in result.attempts],
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_cases(path: Path, records: list[dict[str, Any]]) -> None:
    with (
        path.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed,
        io.TextIOWrapper(compressed, encoding="utf-8", newline="\n") as handle,
    ):
        for record in records:
            handle.write(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            )


def write_summary(path: Path, runs: list[dict[str, Any]]) -> None:
    fields = list(runs[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(runs)


def report(manifest: dict[str, Any], runs: list[dict[str, Any]]) -> str:
    capacity = [run for run in runs if run["kind"] == "capacity"]
    grouped: dict[int, list[dict[str, Any]]] = {}
    for run in capacity:
        grouped.setdefault(run["max_workers"], []).append(run)
    baseline = sum(item["duration_ms"] for item in grouped[1]) / len(grouped[1])
    lines = [
        "# AgentStorm SRE benchmark",
        "",
        "> Fake validation only: this report is not a real-model performance claim.",
        "",
        f"Complete canonical shape: **{manifest['complete']}**; capacity executions: **384**; reliability executions: **32**.",
        "",
        "| Workers | Mean duration (ms) | Mean throughput (agents/s) | Speedup | Efficiency |",
        "|---:|---:|---:|---:|---:|",
    ]
    for workers in (1, 4, 16, 32):
        entries = grouped[workers]
        duration = sum(item["duration_ms"] for item in entries) / len(entries)
        throughput = sum(
            item["throughput_agents_per_second"] for item in entries
        ) / len(entries)
        speedup = baseline / duration
        lines.append(
            f"| {workers} | {duration:.3f} | {throughput:.3f} | {speedup:.3f} | {speedup / workers:.3f} |"
        )
    lines.extend(
        [
            "",
            "The real canonical report is produced only after all released-image KEDA runs complete without changing the dataset, model, or matrix.",
            "",
        ]
    )
    return "\n".join(lines)


def write_checksums(output_dir: Path) -> None:
    entries = []
    for path in sorted(output_dir.iterdir()):
        if path.is_file() and path.name != "SHA256SUMS":
            entries.append(
                f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}"
            )
    (output_dir / "SHA256SUMS").write_text("\n".join(entries) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image-digest", default="")
    args = parser.parse_args()
    asyncio.run(execute_fake(args.output, args.image_digest))


if __name__ == "__main__":
    main()
