#!/usr/bin/env python3
"""Run the no-cost fixed-shape SRE benchmark and produce public artifacts."""

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
FAKE_PERFORMANCE_FIELDS = {
    "cost_per_successful_agent_usd",
    "cost_usd",
    "duration_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "peak_workers",
    "scale_from_zero_ms",
    "scale_to_zero_ms",
    "throughput_agents_per_second",
}


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
    runs = [without_fake_performance(item) for item in runs]
    case_records = [without_fake_case_latency(item) for item in case_records]
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
            "It is not a model performance result and contains no latency, throughput, cost, speedup, efficiency, or KEDA scaling measurements.",
        ],
    }
    write_json(output_dir / "manifest.json", manifest)
    write_json(output_dir / "runs.json", runs)
    write_cases(output_dir / "cases.jsonl.gz", case_records)
    write_summary(output_dir / "summary.csv", runs)
    (output_dir / "REPORT.md").write_text(report(manifest, runs), encoding="utf-8")
    write_checksums(output_dir)
    if not complete:
        raise RuntimeError("fake benchmark did not complete the fixed execution shape")


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
    # retry_count and rate_limited_attempts count case restarts. A 429 that is
    # retried inside one chat call and then succeeds does not add to either.
    # in_call_retry_count is those HTTP 429 responses.
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
    in_call_retries = sum(
        attempt.rate_limit_events for item in results for attempt in item.attempts
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
        "in_call_retry_count": in_call_retries,
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
        "provider_error_code": result.provider_error_code or None,
        "in_call_retry_count": result.in_call_retry_count,
        "attempts": [asdict(attempt) for attempt in result.attempts],
    }


def without_fake_performance(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key not in FAKE_PERFORMANCE_FIELDS
    }


def without_fake_case_latency(record: dict[str, Any]) -> dict[str, Any]:
    output = {key: value for key, value in record.items() if key != "latency_ms"}
    output["attempts"] = [
        {key: value for key, value in attempt.items() if key != "latency_ms"}
        for attempt in record["attempts"]
    ]
    return output


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
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(runs)


def report(manifest: dict[str, Any], runs: list[dict[str, Any]]) -> str:
    capacity = [run for run in runs if run["kind"] == "capacity"]
    reliability = next(run for run in runs if run["kind"] == "reliability")
    lines = [
        "# AgentStorm SRE benchmark",
        "",
        "> Fake validation only: this report is not a real-model performance claim.",
        "",
        f"Complete fixed execution shape: **{manifest['complete']}**; capacity executions: **384**; reliability executions: **32**.",
        "",
        "## Capacity execution shape",
        "",
        f"All {sum(run['total'] for run in capacity)} executions succeeded across the fixed 12-run concurrency order.",
        f"They recorded {sum(run['model_call_count'] for run in capacity)} simulated model calls and",
        f"{sum(run['tool_call_count'] for run in capacity)} deterministic tool calls.",
        "",
        "## Reliability execution shape",
        "",
        f"The 32-case reliability run recorded {reliability['succeeded']} successes,",
        f"{reliability['infrastructure_failures']} expected infrastructure failures,",
        f"{reliability['retry_successes']} safe retry success, and",
        f"{reliability['injected_faults']} injected faults.",
        "",
        "Timing and cost fields are intentionally omitted because in-process fake execution is not a Provider or Kubernetes performance measurement.",
        "",
        "A future real-Provider report must be produced from released images and a fixed, separately approved benchmark protocol.",
        "",
    ]
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
