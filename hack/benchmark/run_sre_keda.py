#!/usr/bin/env python3
"""Run the canonical released-image SRE benchmark on a KEDA-enabled cluster."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "hack/benchmark"))

from run_sre_benchmark import (
    CAPACITY_ORDERS,
    percentile,
    prepare_output,
    write_cases,
    write_checksums,
    write_json,
    write_summary,
)

MODEL = "gpt-5.6-luna"
SPEND_STOP_USD = Decimal(8)
IMAGE_DIGEST = re.compile(r"^[^\s]+@sha256:[a-f0-9]{64}$")
PRICE = re.compile(r"^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$")


class BenchmarkError(RuntimeError):
    pass


class Kubernetes:
    def __init__(self, context: str, namespace: str) -> None:
        self.context = context
        self.namespace = namespace

    def command(self, *arguments: str, input_bytes: bytes | None = None) -> bytes:
        command = ["kubectl"]
        if self.context:
            command.extend(["--context", self.context])
        command.extend(arguments)
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            input=input_bytes,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            message = result.stderr.decode("utf-8", errors="replace").strip()
            raise BenchmarkError(f"kubectl command failed: {message[:500]}")
        return result.stdout

    def apply(self, value: dict[str, Any]) -> None:
        self.command("apply", "-f", "-", input_bytes=json.dumps(value).encode())

    def delete(self, kind: str, name: str) -> None:
        self.command(
            "delete",
            kind,
            name,
            "-n",
            self.namespace,
            "--ignore-not-found",
            "--wait=true",
        )

    def get_json(self, kind: str, name: str = "", *, labels: str = "") -> Any:
        arguments = ["get", kind]
        if name:
            arguments.append(name)
        arguments.extend(["-n", self.namespace, "-o", "json"])
        if labels:
            arguments.extend(["-l", labels])
        return json.loads(self.command(*arguments))

    def install_api_key(self, name: str, api_key: str) -> None:
        # The key is stdin, never a process argument, manifest file, or command log.
        secret = self.command(
            "create",
            "secret",
            "generic",
            name,
            "-n",
            self.namespace,
            "--from-file=api-key=/dev/stdin",
            "--dry-run=client",
            "-o",
            "json",
            input_bytes=api_key.encode(),
        )
        self.command("apply", "-f", "-", input_bytes=secret)


class ResultReader:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def get(self, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": "Bearer " + self.token,
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except Exception as exc:
            raise BenchmarkError("Result API request failed") from exc

    def cases(self, run_id: str) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        cursor = ""
        while True:
            page = self.get(
                f"/v1/runs/{urllib.parse.quote(run_id, safe='')}/cases",
                {"limit": "1000", **({"cursor": cursor} if cursor else {})},
            )
            output.extend(page.get("cases", []))
            cursor = str(page.get("next_cursor") or "")
            if not cursor:
                return output


def config_map(name: str, namespace: str, key: str, value: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace},
        "data": {key: value},
    }


def scenario_document() -> dict[str, Any]:
    return {
        "apiVersion": "agentstorm.io/v1alpha1",
        "kind": "FaultScenario",
        "rules": [
            {
                "name": "first-rate-limit",
                "fault": "rate_limit",
                "probability": 1,
                "caseIDs": ["INC-001"],
                "attempts": [1],
            },
            {
                "name": "timeout-no-retry",
                "fault": "timeout",
                "probability": 1,
                "caseIDs": ["INC-002"],
                "attempts": [1],
                "delayMs": 100,
            },
            {
                "name": "logs-tool-error",
                "fault": "tool_error",
                "probability": 1,
                "caseIDs": ["INC-003"],
                "attempts": [1],
                "toolName": "search_logs",
            },
        ],
    }


def run_resource(
    name: str,
    namespace: str,
    dataset_name: str,
    scenario_name: str,
    api_secret: str,
    worker_image: str,
    workers: int,
    input_price: str,
    output_price: str,
    reliability: bool,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "target": {
            "provider": "openai-agents",
            "model": MODEL,
            "adapterEntrypoint": "agentstorm_worker.benchmarks.sre:create_adapter",
            "apiKeySecretRef": {"name": api_secret, "key": "api-key"},
            "pricing": {
                "inputUSDPerMillionTokens": input_price,
                "outputUSDPerMillionTokens": output_price,
            },
        },
        "workload": {
            "datasetRef": {"name": dataset_name, "key": "cases.jsonl"},
            "parallelism": 32,
            "concurrencyPerWorker": 1,
            "iterations": 1,
            "timeoutSeconds": 1800,
        },
        "evaluation": ({} if reliability else {"minSuccessRate": 1, "maxErrorRate": 0}),
        "scheduling": {
            "strategy": "keda",
            "maxWorkers": workers,
            "resourceProfile": "small",
        },
        "runner": {"image": worker_image, "imagePullPolicy": "IfNotPresent"},
    }
    if reliability:
        spec["reliability"] = {
            "seed": 42,
            "scenarioRef": {"name": scenario_name, "key": "scenario.json"},
            "retry": {"maxAttempts": 2, "jitterRatio": 0},
        }
    return {
        "apiVersion": "agentstorm.io/v1alpha1",
        "kind": "AgentTestRun",
        "metadata": {"name": name, "namespace": namespace},
        "spec": spec,
    }


def active_workers(pods: dict[str, Any]) -> int:
    return sum(
        item.get("status", {}).get("phase") in {"Pending", "Running"}
        for item in pods.get("items", [])
    )


def execute_run(
    kube: Kubernetes,
    reader: ResultReader,
    resource: dict[str, Any],
    timeout_seconds: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    name = resource["metadata"]["name"]
    started = time.monotonic()
    kube.apply(resource)
    run_uid = ""
    first_worker_at: float | None = None
    peak_workers = 0
    detail: dict[str, Any] | None = None
    while time.monotonic() - started < timeout_seconds:
        run = kube.get_json("agenttestrun", name)
        run_uid = str(run.get("metadata", {}).get("uid") or run_uid)
        pods = kube.get_json("pods", labels=f"agentstorm.io/run={name}")
        current = active_workers(pods)
        peak_workers = max(peak_workers, current)
        if current and first_worker_at is None:
            first_worker_at = time.monotonic()
        if run_uid:
            try:
                candidate = reader.get(
                    f"/v1/runs/{urllib.parse.quote(run_uid, safe='')}"
                )
                if candidate.get("status") == "complete":
                    detail = candidate
                    break
            except BenchmarkError:
                pass
        time.sleep(1)
    if detail is None or not run_uid:
        raise BenchmarkError(f"run {name} did not complete within the timeout")
    completed = time.monotonic()
    while time.monotonic() - completed < 300:
        if (
            active_workers(kube.get_json("pods", labels=f"agentstorm.io/run={name}"))
            == 0
        ):
            break
        time.sleep(1)
    else:
        raise BenchmarkError(f"run {name} did not scale back to zero")
    scaled_zero = time.monotonic()
    cases = reader.cases(run_uid)
    if len(cases) != 32:
        raise BenchmarkError(f"run {name} returned {len(cases)} cases instead of 32")
    summary = detail.get("summary") or {}
    latencies = [float(item["latency_ms"]) for item in cases]
    succeeded = int(summary.get("succeeded", 0))
    total = int(summary.get("total", 0))
    quality_failures = int(summary.get("quality_failures", 0))
    infrastructure_failures = int(summary.get("infrastructure_failures", 0))
    error_codes = Counter(
        str(attempt.get("error_code") or "")
        for case in cases
        for attempt in case.get("attempts", [])
    )
    tool_errors = sum(case.get("failure_category") == "tool" for case in cases)
    duration_ms = (completed - started) * 1000
    record = {
        "run_id": run_uid,
        "resource_name": name,
        "kind": "reliability" if resource["spec"].get("reliability") else "capacity",
        "round": int(resource["metadata"].get("annotations", {}).get("round", "1")),
        "max_workers": resource["spec"]["scheduling"]["maxWorkers"],
        "total": total,
        "succeeded": succeeded,
        "failed": int(summary.get("failed", 0)),
        "success_rate": succeeded / total if total else 0,
        "duration_ms": duration_ms,
        "throughput_agents_per_second": 32 / (duration_ms / 1000),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "quality_failures": quality_failures,
        "quality_failure_rate": quality_failures / total if total else 0,
        "infrastructure_failures": infrastructure_failures,
        "infrastructure_failure_rate": infrastructure_failures / total if total else 0,
        "attempt_count": int(summary.get("attempt_count", 0)),
        "retry_count": int(summary.get("retry_count", 0)),
        "retry_successes": int(summary.get("retry_successes", 0)),
        "injected_faults": int(summary.get("injected_faults", 0)),
        "rate_limited_attempts": error_codes["rate_limited"],
        "timeout_attempts": error_codes["timeout"],
        "tool_error_cases": tool_errors,
        "model_call_count": summary.get("model_call_count"),
        "tool_call_count": int(summary.get("tool_call_count", 0)),
        "model_calls_per_successful_agent": summary.get(
            "model_calls_per_successful_agent"
        ),
        "tool_calls_per_successful_agent": summary.get(
            "tool_calls_per_successful_agent"
        ),
        "input_tokens": int(summary.get("input_tokens", 0)),
        "output_tokens": int(summary.get("output_tokens", 0)),
        "cost_usd": summary.get("cost_usd"),
        "cost_per_successful_agent_usd": (
            str(Decimal(summary["cost_usd"]) / succeeded)
            if summary.get("cost_usd") is not None and succeeded
            else None
        ),
        "scale_from_zero_ms": (
            (first_worker_at - started) * 1000 if first_worker_at is not None else None
        ),
        "peak_workers": peak_workers,
        "scale_to_zero_ms": (scaled_zero - completed) * 1000,
    }
    return record, [{"run_id": run_uid, **case} for case in cases]


def _deployment_image(kube: Kubernetes, name: str) -> str:
    deployment = kube.get_json("deployment", name)
    containers = (
        deployment.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )
    if len(containers) != 1 or not IMAGE_DIGEST.fullmatch(
        str(containers[0].get("image", ""))
    ):
        raise BenchmarkError(
            f"deployment {name} must use one immutable digest-pinned image"
        )
    return str(containers[0]["image"])


def worker_runtime_metadata(
    kube: Kubernetes, image: str, pod_name: str
) -> dict[str, str]:
    code = (
        "import importlib.metadata,json,platform;"
        "print(json.dumps({'python':platform.python_version(),"
        "'agentstorm_worker':importlib.metadata.version('agentstorm-worker'),"
        "'openai_agents':importlib.metadata.version('openai-agents')}))"
    )
    try:
        kube.command(
            "run",
            pod_name,
            "-n",
            kube.namespace,
            f"--image={image}",
            "--restart=Never",
            "--command",
            "--",
            "python",
            "-c",
            code,
        )
        kube.command(
            "wait",
            "--for=jsonpath={.status.phase}=Succeeded",
            f"pod/{pod_name}",
            "-n",
            kube.namespace,
            "--timeout=300s",
        )
        return json.loads(kube.command("logs", pod_name, "-n", kube.namespace))
    finally:
        try:
            kube.delete("pod", pod_name)
        except BenchmarkError:
            pass


def cluster_metadata(
    kube: Kubernetes, worker_image: str, prefix: str
) -> dict[str, Any]:
    version = json.loads(kube.command("version", "-o", "json"))
    nodes = json.loads(kube.command("get", "nodes", "-o", "json"))
    keda = json.loads(
        kube.command("get", "deployment", "keda-operator", "-n", "keda", "-o", "json")
    )
    return {
        "kubernetes": version.get("serverVersion", {}).get("gitVersion"),
        "benchmark_runner_python": platform.python_version(),
        "architectures": sorted(
            {
                item.get("status", {}).get("nodeInfo", {}).get("architecture")
                for item in nodes.get("items", [])
            }
        ),
        "keda_image": keda["spec"]["template"]["spec"]["containers"][0]["image"],
        "node_images": sorted(
            {
                item.get("status", {}).get("nodeInfo", {}).get("osImage")
                for item in nodes.get("items", [])
            }
        ),
        "images": {
            "controller": _deployment_image(kube, "agentstorm-controller"),
            "worker": worker_image,
            "result_api": _deployment_image(kube, "agentstorm-result-api"),
        },
        "worker_runtime": worker_runtime_metadata(
            kube, worker_image, prefix + "-runtime"
        ),
    }


def real_report(manifest: dict[str, Any], runs: list[dict[str, Any]]) -> str:
    capacity = [run for run in runs if run["kind"] == "capacity"]
    grouped: dict[int, list[dict[str, Any]]] = {}
    for run in capacity:
        grouped.setdefault(run["max_workers"], []).append(run)
    baseline_duration = (
        sum(item["duration_ms"] for item in grouped.get(1, []))
        / len(grouped.get(1, []))
        if grouped.get(1)
        else 0
    )
    lines = [
        "# AgentStorm SRE Agent canonical benchmark",
        "",
        f"Model: `{MODEL}`; complete: **{manifest['complete']}**; Worker: `{manifest['worker_image']}`.",
        "",
        "Capacity rows summarize complete Agent executions, including all model and tool calls.",
        "",
        "| Workers | Mean duration (s) | Agents/s | P50/P95/P99 ms | Success | Speedup | Efficiency | Mean model/tool calls | Mean cost USD | Scale 0→1 / →0 ms | Peak |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for workers in (1, 4, 16, 32):
        entries = grouped.get(workers, [])
        if not entries:
            continue

        def mean(field: str) -> float:
            values = [
                float(item[field]) for item in entries if item.get(field) is not None
            ]
            return sum(values) / len(values) if values else float("nan")

        duration = mean("duration_ms")
        speedup = baseline_duration / duration if baseline_duration and duration else 0
        model_calls = [
            float(item["model_calls_per_successful_agent"])
            for item in entries
            if item["model_calls_per_successful_agent"] is not None
        ]
        model_mean = (
            sum(model_calls) / len(model_calls) if model_calls else float("nan")
        )
        lines.append(
            f"| {workers} | {duration / 1000:.3f} | {mean('throughput_agents_per_second'):.3f} | "
            f"{mean('p50_ms'):.1f}/{mean('p95_ms'):.1f}/{mean('p99_ms'):.1f} | "
            f"{sum(item['succeeded'] for item in entries)}/{sum(item['total'] for item in entries)} | "
            f"{speedup:.3f} | {speedup / workers:.3f} | {model_mean:.2f}/{mean('tool_calls_per_successful_agent'):.2f} | "
            f"{mean('cost_usd'):.6f} | {mean('scale_from_zero_ms'):.0f}/{mean('scale_to_zero_ms'):.0f} | "
            f"{max((item['peak_workers'] or 0) for item in entries)} |"
        )
    reliability = next((run for run in runs if run["kind"] == "reliability"), None)
    if reliability is not None:
        lines.extend(
            [
                "",
                "## Reliability run",
                "",
                f"At 16 Workers: {reliability['succeeded']}/{reliability['total']} succeeded; "
                f"quality failures {reliability['quality_failures']}; infrastructure failures "
                f"{reliability['infrastructure_failures']}; 429 attempts "
                f"{reliability['rate_limited_attempts']}; timeouts {reliability['timeout_attempts']}; "
                f"tool errors {reliability['tool_error_cases']}; retry successes "
                f"{reliability['retry_successes']}.",
            ]
        )
    lines.extend(
        [
            "",
            "The reliability run is reported separately and is not folded into capacity speedup.",
            "",
            "## Environment",
            "",
            "```json",
            json.dumps(manifest.get("environment", {}), indent=2, sort_keys=True),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def scan_secrets(output: Path, secrets: tuple[str, ...]) -> None:
    needles = [value.encode() for value in secrets if value]
    for path in output.iterdir():
        if not path.is_file():
            continue
        payload = (
            gzip.decompress(path.read_bytes())
            if path.suffix == ".gz"
            else path.read_bytes()
        )
        if any(needle in payload for needle in needles):
            raise BenchmarkError(f"secret material detected in artifact {path.name}")


def capacity_evidence_complete(runs: list[dict[str, Any]]) -> bool:
    capacity = [run for run in runs if run["kind"] == "capacity"]
    return (
        len(capacity) == 12
        and all(
            run["total"] == 32
            and run["succeeded"] == 32
            and run["failed"] == 0
            and run["quality_failures"] == 0
            and run["infrastructure_failures"] == 0
            and run["model_call_count"] is not None
            and run["model_call_count"] > 0
            and run["tool_call_count"] == 128
            and run["cost_usd"] is not None
            and run["scale_from_zero_ms"] is not None
            and run["peak_workers"] <= run["max_workers"]
            and run["scale_to_zero_ms"] is not None
            for run in capacity
        )
        and any(
            run["max_workers"] == 32 and run["peak_workers"] == 32 for run in capacity
        )
    )


def reliability_evidence_complete(
    runs: list[dict[str, Any]], cases: list[dict[str, Any]]
) -> bool:
    reliability = [run for run in runs if run["kind"] == "reliability"]
    if len(reliability) != 1:
        return False
    run = reliability[0]
    run_cases = {
        item["case_id"]: item for item in cases if item["run_id"] == run["run_id"]
    }
    if set(run_cases) != {f"INC-{index:03d}" for index in range(1, 33)}:
        return False
    rate_limit = run_cases["INC-001"]
    timeout = run_cases["INC-002"]
    tool_error = run_cases["INC-003"]
    return (
        run["total"] == 32
        and run["succeeded"] == 30
        and run["failed"] == 2
        and run["quality_failures"] == 0
        and run["infrastructure_failures"] == 2
        and run["rate_limited_attempts"] == 1
        and run["timeout_attempts"] == 1
        and run["tool_error_cases"] == 1
        and run["retry_count"] == 1
        and run["retry_successes"] == 1
        and run["injected_faults"] == 3
        and rate_limit.get("success") is True
        and len(rate_limit.get("attempts", [])) == 2
        and rate_limit["attempts"][0].get("error_code") == "rate_limited"
        and rate_limit["attempts"][0].get("retry_decision") == "retry_safe"
        and timeout.get("success") is False
        and len(timeout.get("attempts", [])) == 1
        and timeout["attempts"][0].get("error_code") == "timeout"
        and timeout["attempts"][0].get("retry_decision") == "ambiguous_blocked"
        and tool_error.get("success") is False
        and tool_error.get("failure_category") == "tool"
        and tool_error.get("error_code") == "injected_tool_error"
        and all(run_cases[f"INC-{index:03d}"].get("success") for index in range(4, 33))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="agentstorm-system")
    parser.add_argument("--kube-context", default="")
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--result-api-url", required=True)
    parser.add_argument("--input-price", required=True)
    parser.add_argument("--output-price", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=2400)
    parser.add_argument("--keep-resources", action="store_true")
    args = parser.parse_args()
    if not IMAGE_DIGEST.fullmatch(args.worker_image):
        raise SystemExit("--worker-image must be an immutable sha256 digest reference")
    if not PRICE.fullmatch(args.input_price) or not PRICE.fullmatch(args.output_price):
        raise SystemExit("prices must be non-negative decimal strings")
    if not 60 <= args.timeout_seconds <= 7200:
        raise SystemExit("--timeout-seconds must be between 60 and 7200")
    api_key = os.environ.get("OPENAI_API_KEY", "")
    read_token = os.environ.get("AGENTSTORM_RESULT_READ_TOKEN", "")
    if not api_key or not read_token:
        raise SystemExit("OPENAI_API_KEY and AGENTSTORM_RESULT_READ_TOKEN are required")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    prefix = f"sre-{timestamp}"
    dataset_name = prefix + "-dataset"
    scenario_name = prefix + "-scenario"
    secret_name = prefix + "-openai"
    kube = Kubernetes(args.kube_context, args.namespace)
    reader = ResultReader(args.result_api_url, read_token)
    try:
        prepare_output(args.output)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    created_runs: list[str] = []
    runs: list[dict[str, Any]] = []
    cases: list[dict[str, Any]] = []
    stopped_for_budget = False
    error: str | None = None
    try:
        metadata = cluster_metadata(kube, args.worker_image, prefix)
        dataset_path = REPO_ROOT / "examples/datasets/sre-incidents.jsonl"
        dataset = dataset_path.read_text(encoding="utf-8")
        kube.apply(config_map(dataset_name, args.namespace, "cases.jsonl", dataset))
        kube.apply(
            config_map(
                scenario_name,
                args.namespace,
                "scenario.json",
                json.dumps(scenario_document(), sort_keys=True, separators=(",", ":")),
            )
        )
        kube.install_api_key(secret_name, api_key)
        spent = Decimal(0)
        for round_index, order in enumerate(CAPACITY_ORDERS, start=1):
            for sequence, workers in enumerate(order, start=1):
                if spent >= SPEND_STOP_USD:
                    stopped_for_budget = True
                    raise BenchmarkError(
                        "USD 8 recorded-cost stop reached before the next run"
                    )
                name = f"{prefix}-r{round_index}s{sequence}w{workers}"
                resource = run_resource(
                    name,
                    args.namespace,
                    dataset_name,
                    scenario_name,
                    secret_name,
                    args.worker_image,
                    workers,
                    args.input_price,
                    args.output_price,
                    False,
                )
                resource["metadata"]["annotations"] = {"round": str(round_index)}
                created_runs.append(name)
                record, result_cases = execute_run(
                    kube, reader, resource, args.timeout_seconds
                )
                runs.append(record)
                cases.extend(result_cases)
                if record["cost_usd"] is None:
                    raise BenchmarkError(
                        "capacity cost is unknown; refusing to start another run"
                    )
                spent += Decimal(record["cost_usd"])
        if spent >= SPEND_STOP_USD:
            stopped_for_budget = True
            raise BenchmarkError(
                "USD 8 recorded-cost stop reached before the reliability run"
            )
        reliability_name = prefix + "-reliability"
        resource = run_resource(
            reliability_name,
            args.namespace,
            dataset_name,
            scenario_name,
            secret_name,
            args.worker_image,
            16,
            args.input_price,
            args.output_price,
            True,
        )
        created_runs.append(reliability_name)
        record, result_cases = execute_run(kube, reader, resource, args.timeout_seconds)
        runs.append(record)
        cases.extend(result_cases)
    except BenchmarkError as exc:
        error = str(exc)
        metadata = locals().get("metadata", {})
    finally:
        # Credentials are always removed, even when evidence resources are retained.
        try:
            kube.delete("secret", secret_name)
        except BenchmarkError:
            pass
        if not args.keep_resources:
            for name in created_runs:
                try:
                    kube.delete("agenttestrun", name)
                except BenchmarkError:
                    pass
            for kind, name in (
                ("configmap", dataset_name),
                ("configmap", scenario_name),
            ):
                try:
                    kube.delete(kind, name)
                except BenchmarkError:
                    pass

    complete = (
        error is None
        and len(runs) == 13
        and sum(run["total"] for run in runs if run["kind"] == "capacity") == 384
        and len(cases) == 416
        and capacity_evidence_complete(runs)
        and reliability_evidence_complete(runs, cases)
    )
    if error is None and not complete:
        error = "canonical evidence validation failed"
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    manifest = {
        "schema_version": "agentstorm-benchmark/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "openai-keda",
        "canonical": complete,
        "complete": complete,
        "error": error,
        "stopped_for_budget": stopped_for_budget,
        "spend_stop_usd": str(SPEND_STOP_USD),
        "model": MODEL,
        "worker_image": args.worker_image,
        "source_revision": commit,
        "adapter_entrypoint": "agentstorm_worker.benchmarks.sre:create_adapter",
        "dataset_cases": 32,
        "dataset_sha256": hashlib.sha256(
            (REPO_ROOT / "examples/datasets/sre-incidents.jsonl").read_bytes()
        ).hexdigest(),
        "capacity_orders": CAPACITY_ORDERS,
        "capacity_executions": sum(
            run["total"] for run in runs if run["kind"] == "capacity"
        ),
        "reliability_executions": sum(
            run["total"] for run in runs if run["kind"] == "reliability"
        ),
        "pricing_usd_per_million_tokens": {
            "input": args.input_price,
            "output": args.output_price,
        },
        "run_configuration": {
            "target": {
                "provider": "openai-agents",
                "model": MODEL,
                "reasoning_effort": "none",
                "parallel_tool_calls": True,
                "adapter_entrypoint": "agentstorm_worker.benchmarks.sre:create_adapter",
            },
            "workload": {
                "dataset_cases": 32,
                "parallelism": 32,
                "concurrency_per_worker": 1,
                "iterations": 1,
            },
            "scheduling": {
                "strategy": "keda",
                "worker_gradients": [1, 4, 16, 32],
            },
            "reliability": {"workers": 16, "seed": 42, "scenario": scenario_document()},
        },
        "environment": metadata,
    }
    write_json(args.output / "manifest.json", manifest)
    write_json(args.output / "runs.json", runs)
    write_cases(args.output / "cases.jsonl.gz", cases)
    if runs:
        write_summary(args.output / "summary.csv", runs)
    else:
        (args.output / "summary.csv").write_text("", encoding="utf-8")
    (args.output / "REPORT.md").write_text(
        real_report(manifest, runs), encoding="utf-8"
    )
    scan_secrets(args.output, (api_key, read_token))
    write_checksums(args.output)
    if not complete:
        raise SystemExit(error or "canonical benchmark is incomplete")


if __name__ == "__main__":
    main()
