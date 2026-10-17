#!/usr/bin/env python3
"""Run the fixed SRE protocol on Kubernetes through an OpenAI-compatible endpoint.

The API key is read from OPENAI_API_KEY and installed as a Secret. It is never
written to a manifest, argument list, or artifact. A run is canonical only when
the full matrix, price snapshot, pinned endpoint, and KEDA evidence all agree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from collections.abc import Callable
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "hack/benchmark"))

from run_sre_benchmark import (
    CAPACITY_ORDERS,
    prepare_output,
    write_cases,
    write_checksums,
    write_json,
    write_summary,
)
from run_sre_compatible import (
    ADAPTER,
    DATASET,
    REAL_CASE_TIMEOUT_SECONDS,
    SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
    SRE_COMPATIBLE_TEMPERATURE,
    BenchmarkError,
    _revision,
    assert_case_timeout_covers_calls,
    claim_policy,
    confirm_zero_price,
    limitations,
    render_report,
    require_spend_opt_in,
    reserve_benchmark_spend,
    validate_endpoint,
)
from agentstorm_worker.adapters.openai_compatible import (
    DEFAULT_HTTP_RETRY_ATTEMPTS,
    DEFAULT_HTTP_RETRY_MAX_SECONDS,
    validate_http_retry_attempts,
    validate_http_retry_max_wait_seconds,
    validate_max_output_tokens,
    validate_request_fields,
)
from run_sre_keda import (
    SPEND_STOP_USD,
    IMAGE_DIGEST,
    Kubernetes,
    ResultReader,
    capacity_evidence_complete,
    cluster_metadata,
    config_map,
    execute_run,
    reliability_evidence_complete,
    scan_secrets,
    scenario_document,
)

RUNTIME_CODE = (
    "import importlib.metadata,json,platform\n"
    "versions={'python':platform.python_version(),"
    "'agentstorm_worker':importlib.metadata.version('agentstorm-worker')}\n"
    "try:\n"
    "    versions['openai_agents']=importlib.metadata.version('openai-agents')\n"
    "except importlib.metadata.PackageNotFoundError:\n"
    "    versions['openai_agents']=None\n"
    "print(json.dumps(versions))\n"
)


def compatible_run_resource(
    name: str,
    namespace: str,
    dataset_name: str,
    scenario_name: str,
    api_secret: str,
    worker_image: str,
    workers: int,
    model: str,
    base_url: str,
    input_price: str,
    output_price: str,
    reliability: bool,
    max_tokens: int | None = None,
    request_fields: dict[str, Any] | None = None,
    http_retry_attempts: int | None = None,
    http_retry_max_wait_seconds: int | None = None,
) -> dict[str, Any]:
    target: dict[str, Any] = {
        "provider": "openai-compatible",
        "model": model,
        "baseURL": base_url,
        "adapterEntrypoint": ADAPTER,
        "apiKeySecretRef": {"name": api_secret, "key": "api-key"},
        "pricing": {
            "inputUSDPerMillionTokens": input_price,
            "outputUSDPerMillionTokens": output_price,
        },
    }
    if max_tokens is not None:
        target["maxTokens"] = max_tokens
    if request_fields:
        target["requestFields"] = request_fields
    if http_retry_attempts is not None:
        target["httpRetryAttempts"] = http_retry_attempts
    if http_retry_max_wait_seconds is not None:
        target["httpRetryMaxWaitSeconds"] = http_retry_max_wait_seconds
    spec: dict[str, Any] = {
        "target": target,
        "workload": {
            "datasetRef": {"name": dataset_name, "key": "cases.jsonl"},
            "parallelism": 32,
            "concurrencyPerWorker": 1,
            "iterations": 1,
            "timeoutSeconds": REAL_CASE_TIMEOUT_SECONDS,
        },
        "evaluation": {} if reliability else {"minSuccessRate": 1, "maxErrorRate": 0},
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


def drive_until_budget(
    input_price: str,
    output_price: str,
    start: Callable[[str, int, int, int], dict[str, Any]],
    *,
    max_output_tokens: int = SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    """Start capacity runs, then reliability, until the spend estimate cannot cover the next one.

    ``start(kind, workers, round_index, sequence)`` performs one run. ``kind`` is
    ``capacity`` or ``reliability``. A reserve refusal sets the stop flag. Any
    other benchmark error is returned without that flag.
    """
    spent = Decimal(0)
    runs: list[dict[str, Any]] = []
    stopped = False
    error: str | None = None
    try:
        for round_index, order in enumerate(CAPACITY_ORDERS, start=1):
            for sequence, workers in enumerate(order, start=1):
                try:
                    reserve_benchmark_spend(
                        spent,
                        input_price,
                        output_price,
                        1,
                        max_output_tokens=max_output_tokens,
                    )
                except BenchmarkError:
                    stopped = True
                    raise
                record = start("capacity", workers, round_index, sequence)
                runs.append(record)
                if record["cost_usd"] is None:
                    raise BenchmarkError("capacity cost is unknown; refusing to start another run")
                spent += Decimal(record["cost_usd"])
        try:
            reserve_benchmark_spend(
                spent,
                input_price,
                output_price,
                2,
                max_output_tokens=max_output_tokens,
            )
        except BenchmarkError:
            stopped = True
            raise
        runs.append(start("reliability", 16, 1, 1))
    except BenchmarkError as exc:
        error = str(exc)
    return runs, error, stopped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="agentstorm-system")
    parser.add_argument("--kube-context", default="")
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--result-api-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--endpoint-class", required=True)
    parser.add_argument("--input-price", required=True)
    parser.add_argument("--output-price", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-version", default="")
    parser.add_argument("--inference-server", default="")
    parser.add_argument("--quantization", default="")
    parser.add_argument("--hardware", default="")
    parser.add_argument("--topology", default="")
    parser.add_argument("--timeout-seconds", type=int, default=2400)
    parser.add_argument("--keep-resources", action="store_true")
    parser.add_argument("--max-tokens", type=int, default=0)
    parser.add_argument("--request-fields", default="")
    parser.add_argument("--price-is-zero-confirmed", action="store_true")
    parser.add_argument("--http-retry-attempts", type=int, default=0)
    parser.add_argument("--http-retry-max-wait", type=int, default=0)
    args = parser.parse_args()
    try:
        require_spend_opt_in()
        confirm_zero_price(args.input_price, args.output_price, args.price_is_zero_confirmed)
        max_tokens = validate_max_output_tokens(
            args.max_tokens or SRE_COMPATIBLE_MAX_OUTPUT_TOKENS
        )
        request_fields: dict[str, Any] = {}
        if args.request_fields:
            try:
                parsed_fields = json.loads(args.request_fields)
            except json.JSONDecodeError as exc:
                raise BenchmarkError("--request-fields must be a JSON object") from exc
            request_fields = validate_request_fields(
                parsed_fields, reject_keys={"temperature", "max_tokens"}
            )
        http_retry_attempts = (
            validate_http_retry_attempts(args.http_retry_attempts)
            if args.http_retry_attempts
            else DEFAULT_HTTP_RETRY_ATTEMPTS
        )
        http_retry_max_wait_seconds = (
            validate_http_retry_max_wait_seconds(args.http_retry_max_wait)
            if args.http_retry_max_wait
            else int(DEFAULT_HTTP_RETRY_MAX_SECONDS)
        )
        endpoint = validate_endpoint(
            model=args.model,
            base_url=args.base_url,
            endpoint_class=args.endpoint_class,
            input_price=args.input_price,
            output_price=args.output_price,
            model_version=args.model_version,
            inference_server=args.inference_server,
            quantization=args.quantization,
            hardware=args.hardware,
            topology=args.topology,
            reject_loopback=True,
        )
    except BenchmarkError as exc:
        raise SystemExit(str(exc)) from exc
    if not IMAGE_DIGEST.fullmatch(args.worker_image):
        raise SystemExit("--worker-image must be an immutable sha256 digest reference")
    assert_case_timeout_covers_calls(REAL_CASE_TIMEOUT_SECONDS, 2)
    if not REAL_CASE_TIMEOUT_SECONDS < args.timeout_seconds <= 7200:
        raise SystemExit(
            "--timeout-seconds must be greater than the case timeout "
            f"({REAL_CASE_TIMEOUT_SECONDS}s) and at most 7200"
        )
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    read_token = os.environ.get("AGENTSTORM_RESULT_READ_TOKEN", "")
    if not api_key or not read_token:
        raise SystemExit("OPENAI_API_KEY and AGENTSTORM_RESULT_READ_TOKEN are required")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    prefix = f"sreoc{timestamp}"
    dataset_name = prefix + "-dataset"
    scenario_name = prefix + "-scenario"
    secret_name = prefix + "-provider"
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
    metadata: dict[str, Any] = {}
    try:
        metadata = cluster_metadata(
            kube, args.worker_image, prefix, runtime_code=RUNTIME_CODE
        )
        kube.apply(
            config_map(dataset_name, args.namespace, "cases.jsonl", DATASET.read_text(encoding="utf-8"))
        )
        kube.apply(
            config_map(
                scenario_name,
                args.namespace,
                "scenario.json",
                json.dumps(scenario_document(), sort_keys=True, separators=(",", ":")),
            )
        )
        kube.install_api_key(secret_name, api_key)

        def start(kind: str, workers: int, round_index: int, sequence: int) -> dict[str, Any]:
            reliability = kind == "reliability"
            name = (
                prefix + "-reliability"
                if reliability
                else f"{prefix}-r{round_index}s{sequence}w{workers}"
            )
            resource = compatible_run_resource(
                name,
                args.namespace,
                dataset_name,
                scenario_name,
                secret_name,
                args.worker_image,
                workers,
                args.model,
                args.base_url,
                args.input_price,
                args.output_price,
                reliability,
                max_tokens=max_tokens,
                request_fields=request_fields or None,
                http_retry_attempts=http_retry_attempts,
                http_retry_max_wait_seconds=http_retry_max_wait_seconds,
            )
            if not reliability:
                resource["metadata"]["annotations"] = {"round": str(round_index)}
            created_runs.append(name)
            record, result_cases = execute_run(kube, reader, resource, args.timeout_seconds)
            cases.extend(result_cases)
            return record

        runs, error, stopped_for_budget = drive_until_budget(
            args.input_price,
            args.output_price,
            start,
            max_output_tokens=max_tokens,
        )
    except BenchmarkError as exc:
        error = str(exc)
    finally:
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
            for name in (dataset_name, scenario_name):
                try:
                    kube.delete("configmap", name)
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
    manifest = {
        "schema_version": "agentstorm-benchmark/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "openai-compatible-keda",
        "canonical": complete,
        "complete": complete,
        "error": error,
        "stopped_for_budget": stopped_for_budget,
        "spend_stop_usd": str(SPEND_STOP_USD),
        "price_is_zero_confirmed": args.price_is_zero_confirmed
        and Decimal(args.input_price) == 0
        and Decimal(args.output_price) == 0,
        "spend_guard": (
            "confirmed-zero-price"
            if args.price_is_zero_confirmed
            and Decimal(args.input_price) == 0
            and Decimal(args.output_price) == 0
            else "recorded-cost-stop"
        ),
        "provider": "openai-compatible",
        "protocol": "openai-chat-completions",
        "model": args.model,
        "endpoint": endpoint,
        "worker_image": args.worker_image,
        "source_revision": _revision(),
        "adapter_entrypoint": ADAPTER,
        "dataset_cases": 32,
        "dataset_sha256": hashlib.sha256(DATASET.read_bytes()).hexdigest(),
        "capacity_orders": CAPACITY_ORDERS,
        "capacity_executions": sum(run["total"] for run in runs if run["kind"] == "capacity"),
        "reliability_executions": sum(
            run["total"] for run in runs if run["kind"] == "reliability"
        ),
        "pricing_usd_per_million_tokens": {
            "input": args.input_price,
            "output": args.output_price,
        },
        "run_configuration": {
            "target": {
                "provider": "openai-compatible",
                "model": args.model,
                "base_url": args.base_url,
                "temperature": SRE_COMPATIBLE_TEMPERATURE,
                "max_tokens": max_tokens,
                "request_fields": request_fields,
                "http_retry_attempts": http_retry_attempts,
                "http_retry_max_wait_seconds": http_retry_max_wait_seconds,
                "stream": False,
                "adapter_entrypoint": ADAPTER,
            },
            "scheduling": {"strategy": "keda", "worker_gradients": [1, 4, 16, 32]},
            "reliability": {"workers": 16, "seed": 42, "scenario": scenario_document()},
        },
        "environment": metadata,
        "claims": claim_policy(
            "openai-compatible-keda", canonical=complete, complete=complete
        ),
        "limitations": limitations("openai-compatible-keda", endpoint),
    }
    write_json(args.output / "manifest.json", manifest)
    write_json(args.output / "runs.json", runs)
    write_cases(args.output / "cases.jsonl.gz", cases)
    if runs:
        write_summary(args.output / "summary.csv", runs)
    else:
        (args.output / "summary.csv").write_text("", encoding="utf-8")
    (args.output / "REPORT.md").write_text(render_report(manifest, runs), encoding="utf-8")
    scan_secrets(args.output, (api_key, read_token))
    write_checksums(args.output)
    if not complete:
        raise SystemExit(error or "canonical benchmark is incomplete")


if __name__ == "__main__":
    main()
