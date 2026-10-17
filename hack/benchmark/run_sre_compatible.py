#!/usr/bin/env python3
"""Run the fixed SRE protocol through the OpenAI-compatible provider.

Mock mode uses a local scripted server and never reads a provider credential.
Local mode calls a real endpoint in-process. It records duration and token cost
for that process, and it is not a Kubernetes scaling result.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "worker/src"))
sys.path.insert(0, str(REPO_ROOT / "hack/benchmark"))

from agentstorm_worker.adapters import AdapterFactoryContext
from agentstorm_worker.adapters.openai_compatible import validate_base_url
from agentstorm_worker.benchmarks.sre import (
    SRE_COMPATIBLE_INPUT_TOKEN_ALLOWANCE,
    SRE_COMPATIBLE_MAX_MODEL_CALLS,
    SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
    SRE_COMPATIBLE_REQUEST_TIMEOUT_SECONDS,
    SRE_COMPATIBLE_TEMPERATURE,
    create_adapter,
)
from agentstorm_worker.adapters.openai_compatible import validate_max_output_tokens, validate_request_fields
from agentstorm_worker.config import (
    PricingConfig,
    ReliabilityConfig,
    RetryConfig,
    load_dataset,
    load_run_config,
)
from agentstorm_worker.runner import WorkloadRunner
from openai_compatible_mock import serve
from run_sre_benchmark import (
    CAPACITY_ORDERS,
    prepare_output,
    scenario,
    without_fake_case_latency,
    without_fake_performance,
    write_cases,
    write_checksums,
    write_json,
    write_summary,
)
from run_sre_benchmark import (
    case_record as benchmark_case_record,
)
from run_sre_benchmark import (
    run_record as benchmark_run_record,
)
from run_sre_keda import SPEND_STOP_USD, reliability_evidence_complete, scan_secrets

MOCK_MODEL = "mock-sre-protocol"
ADAPTER = "agentstorm_worker.benchmarks.sre:create_adapter"
DATASET = REPO_ROOT / "examples/datasets/sre-incidents.jsonl"
DATASET_CASES = 32
# One incident from each root-cause family. The dataset groups eight cases per family.
SMOKE_CASE_IDS = ("INC-001", "INC-009", "INC-017", "INC-025")
ENDPOINT_CLASSES = ("domestic-cloud", "hosted-api", "self-hosted")
UNPINNED_MODELS = {"latest", "auto", "default"}
PRICE = re.compile(r"^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$")
_FIELD = re.compile(r"^[^\r\n]{1,512}$")
# urllib's timeout applies to each socket operation, not the whole HTTP request.
# The case timeout is still set above two reliability attempts times eight calls
# times that socket timeout, plus a margin. One request can outlast a single
# 120 second interval when more than one socket operation uses the timeout.
REAL_CASE_TIMEOUT_SECONDS = (
    2 * SRE_COMPATIBLE_MAX_MODEL_CALLS * SRE_COMPATIBLE_REQUEST_TIMEOUT_SECONDS + 180
)


class BenchmarkError(RuntimeError):
    pass


def require_spend_opt_in() -> None:
    if os.environ.get("AGENTSTORM_ALLOW_PROVIDER_SPEND") != "1":
        raise BenchmarkError(
            "AGENTSTORM_ALLOW_PROVIDER_SPEND=1 is required before a real endpoint call"
        )


def validate_endpoint(
    *,
    model: str,
    base_url: str,
    endpoint_class: str,
    input_price: str,
    output_price: str,
    model_version: str = "",
    inference_server: str = "",
    quantization: str = "",
    hardware: str = "",
    topology: str = "",
    reject_loopback: bool = False,
) -> dict[str, Any]:
    """Return pinned endpoint metadata or reject an unpublishable configuration."""
    if endpoint_class not in ENDPOINT_CLASSES:
        raise BenchmarkError(
            "endpoint class must be domestic-cloud, hosted-api, or self-hosted"
        )
    if not model or model.casefold() in UNPINNED_MODELS or any(character.isspace() for character in model):
        raise BenchmarkError("model must be an exact pinned id, not latest, auto, or default")
    try:
        validate_base_url(base_url)
    except ValueError as exc:
        raise BenchmarkError(str(exc)) from exc
    parsed = urlsplit(base_url)
    host = parsed.hostname or ""
    if endpoint_class in {"domestic-cloud", "hosted-api"} and parsed.scheme != "https":
        raise BenchmarkError(f"{endpoint_class} endpoints must use https")
    if reject_loopback and _kubernetes_unreachable(host):
        raise BenchmarkError(
            "Kubernetes Workers cannot use a loopback or unspecified endpoint"
        )
    if not PRICE.fullmatch(input_price) or not PRICE.fullmatch(output_price):
        raise BenchmarkError("prices must be non-negative decimal strings")
    version = model_version or model
    if version.casefold() in UNPINNED_MODELS or any(character.isspace() for character in version):
        raise BenchmarkError("model version must be pinned")
    endpoint: dict[str, Any] = {
        "class": endpoint_class,
        "protocol": "openai-chat-completions",
        "base_url": base_url,
        "host": host,
        "model": model,
        "model_version": version,
        "price_snapshot_required": True,
    }
    if endpoint_class == "self-hosted":
        attested = {
            "inference_server": inference_server,
            "quantization": quantization,
            "hardware": hardware,
            "topology": topology,
        }
        for name, value in attested.items():
            if not _FIELD.fullmatch(value) or "Bearer " in value:
                raise BenchmarkError(f"self-hosted {name} must be operator-attested metadata")
        endpoint.update(attested)
        endpoint["attestation"] = "operator-supplied; AgentStorm does not probe GPUs"
    return endpoint


def _kubernetes_unreachable(host: str) -> bool:
    """Reject hosts a Worker pod cannot use as a remote inference endpoint."""
    normalized = host.strip().rstrip(".").casefold()
    if normalized == "localhost":
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    # Before Python 3.13, IPv6Address.is_loopback is only ::1. An IPv4-mapped
    # address such as ::ffff:127.0.0.1 is loopback only through ipv4_mapped.
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        address = mapped
    return address.is_loopback or address.is_unspecified


def input_token_allowance(max_output_tokens: int = SRE_COMPATIBLE_MAX_OUTPUT_TOKENS) -> int:
    """Input estimate for one call, including assistant history resent at ``max_tokens``."""
    history = (SRE_COMPATIBLE_MAX_MODEL_CALLS - 1) * max_output_tokens
    return max(SRE_COMPATIBLE_INPUT_TOKEN_ALLOWANCE, history)


def worst_case_run_usd(
    input_price: str,
    output_price: str,
    attempts: int,
    *,
    cases: int = DATASET_CASES,
    max_output_tokens: int = SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
) -> Decimal:
    """Spend estimate for one run before it is allowed to start.

    Output tokens follow the configured ``max_tokens`` limit. Input tokens use
    the larger of ``SRE_COMPATIBLE_INPUT_TOKEN_ALLOWANCE`` and the assistant
    history those calls can resend at that limit. The endpoint does not enforce
    either number, so this is an estimate rather than a cap on what a provider
    can bill. A confirmed price of zero makes the estimate zero and disables
    the recorded-cost stop; callers must reject that price unless the operator
    confirmed it.
    """
    calls = Decimal(cases * SRE_COMPATIBLE_MAX_MODEL_CALLS * attempts)
    million = Decimal(1_000_000)
    input_tokens = calls * Decimal(input_token_allowance(max_output_tokens))
    output_tokens = calls * Decimal(max_output_tokens)
    return (input_tokens * Decimal(input_price) + output_tokens * Decimal(output_price)) / million


def confirm_zero_price(input_price: str, output_price: str, confirmed: bool) -> bool:
    """Reject an unconfirmed zero price, which would disable the spend stop."""
    zero = Decimal(input_price) == 0 and Decimal(output_price) == 0
    if zero and not confirmed:
        raise BenchmarkError(
            "a price of 0 disables the USD 8 recorded-cost stop; "
            "pass --price-is-zero-confirmed after checking the provider bill"
        )
    return zero and confirmed


def reserve_benchmark_spend(
    spent: Decimal,
    input_price: str,
    output_price: str,
    attempts: int,
    *,
    cases: int = DATASET_CASES,
    max_output_tokens: int = SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
) -> None:
    """Refuse to start a run that could finish above the recorded-cost stop."""
    estimate = worst_case_run_usd(
        input_price,
        output_price,
        attempts,
        cases=cases,
        max_output_tokens=max_output_tokens,
    )
    if spent + estimate > SPEND_STOP_USD:
        raise BenchmarkError(
            "USD 8 recorded-cost stop: spent "
            f"{spent} plus estimate {estimate} exceeds the cap"
        )


def assert_case_timeout_covers_calls(timeout_seconds: int, attempts: int) -> None:
    needed = attempts * SRE_COMPATIBLE_MAX_MODEL_CALLS * SRE_COMPATIBLE_REQUEST_TIMEOUT_SECONDS
    if needed >= timeout_seconds:
        raise BenchmarkError(
            "case timeout must exceed max attempts times model calls times the per-socket timeout"
        )


def claim_policy(mode: str, *, canonical: bool, complete: bool) -> dict[str, Any]:
    if mode == "openai-compatible-mock":
        return {
            "performance": False,
            "kubernetes_scaling": False,
            "allowed": [
                "OpenAI-compatible chat completions tool loop",
                "fixed SRE tool-path assertions",
                "price-snapshot cost plumbing",
                "artifact checksums",
            ],
        }
    if mode == "openai-compatible-mock-smoke":
        return {
            "performance": False,
            "kubernetes_scaling": False,
            "allowed": ["one no-cost pass of the 32 SRE incidents through the compatible adapter"],
        }
    if mode == "openai-compatible-local-smoke":
        return {
            "performance": False,
            "kubernetes_scaling": False,
            "allowed": [
                "a non-publishable subset of the SRE incidents against one pinned endpoint"
            ],
        }
    if mode == "openai-compatible-local" and complete:
        return {
            "performance": True,
            "kubernetes_scaling": False,
            "allowed": [
                "in-process duration and token cost for this pinned endpoint and price snapshot"
            ],
        }
    if mode == "openai-compatible-keda" and canonical:
        return {
            "performance": True,
            "kubernetes_scaling": True,
            "allowed": [
                "fixed-protocol Kubernetes measurements for this pinned model, endpoint, price snapshot, dataset, and recorded cluster"
            ],
        }
    return {
        "performance": False,
        "kubernetes_scaling": False,
        "allowed": ["no performance or scaling claim; the fixed protocol did not finish cleanly"],
    }


def limitations(mode: str, endpoint: dict[str, Any] | None = None) -> list[str]:
    if mode.startswith("openai-compatible-mock"):
        return [
            "Mock mode validates the chat/tool protocol, SRE tool chain, price-snapshot plumbing, and checksums.",
            "It is not a domestic-cloud, self-hosted, or Kubernetes measurement.",
            "Timing, throughput, token-cost, scaling, and utilization fields are omitted.",
        ]
    if mode == "openai-compatible-local":
        return [
            "Local mode measures one in-process Worker against the pinned endpoint.",
            "It does not measure Kubernetes scheduling or KEDA scale events.",
            "A publishable scaling claim requires the Kubernetes runner and canonical=true.",
        ]
    if mode == "openai-compatible-local-smoke":
        return [
            "Smoke mode runs a subset at a capped concurrency and is not publishable.",
            "It does not measure Kubernetes scheduling, KEDA scale events, or the fixed 416-execution protocol.",
            "A zero price is accepted only when --price-is-zero-confirmed is set, and that confirmation disables the USD 8 stop.",
        ]
    notes = [
        "A publishable result requires the full 384-capacity plus 32-reliability protocol.",
        "Partial matrices, budget stops, and changed dataset or model ids are not publishable.",
        "Evidence applies only to the recorded model, endpoint, price snapshot, and cluster.",
    ]
    if endpoint and endpoint.get("class") == "self-hosted":
        notes.append(
            "Hardware, quantization, inference server, and topology are operator-attested, not probed."
        )
    notes.append("SSH is transport for a Worker-reachable HTTP endpoint and is not an adapter.")
    return notes


async def execute_mock(output_dir: Path, shape: str, image_digest: str = "") -> None:
    if shape not in {"full", "smoke"}:
        raise BenchmarkError("mock shape must be full or smoke")
    held_key = os.environ.pop("OPENAI_API_KEY", None)
    try:
        with serve() as base_url:
            await _execute_mock(output_dir, shape, image_digest, base_url, held_key)
    finally:
        if held_key is not None:
            os.environ["OPENAI_API_KEY"] = held_key


async def _execute_mock(
    output_dir: Path,
    shape: str,
    image_digest: str,
    base_url: str,
    held_key: str | None,
) -> None:
    cases = load_dataset(DATASET)
    base = load_run_config(REPO_ROOT / "examples/run.sre.local.json")
    adapter = create_adapter(
        AdapterFactoryContext(
            provider="openai-compatible", model=MOCK_MODEL, base_url=base_url
        )
    )
    prepare_output(output_dir)
    runs: list[dict[str, Any]] = []
    case_records: list[dict[str, Any]] = []
    if shape == "smoke":
        await _append_run(
            runs,
            case_records,
            base,
            adapter,
            cases,
            "mock-smoke",
            "capacity",
            8,
            1,
            None,
        )
    else:
        for round_index, order in enumerate(CAPACITY_ORDERS, start=1):
            for sequence, workers in enumerate(order, start=1):
                await _append_run(
                    runs,
                    case_records,
                    base,
                    adapter,
                    cases,
                    f"mock-capacity-r{round_index}-s{sequence}-w{workers}",
                    "capacity",
                    workers,
                    round_index,
                    base_url,
                )
        await _append_run(
            runs,
            case_records,
            base,
            adapter,
            cases,
            "mock-reliability-w16",
            "reliability",
            16,
            1,
            base_url,
            reliability=True,
        )
    _assert_mock_accounting(runs, case_records, shape)
    runs = [without_fake_performance(item) for item in runs]
    case_records = [without_fake_case_latency(item) for item in case_records]
    mode = "openai-compatible-mock" if shape == "full" else "openai-compatible-mock-smoke"
    protocol_complete = shape == "full" and len(case_records) == 416
    manifest = _manifest(
        mode=mode,
        complete=protocol_complete,
        canonical=False,
        model=MOCK_MODEL,
        endpoint={
            "class": "mock",
            "protocol": "openai-chat-completions",
            "base_url": base_url,
            "host": urlsplit(base_url).hostname,
            "model": MOCK_MODEL,
            "model_version": MOCK_MODEL,
        },
        input_price="1",
        output_price="2",
        runs=runs,
        case_count=len(case_records),
        image_digest=image_digest,
        error=None,
        stopped_for_budget=False,
    )
    _write_artifacts(output_dir, manifest, runs, case_records, (held_key,))
    if shape == "full" and not protocol_complete:
        raise BenchmarkError("mock benchmark did not complete the fixed execution shape")
    if shape == "smoke" and runs[0]["succeeded"] != 32:
        raise BenchmarkError("mock smoke did not pass all 32 incidents")


def select_local_cases(cases: list[Any], shape: str, case_limit: int | None) -> list[Any]:
    """Return the fixed dataset, or a non-publishable local subset."""
    if shape == "full":
        if case_limit is not None:
            raise BenchmarkError("--cases is only valid with --shape smoke")
        return list(cases)
    if shape != "smoke":
        raise BenchmarkError("local shape must be full or smoke")
    if case_limit is not None:
        if not 1 <= case_limit <= len(cases):
            raise BenchmarkError("--cases must be between 1 and the dataset size")
        return list(cases[:case_limit])
    selected = [case for case in cases if case.case_id in SMOKE_CASE_IDS]
    if [case.case_id for case in selected] != list(SMOKE_CASE_IDS):
        raise BenchmarkError("smoke cases are missing from the dataset")
    return selected


def resolve_concurrency(requested: int, shape: str, cap: int | None) -> int:
    """Cap workers. Smoke defaults to one worker so a free tier can be called safely."""
    if shape == "smoke" and cap is None:
        cap = 1
    if cap is None:
        return requested
    if not 1 <= cap <= 32:
        raise BenchmarkError("--max-concurrency must be between 1 and 32")
    return min(requested, cap)


def local_matrix_publishable(
    shape: str, case_limit: int | None, max_concurrency: int | None
) -> bool:
    """The fixed 416-execution matrix is the only publishable local protocol."""
    return (
        shape == "full"
        and case_limit is None
        and (max_concurrency is None or max_concurrency >= 32)
    )


async def execute_local(
    output_dir: Path,
    endpoint: dict[str, Any],
    image_digest: str = "",
    *,
    shape: str = "full",
    case_limit: int | None = None,
    max_concurrency: int | None = None,
    max_output_tokens: int = SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
    request_fields: dict[str, Any] | None = None,
    price_is_zero_confirmed: bool = False,
) -> None:
    require_spend_opt_in()
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise BenchmarkError("OPENAI_API_KEY is required")
    fields = validate_request_fields(
        request_fields or {}, reject_keys={"temperature", "max_tokens"}
    )
    max_output_tokens = validate_max_output_tokens(max_output_tokens)
    prices = endpoint["pricing"]
    zero_confirmed = confirm_zero_price(
        str(prices["input"]), str(prices["output"]), price_is_zero_confirmed
    )
    loaded = load_dataset(DATASET)
    cases = select_local_cases(loaded, shape, case_limit)
    publishable = local_matrix_publishable(shape, case_limit, max_concurrency)
    base = load_run_config(REPO_ROOT / "examples/run.sre.local.json")
    adapter = create_adapter(
        AdapterFactoryContext(
            provider="openai-compatible",
            model=str(endpoint["model"]),
            base_url=str(endpoint["base_url"]),
            max_output_tokens=max_output_tokens,
            request_fields=fields,
        )
    )
    prepare_output(output_dir)
    runs: list[dict[str, Any]] = []
    case_records: list[dict[str, Any]] = []
    error: str | None = None
    stopped = False
    spent = Decimal(0)
    assert_case_timeout_covers_calls(REAL_CASE_TIMEOUT_SECONDS, 2)
    try:
        if shape == "smoke":
            workers = resolve_concurrency(1, shape, max_concurrency)
            reserve_benchmark_spend(
                spent,
                prices["input"],
                prices["output"],
                1,
                cases=len(cases),
                max_output_tokens=max_output_tokens,
            )
            await _append_run(
                runs,
                case_records,
                base,
                adapter,
                cases,
                f"local-smoke-w{workers}",
                "capacity",
                workers,
                1,
                str(endpoint["base_url"]),
                endpoint=endpoint,
            )
        else:
            for round_index, order in enumerate(CAPACITY_ORDERS, start=1):
                for sequence, requested in enumerate(order, start=1):
                    workers = resolve_concurrency(requested, shape, max_concurrency)
                    try:
                        reserve_benchmark_spend(
                            spent,
                            prices["input"],
                            prices["output"],
                            1,
                            cases=len(cases),
                            max_output_tokens=max_output_tokens,
                        )
                    except BenchmarkError:
                        stopped = True
                        raise
                    record = await _append_run(
                        runs,
                        case_records,
                        base,
                        adapter,
                        cases,
                        f"local-capacity-r{round_index}-s{sequence}-w{workers}",
                        "capacity",
                        workers,
                        round_index,
                        str(endpoint["base_url"]),
                        endpoint=endpoint,
                    )
                    if record["cost_usd"] is None:
                        raise BenchmarkError(
                            "capacity cost is unknown; refusing to start another run"
                        )
                    spent += Decimal(record["cost_usd"])
            try:
                reserve_benchmark_spend(
                    spent,
                    prices["input"],
                    prices["output"],
                    2,
                    cases=len(cases),
                    max_output_tokens=max_output_tokens,
                )
            except BenchmarkError:
                stopped = True
                raise
            await _append_run(
                runs,
                case_records,
                base,
                adapter,
                cases,
                "local-reliability-w16",
                "reliability",
                resolve_concurrency(16, shape, max_concurrency),
                1,
                str(endpoint["base_url"]),
                endpoint=endpoint,
                reliability=True,
            )
    except BenchmarkError as exc:
        error = str(exc)
    complete = (
        publishable and error is None and len(runs) == 13 and len(case_records) == 416
    )
    mode = "openai-compatible-local-smoke" if shape == "smoke" else "openai-compatible-local"
    manifest = _manifest(
        mode=mode,
        complete=complete,
        canonical=False,
        model=str(endpoint["model"]),
        endpoint=endpoint,
        input_price=str(endpoint["pricing"]["input"]),
        output_price=str(endpoint["pricing"]["output"]),
        runs=runs,
        case_count=len(case_records),
        image_digest=image_digest,
        error=error,
        stopped_for_budget=stopped,
        max_output_tokens=max_output_tokens,
        request_fields=fields,
        price_is_zero_confirmed=zero_confirmed,
    )
    if shape == "full" and not publishable:
        manifest["limitations"] = [
            *manifest["limitations"],
            "max concurrency is below 32, so this is not the fixed protocol and is not publishable.",
        ]
        manifest["claims"] = claim_policy(mode, canonical=False, complete=False)
    _write_artifacts(output_dir, manifest, runs, case_records, (api_key,))
    if publishable and not complete:
        raise BenchmarkError(error or "local compatible benchmark is incomplete")
    if error is not None:
        raise BenchmarkError(error)


async def _append_run(
    runs: list[dict[str, Any]],
    case_records: list[dict[str, Any]],
    base: Any,
    adapter: Any,
    cases: list[Any],
    run_id: str,
    kind: str,
    workers: int,
    round_index: int,
    base_url: str | None,
    endpoint: dict[str, Any] | None = None,
    reliability: bool = False,
) -> dict[str, Any]:
    del base_url
    pricing = None
    if endpoint is not None:
        pricing = PricingConfig(endpoint["pricing"]["input"], endpoint["pricing"]["output"])
    else:
        pricing = PricingConfig("1", "2")
    model = endpoint["model"] if endpoint is not None else MOCK_MODEL
    config = replace(
        base,
        run_id=run_id,
        target=replace(
            base.target,
            provider="openai-compatible",
            model=str(model),
            base_url=str(endpoint["base_url"] if endpoint is not None else ""),
            pricing=pricing,
        ),
        workload=replace(
            base.workload,
            concurrency=workers,
            iterations=1,
            timeout_seconds=REAL_CASE_TIMEOUT_SECONDS if endpoint is not None else 120,
        ),
        reliability=(
            ReliabilityConfig(
                seed=42,
                retry=RetryConfig(max_attempts=2, jitter_ratio=0),
                scenario=scenario(),
            )
            if reliability
            else None
        ),
        evaluation=replace(base.evaluation, max_p95_latency_ms=None),
    )
    if endpoint is None:
        # The mock adapter was constructed with the live base URL. Keep that URL
        # on the run config so traces and summaries name the same endpoint.
        config = replace(
            config,
            target=replace(config.target, base_url=_mock_base_url(adapter)),
        )
    results, summary = await WorkloadRunner(config, adapter).execute(cases)
    record = benchmark_run_record(run_id, kind, workers, round_index, results, summary)
    runs.append(record)
    case_records.extend(benchmark_case_record(run_id, item) for item in results)
    return record


def _mock_base_url(adapter: Any) -> str:
    inner = getattr(adapter, "_adapter", None)
    url = getattr(inner, "_url", "")
    if url.endswith("/chat/completions"):
        return url[: -len("/chat/completions")]
    return str(url)


def _assert_mock_accounting(
    runs: list[dict[str, Any]], case_records: list[dict[str, Any]], shape: str
) -> None:
    capacity = [run for run in runs if run["kind"] == "capacity"]
    if any(run["cost_usd"] is None or run["tool_call_count"] != 128 for run in capacity):
        raise BenchmarkError("mock price snapshot or tool accounting is incomplete")
    if any(run["model_call_count"] in (None, 0) for run in capacity):
        raise BenchmarkError("mock model-call accounting is incomplete")
    if shape == "full" and not reliability_evidence_complete(runs, case_records):
        raise BenchmarkError("mock reliability run did not match the fixed fault protocol")


def _manifest(
    *,
    mode: str,
    complete: bool,
    canonical: bool,
    model: str,
    endpoint: dict[str, Any],
    input_price: str,
    output_price: str,
    runs: list[dict[str, Any]],
    case_count: int,
    image_digest: str,
    error: str | None,
    stopped_for_budget: bool,
    max_output_tokens: int = SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
    request_fields: dict[str, Any] | None = None,
    price_is_zero_confirmed: bool = False,
) -> dict[str, Any]:
    endpoint = dict(endpoint)
    endpoint.pop("pricing", None)
    return {
        "schema_version": "agentstorm-benchmark/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "canonical": canonical,
        "complete": complete,
        "error": error,
        "stopped_for_budget": stopped_for_budget,
        "spend_stop_usd": str(SPEND_STOP_USD),
        "price_is_zero_confirmed": price_is_zero_confirmed,
        "spend_guard": (
            "confirmed-zero-price" if price_is_zero_confirmed else "recorded-cost-stop"
        ),
        "provider": "openai-compatible",
        "protocol": "openai-chat-completions",
        "model": model,
        "endpoint": endpoint,
        "adapter_entrypoint": ADAPTER,
        "dataset_cases": 32,
        "dataset_sha256": hashlib.sha256(DATASET.read_bytes()).hexdigest(),
        "capacity_orders": [] if mode.endswith("-smoke") else CAPACITY_ORDERS,
        "capacity_executions": sum(run["total"] for run in runs if run["kind"] == "capacity"),
        "reliability_executions": sum(run["total"] for run in runs if run["kind"] == "reliability"),
        "recorded_cases": case_count,
        "pricing_usd_per_million_tokens": {"input": input_price, "output": output_price},
        "run_configuration": {
            "target": {
                "provider": "openai-compatible",
                "model": model,
                "temperature": SRE_COMPATIBLE_TEMPERATURE,
                "max_tokens": max_output_tokens,
                "request_fields": request_fields or {},
                "stream": False,
                "adapter_entrypoint": ADAPTER,
            }
        },
        "worker_image_digest": image_digest or None,
        "source_revision": _revision(),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
        },
        "claims": claim_policy(mode, canonical=canonical, complete=complete),
        "limitations": limitations(mode, endpoint),
    }


def _revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout.strip()


def _write_artifacts(
    output_dir: Path,
    manifest: dict[str, Any],
    runs: list[dict[str, Any]],
    case_records: list[dict[str, Any]],
    secrets: tuple[str | None, ...],
) -> None:
    write_json(output_dir / "manifest.json", manifest)
    write_json(output_dir / "runs.json", runs)
    write_cases(output_dir / "cases.jsonl.gz", case_records)
    if runs:
        write_summary(output_dir / "summary.csv", runs)
    else:
        (output_dir / "summary.csv").write_text("", encoding="utf-8")
    (output_dir / "REPORT.md").write_text(render_report(manifest, runs), encoding="utf-8")
    scan_secrets(output_dir, tuple(value for value in secrets if value))
    write_checksums(output_dir)


def render_report(manifest: dict[str, Any], runs: list[dict[str, Any]]) -> str:
    mode = str(manifest["mode"])
    if mode == "openai-compatible-local" and manifest.get("complete"):
        return _local_report(manifest, runs)
    if mode == "openai-compatible-keda" and manifest.get("canonical"):
        return _canonical_report(manifest, runs)
    return _non_claim_report(manifest)


def _non_claim_report(manifest: dict[str, Any]) -> str:
    lines = [
        "# AgentStorm SRE OpenAI-compatible benchmark",
        "",
        "> This artifact is not publishable performance evidence.",
        "",
        f"Mode: `{manifest['mode']}`. Fixed protocol complete: **{manifest['complete']}**. Canonical: **{manifest['canonical']}**.",
        "",
        "## What this evidence supports",
        "",
    ]
    lines.extend(f"- {item}" for item in manifest.get("claims", {}).get("allowed", []))
    lines.extend(["", "## Limits", ""])
    lines.extend(f"- {item}" for item in manifest.get("limitations", []))
    if manifest.get("error"):
        lines.extend(["", f"Run stopped: {manifest['error']}"])
    lines.append("")
    return "\n".join(lines)


def _local_report(manifest: dict[str, Any], runs: list[dict[str, Any]]) -> str:
    lines = [
        "# AgentStorm SRE OpenAI-compatible in-process run",
        "",
        "Figures below are in-process measurements for the pinned endpoint and price snapshot.",
        "",
        f"Model: `{manifest['model']}`; endpoint host: `{manifest['endpoint']['host']}`.",
        "",
        "| Workers | Mean duration (s) | Succeeded | Model calls | Tool calls | Recorded cost |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for run in runs:
        duration = float(run["duration_ms"]) / 1000
        lines.append(
            f"| {run['max_workers']} | {duration:.3f} | {run['succeeded']}/{run['total']} | "
            f"{run['model_call_count']} | {run['tool_call_count']} | {run['cost_usd']} |"
        )
    lines.extend(["", "## Limits", ""])
    lines.extend(f"- {item}" for item in manifest["limitations"])
    lines.append("")
    return "\n".join(lines)


def _canonical_report(manifest: dict[str, Any], runs: list[dict[str, Any]]) -> str:
    lines = [
        "# AgentStorm SRE OpenAI-compatible canonical benchmark",
        "",
        "Claims are limited to this pinned model, endpoint, price snapshot, dataset checksum, and recorded cluster.",
        "",
        f"Model: `{manifest['model']}`; host: `{manifest['endpoint']['host']}`; canonical: **true**.",
        "",
        "| Workers | Mean duration (s) | Agents/s | P50/P95/P99 ms | Success | Speedup | Efficiency | Mean cost USD |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    capacity = [run for run in runs if run["kind"] == "capacity"]
    grouped: dict[int, list[dict[str, Any]]] = {}
    for run in capacity:
        grouped.setdefault(int(run["max_workers"]), []).append(run)
    baseline = (
        sum(float(item["duration_ms"]) for item in grouped.get(1, [])) / len(grouped[1])
        if grouped.get(1)
        else 0
    )
    for workers in (1, 4, 16, 32):
        entries = grouped.get(workers, [])
        if not entries:
            continue
        duration = sum(float(item["duration_ms"]) for item in entries) / len(entries)
        throughput = sum(float(item["throughput_agents_per_second"]) for item in entries) / len(entries)
        speedup = baseline / duration if baseline and duration else 0
        lines.append(
            f"| {workers} | {duration / 1000:.3f} | {throughput:.3f} | "
            f"{_mean(entries, 'p50_ms'):.1f}/{_mean(entries, 'p95_ms'):.1f}/{_mean(entries, 'p99_ms'):.1f} | "
            f"{sum(item['succeeded'] for item in entries)}/{sum(item['total'] for item in entries)} | "
            f"{speedup:.3f} | {speedup / workers:.3f} | {_mean(entries, 'cost_usd'):.6f} |"
        )
    lines.extend(["", "## Limits", ""])
    lines.extend(f"- {item}" for item in manifest["limitations"])
    lines.append("")
    return "\n".join(lines)


def _mean(entries: list[dict[str, Any]], field: str) -> float:
    values = [float(item[field]) for item in entries if item.get(field) is not None]
    return sum(values) / len(values) if values else float("nan")


def _with_pricing(endpoint: dict[str, Any], input_price: str, output_price: str) -> dict[str, Any]:
    priced = dict(endpoint)
    priced["pricing"] = {"input": input_price, "output": output_price}
    return priced


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("mock", "local"), required=True)
    parser.add_argument("--shape", choices=("full", "smoke"), default="full")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image-digest", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--endpoint-class", default="")
    parser.add_argument("--input-price", default="")
    parser.add_argument("--output-price", default="")
    parser.add_argument("--model-version", default="")
    parser.add_argument("--inference-server", default="")
    parser.add_argument("--quantization", default="")
    parser.add_argument("--hardware", default="")
    parser.add_argument("--topology", default="")
    parser.add_argument("--cases", type=int, default=0)
    parser.add_argument("--max-concurrency", type=int, default=0)
    parser.add_argument("--max-tokens", type=int, default=0)
    parser.add_argument("--request-fields", default="")
    parser.add_argument("--price-is-zero-confirmed", action="store_true")
    args = parser.parse_args()
    try:
        if args.mode == "mock":
            if (
                args.cases
                or args.max_concurrency
                or args.max_tokens
                or args.request_fields
                or args.price_is_zero_confirmed
            ):
                raise BenchmarkError(
                    "subset, concurrency, sampling, and zero-price flags are local-mode options"
                )
            asyncio.run(execute_mock(args.output, args.shape, args.image_digest))
            return
        request_fields = {}
        if args.request_fields:
            try:
                parsed_fields = json.loads(args.request_fields)
            except json.JSONDecodeError as exc:
                raise BenchmarkError("--request-fields must be a JSON object") from exc
            request_fields = validate_request_fields(
                parsed_fields, reject_keys={"temperature", "max_tokens"}
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
        )
        asyncio.run(
            execute_local(
                args.output,
                _with_pricing(endpoint, args.input_price, args.output_price),
                args.image_digest,
                shape=args.shape,
                case_limit=args.cases or None,
                max_concurrency=args.max_concurrency or None,
                max_output_tokens=args.max_tokens or SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
                request_fields=request_fields,
                price_is_zero_confirmed=args.price_is_zero_confirmed,
            )
        )
    except (BenchmarkError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
