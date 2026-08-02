from __future__ import annotations

import asyncio
import json
import math
import time
from pathlib import Path

from .adapters import AgentAdapter
from .config import EvaluationConfig, RunConfig
from .models import CaseResult, RunSummary, TestCase
from .telemetry import NoopTelemetry, TelemetryClient


class WorkloadRunner:
    def __init__(
        self,
        config: RunConfig,
        adapter: AgentAdapter,
        shard_index: int = 0,
        shard_count: int = 1,
        telemetry: TelemetryClient | None = None,
    ) -> None:
        if shard_count < 1 or not 0 <= shard_index < shard_count:
            raise ValueError("shard index must be within shard count")
        self._config = config
        self._adapter = adapter
        self._shard_index = shard_index
        self._shard_count = shard_count
        self._telemetry = telemetry or NoopTelemetry()

    async def execute(self, cases: list[TestCase]) -> tuple[list[CaseResult], RunSummary]:
        started = time.perf_counter()
        selected = [
            case
            for index, case in enumerate(cases)
            if index % self._shard_count == self._shard_index
        ]
        work = [
            (case, iteration)
            for iteration in range(self._config.workload.iterations)
            for case in selected
        ]
        semaphore = asyncio.Semaphore(self._config.workload.concurrency)

        async def bounded(case: TestCase, iteration: int) -> CaseResult:
            async with semaphore:
                return await self._execute_case(case, iteration)

        results = await asyncio.gather(*(bounded(case, iteration) for case, iteration in work))
        return results, summarize(
            self._config.run_id,
            self._shard_index,
            self._shard_count,
            results,
            self._config.evaluation,
            duration_ms=(time.perf_counter() - started) * 1000,
        )

    async def _execute_case(self, case: TestCase, iteration: int) -> CaseResult:
        started = time.perf_counter()
        case_attributes: dict[str, str | int | bool | float] = {
            "agentstorm.run.id": self._config.run_id,
            "agentstorm.case.id": case.case_id,
            "agentstorm.case.iteration": iteration,
        }
        with self._telemetry.start_span("agentstorm.case", case_attributes) as case_span:
            try:
                provider_attributes: dict[str, str | int | bool | float] = {
                    "gen_ai.operation.name": "invoke_agent",
                    "gen_ai.provider.name": self._config.target.provider,
                }
                if self._config.target.model:
                    provider_attributes["gen_ai.request.model"] = self._config.target.model
                with self._telemetry.start_span(
                    "gen_ai.invoke_agent", provider_attributes
                ) as provider_span:
                    try:
                        response = await asyncio.wait_for(
                            self._adapter.run(case),
                            timeout=self._config.workload.timeout_seconds,
                        )
                    except Exception as exc:
                        provider_span.set_error(type(exc).__name__)
                        raise
                    if response.input_tokens is not None:
                        provider_span.set_attribute(
                            "gen_ai.usage.input_tokens", response.input_tokens
                        )
                    if response.output_tokens is not None:
                        provider_span.set_attribute(
                            "gen_ai.usage.output_tokens", response.output_tokens
                        )

                evaluator_name = "contains" if case.expected_contains is not None else "none"
                with self._telemetry.start_span(
                    "agentstorm.evaluate",
                    {
                        "gen_ai.evaluation.name": evaluator_name,
                        "agentstorm.evaluation.deterministic": True,
                    },
                ) as evaluator_span:
                    success = (
                        case.expected_contains is None
                        or case.expected_contains in response.output
                    )
                    evaluator_span.set_attribute(
                        "gen_ai.evaluation.score.label", "pass" if success else "fail"
                    )
                    if not success:
                        evaluator_span.set_error("assertion")
                error = (
                    None
                    if success
                    else f"output does not contain {case.expected_contains!r}"
                )
                result = CaseResult(
                    case_id=case.case_id,
                    iteration=iteration,
                    success=success,
                    latency_ms=(time.perf_counter() - started) * 1000,
                    failure_kind="" if success else "assertion",
                    output=response.output,
                    error=error,
                    input_tokens=response.input_tokens,
                    output_tokens=response.output_tokens,
                )
            except Exception as exc:  # noqa: BLE001 - provider errors are test results
                case_span.set_error(type(exc).__name__)
                result = CaseResult(
                    case_id=case.case_id,
                    iteration=iteration,
                    success=False,
                    latency_ms=(time.perf_counter() - started) * 1000,
                    failure_kind="provider",
                    error=f"{type(exc).__name__}: {exc}",
                )
            case_span.set_attribute("agentstorm.case.success", result.success)
            case_span.set_attribute(
                "agentstorm.case.failure_kind", result.failure_kind or "none"
            )
            if not result.success:
                case_span.set_error(result.failure_kind or "failure")
            if result.input_tokens is not None:
                case_span.set_attribute("gen_ai.usage.input_tokens", result.input_tokens)
            if result.output_tokens is not None:
                case_span.set_attribute("gen_ai.usage.output_tokens", result.output_tokens)
            return result


def summarize(
    run_id: str,
    shard_index: int,
    shard_count: int,
    results: list[CaseResult],
    evaluation: EvaluationConfig,
    duration_ms: float = 0.0,
) -> RunSummary:
    total = len(results)
    succeeded = sum(1 for result in results if result.success)
    failed = total - succeeded
    success_rate = succeeded / total if total else 0.0
    error_rate = failed / total if total else 0.0
    latencies = sorted(result.latency_ms for result in results)
    p95 = latencies[max(0, math.ceil(len(latencies) * 0.95) - 1)] if latencies else 0.0
    failures: list[str] = []
    if evaluation.min_success_rate is not None and success_rate < evaluation.min_success_rate:
        failures.append(
            f"success_rate {success_rate:.4f} < {evaluation.min_success_rate:.4f}"
        )
    if evaluation.max_error_rate is not None and error_rate > evaluation.max_error_rate:
        failures.append(f"error_rate {error_rate:.4f} > {evaluation.max_error_rate:.4f}")
    if evaluation.max_p95_latency_ms is not None and p95 > evaluation.max_p95_latency_ms:
        failures.append(f"p95_latency_ms {p95:.2f} > {evaluation.max_p95_latency_ms}")
    return RunSummary(
        run_id=run_id,
        shard_index=shard_index,
        shard_count=shard_count,
        duration_ms=duration_ms,
        total=total,
        succeeded=succeeded,
        failed=failed,
        success_rate=success_rate,
        error_rate=error_rate,
        p95_latency_ms=p95,
        input_tokens=sum(result.input_tokens or 0 for result in results),
        output_tokens=sum(result.output_tokens or 0 for result in results),
        thresholds_passed=not failures,
        threshold_failures=failures,
    )


def write_results(output_dir: str | Path, results: list[CaseResult], summary: RunSummary) -> None:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    with (target / "results.jsonl").open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result.to_dict(), ensure_ascii=False) + "\n")
    (target / "summary.json").write_text(
        json.dumps(summary.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
