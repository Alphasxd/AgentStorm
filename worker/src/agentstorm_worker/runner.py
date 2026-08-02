from __future__ import annotations

import asyncio
import json
import math
import time
from pathlib import Path

from .adapters import (
    AgentAdapter,
    AgentLifecycle,
    HandoffLifecycleEvent,
    ToolLifecycleEvent,
)
from .config import EvaluationConfig, RunConfig
from .evaluators import EvaluationContext, EvaluatorRegistry
from .execution_errors import ExecutionError, classify_adapter_exception
from .faults import FaultInjectionMiddleware
from .models import CaseResult, RunSummary, TestCase
from .pricing import case_costs, sum_costs
from .telemetry import (
    ContentSanitizer,
    DetachedTraceSpan,
    NoopTelemetry,
    TelemetryClient,
    telemetry_with_policy,
)


class _CaseLifecycle(AgentLifecycle):
    def __init__(
        self, telemetry: TelemetryClient, content_sanitizer: ContentSanitizer | None
    ) -> None:
        self._telemetry = telemetry
        self._content_sanitizer = content_sanitizer
        self._tools: dict[str, DetachedTraceSpan] = {}
        self.tool_path: list[str] = []

    def tool_started(self, event: ToolLifecycleEvent) -> None:
        self.tool_path.append(event.name)
        previous = self._tools.pop(event.invocation_id, None)
        if previous is not None:
            previous.set_error("duplicate_tool_start")
            previous.end()
        attributes: dict[str, str | int | bool | float] = {
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": event.name,
            "gen_ai.tool.type": event.tool_type,
        }
        if event.call_id is not None:
            attributes["gen_ai.tool.call.id"] = event.call_id
        if event.agent_name is not None:
            attributes["gen_ai.agent.name"] = event.agent_name
        if self._content_sanitizer is not None and event.arguments is not None:
            arguments = self._content_sanitizer.content(event.arguments)
            if arguments is not None:
                attributes["agentstorm.content.tool.arguments"] = arguments
        self._tools[event.invocation_id] = self._telemetry.start_detached_span(
            "gen_ai.execute_tool", attributes
        )

    def tool_finished(self, event: ToolLifecycleEvent, error_type: str = "") -> None:
        span = self._tools.pop(event.invocation_id, None)
        if span is None:
            self.tool_started(event)
            span = self._tools.pop(event.invocation_id)
        if error_type:
            span.set_error(error_type)
        if self._content_sanitizer is not None and event.result is not None:
            result = self._content_sanitizer.content(event.result)
            if result is not None:
                span.set_attribute("agentstorm.content.tool.result", result)
        span.end()

    def handed_off(self, event: HandoffLifecycleEvent) -> None:
        span = self._telemetry.start_detached_span(
            "agentstorm.handoff",
            {
                "agentstorm.handoff.source_agent": event.source_agent,
                "agentstorm.handoff.target_agent": event.target_agent,
            },
        )
        span.end()

    def close(self, error_type: str = "incomplete_tool_call") -> None:
        for span in self._tools.values():
            span.set_error(error_type)
            span.end()
        self._tools.clear()


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
        self._shard_index = shard_index
        self._shard_count = shard_count
        self._telemetry = telemetry_with_policy(
            telemetry or NoopTelemetry(), config.telemetry
        )
        self._content_sanitizer = (
            ContentSanitizer(config.telemetry.redaction.patterns)
            if config.telemetry.content_mode == "redacted"
            else None
        )
        self._evaluators: EvaluatorRegistry | None = None
        reliability = config.reliability
        self._faults = FaultInjectionMiddleware(
            adapter,
            reliability.scenario if reliability is not None else None,
            reliability.seed if reliability is not None else None,
        )

    async def execute(self, cases: list[TestCase]) -> tuple[list[CaseResult], RunSummary]:
        self._evaluators = EvaluatorRegistry(cases)
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
        case_attributes: dict[str, str | int | bool | float] = {
            "agentstorm.run.id": self._config.run_id,
            "agentstorm.case.id": case.case_id,
            "agentstorm.case.iteration": iteration,
        }
        if self._content_sanitizer is not None:
            case_attributes["agentstorm.content.prompt"] = self._content_sanitizer.string(
                case.prompt
            )
            for key in self._config.telemetry.redaction.metadata_keys:
                if key not in case.metadata or self._content_sanitizer.sensitive_key(key):
                    continue
                value = self._content_sanitizer.content(case.metadata[key])
                if value is not None:
                    case_attributes[f"agentstorm.case.metadata.{key}"] = value
        with self._telemetry.start_span("agentstorm.case", case_attributes) as case_span:
            lifecycle = _CaseLifecycle(self._telemetry, self._content_sanitizer)
            provider_attributes: dict[str, str | int | bool | float] = {
                "gen_ai.operation.name": "invoke_agent",
                "gen_ai.provider.name": self._config.target.provider,
            }
            if self._config.target.model:
                provider_attributes["gen_ai.request.model"] = self._config.target.model
            response = None
            execution_error: ExecutionError | None = None
            provider_error_type = ""
            with self._telemetry.start_span(
                "gen_ai.invoke_agent", provider_attributes
            ) as provider_span:
                provider_started = time.perf_counter()
                try:
                    response = await asyncio.wait_for(
                        self._faults.run(case, iteration, 1, lifecycle=lifecycle),
                        timeout=self._config.workload.timeout_seconds,
                    )
                except Exception as exc:  # noqa: BLE001 - adapter boundary
                    provider_error_type = type(exc).__name__
                    execution_error = classify_adapter_exception(exc)
                    response = execution_error.response
                    provider_span.set_error(provider_error_type)
                    provider_span.set_attribute(
                        "agentstorm.failure.category", execution_error.category
                    )
                    provider_span.set_attribute(
                        "agentstorm.error.code", execution_error.code
                    )
                provider_latency_ms = (time.perf_counter() - provider_started) * 1000
                lifecycle.close(provider_error_type)
                if response is not None and response.input_tokens is not None:
                    provider_span.set_attribute(
                        "gen_ai.usage.input_tokens", response.input_tokens
                    )
                if response is not None and response.output_tokens is not None:
                    provider_span.set_attribute(
                        "gen_ai.usage.output_tokens", response.output_tokens
                    )
                if (
                    execution_error is None
                    and response is not None
                    and self._content_sanitizer is not None
                ):
                    provider_span.set_attribute(
                        "agentstorm.content.output",
                        self._content_sanitizer.string(response.output),
                    )

            if execution_error is not None:
                input_tokens = response.input_tokens if response is not None else None
                output_tokens = response.output_tokens if response is not None else None
                input_cost, output_cost, total_cost = case_costs(
                    self._config.target.pricing,
                    input_tokens or 0,
                    output_tokens or 0,
                )
                result = CaseResult(
                    case_id=case.case_id,
                    iteration=iteration,
                    success=False,
                    latency_ms=provider_latency_ms,
                    failure_kind=execution_error.failure_kind,
                    failure_category=execution_error.category,
                    error_code=execution_error.code,
                    error=str(execution_error),
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    tool_path=lifecycle.tool_path,
                    input_cost_usd=input_cost,
                    output_cost_usd=output_cost,
                    cost_usd=total_cost,
                )
            else:
                assert response is not None
                assert self._evaluators is not None
                try:
                    outcomes = await self._evaluators.evaluate(
                        case,
                        EvaluationContext(
                            case_id=case.case_id,
                            prompt=case.prompt,
                            output=response.output,
                            latency_ms=provider_latency_ms,
                            tool_path=lifecycle.tool_path,
                            metadata=case.metadata,
                        ),
                    )
                except Exception as exc:  # noqa: BLE001 - unexpected evaluator failure is harness
                    del exc
                    input_cost, output_cost, total_cost = case_costs(
                        self._config.target.pricing,
                        response.input_tokens or 0,
                        response.output_tokens or 0,
                    )
                    result = CaseResult(
                        case_id=case.case_id,
                        iteration=iteration,
                        success=False,
                        latency_ms=provider_latency_ms,
                        failure_kind="harness",
                        failure_category="harness",
                        error_code="evaluator_failure",
                        error="harness/evaluator_failure",
                        input_tokens=response.input_tokens,
                        output_tokens=response.output_tokens,
                        tool_path=lifecycle.tool_path,
                        input_cost_usd=input_cost,
                        output_cost_usd=output_cost,
                        cost_usd=total_cost,
                    )
                    outcomes = None
                if outcomes is not None:
                    if not outcomes:
                        with self._telemetry.start_span(
                            "agentstorm.evaluate",
                            {
                                "gen_ai.evaluation.name": "none",
                                "agentstorm.evaluation.index": 0,
                                "agentstorm.evaluation.deterministic": True,
                                "gen_ai.evaluation.score.label": "pass",
                            },
                        ):
                            pass
                    for outcome in outcomes:
                        with self._telemetry.start_span(
                            "agentstorm.evaluate",
                            {
                                "gen_ai.evaluation.name": outcome.type,
                                "agentstorm.evaluation.index": outcome.index,
                                "agentstorm.evaluation.deterministic": True,
                                "gen_ai.evaluation.score.label": (
                                    "pass" if outcome.passed else "fail"
                                ),
                            },
                        ) as evaluator_span:
                            if not outcome.passed:
                                evaluator_span.set_error("assertion")
                    success = all(outcome.passed for outcome in outcomes)
                    failed_types = [outcome.type for outcome in outcomes if not outcome.passed]
                    error = None if success else f"failed assertions: {', '.join(failed_types)}"
                    input_cost, output_cost, total_cost = case_costs(
                        self._config.target.pricing,
                        response.input_tokens or 0,
                        response.output_tokens or 0,
                    )
                    result = CaseResult(
                        case_id=case.case_id,
                        iteration=iteration,
                        success=success,
                        latency_ms=provider_latency_ms,
                        failure_kind="" if success else "assertion",
                        failure_category="" if success else "evaluation",
                        error_code="" if success else "assertion_failed",
                        output=response.output,
                        error=error,
                        input_tokens=response.input_tokens,
                        output_tokens=response.output_tokens,
                        tool_path=lifecycle.tool_path,
                        assertions=outcomes,
                        input_cost_usd=input_cost,
                        output_cost_usd=output_cost,
                        cost_usd=total_cost,
                    )
            case_span.set_attribute("agentstorm.case.success", result.success)
            case_span.set_attribute(
                "agentstorm.case.failure_kind", result.failure_kind or "none"
            )
            if not result.success:
                case_span.set_error(result.failure_kind or "failure")
                case_span.set_attribute(
                    "agentstorm.case.failure_category", result.failure_category
                )
                case_span.set_attribute("agentstorm.case.error_code", result.error_code)
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
        input_cost_usd=sum_costs([result.input_cost_usd for result in results]),
        output_cost_usd=sum_costs([result.output_cost_usd for result in results]),
        cost_usd=sum_costs([result.cost_usd for result in results]),
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
