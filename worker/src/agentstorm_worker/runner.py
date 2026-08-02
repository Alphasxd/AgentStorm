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
from .circuit import CircuitBreaker, CircuitPermit
from .evaluators import EvaluationContext, EvaluatorRegistry
from .execution_errors import ExecutionError, classify_adapter_exception
from .faults import FaultInjectionMiddleware
from .models import AdapterResponse, AttemptResult, CaseResult, RunSummary, TestCase
from .pricing import case_costs, sum_costs
from .retry import retry_backoff_ms, retry_eligibility
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
        self._circuit = CircuitBreaker(
            reliability.circuit_breaker if reliability is not None else None
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
        case_started = time.perf_counter()
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
            attempts: list[AttemptResult] = []
            tool_path: list[str] = []
            try:
                permit = await self._circuit.acquire()
            except ExecutionError as circuit_error:
                self._trace_circuit_events(circuit_error.circuit_events)
                attempts.append(
                    AttemptResult(
                        number=1,
                        latency_ms=0,
                        outcome="rejected",
                        failure_category=circuit_error.category,
                        error_code=circuit_error.code,
                        retry_decision="not_retryable",
                        input_tokens=0,
                        output_tokens=0,
                        usage_complete=True,
                        circuit_events=list(circuit_error.circuit_events),
                    )
                )
                result = self._failed_result(
                    case,
                    iteration,
                    circuit_error,
                    attempts,
                    tool_path,
                    (time.perf_counter() - case_started) * 1000,
                )
            else:
                result = await self._execute_permitted_case(
                    case, iteration, permit, attempts, tool_path, case_started
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

    async def _execute_permitted_case(
        self,
        case: TestCase,
        iteration: int,
        permit: CircuitPermit,
        attempts: list[AttemptResult],
        tool_path: list[str],
        case_started: float,
    ) -> CaseResult:
        reliability = self._config.reliability
        retry = reliability.retry if reliability is not None else None
        deadline = case_started + self._config.workload.timeout_seconds
        cumulative_backoff_ms = 0
        response: AdapterResponse | None = None
        execution_error: ExecutionError | None = None
        final_provider_latency_ms = 0.0
        attempt_number = 1

        self._trace_circuit_events(permit.events)

        while True:
            remaining = max(0.0, deadline - time.perf_counter())
            with self._telemetry.start_span(
                "agentstorm.attempt",
                {
                    "agentstorm.case.id": case.case_id,
                    "agentstorm.case.iteration": iteration,
                    "agentstorm.attempt.number": attempt_number,
                },
            ) as attempt_span:
                response, execution_error, attempt, attempt_tools = await self._invoke_attempt(
                    case, iteration, attempt_number, remaining
                )
                if attempt_number == 1:
                    attempt.circuit_events.extend(permit.events)
                tool_path.extend(attempt_tools)
                final_provider_latency_ms = attempt.latency_ms
                if execution_error is None:
                    attempt.retry_decision = "not_needed"
                else:
                    decision = (
                        retry_eligibility(execution_error, retry)
                        if retry is not None
                        else "not_retryable"
                    )
                    if decision.startswith("retry_") and permit.probe:
                        decision = "half_open_no_retry"
                    if decision.startswith("retry_") and attempt_number >= retry.max_attempts:
                        decision = "attempt_limit"
                    backoff_ms = 0
                    if decision.startswith("retry_"):
                        assert retry is not None
                        backoff_ms = retry_backoff_ms(
                            retry,
                            reliability.seed if reliability is not None else None,
                            case.case_id,
                            iteration,
                            attempt_number,
                        )
                        if cumulative_backoff_ms + backoff_ms > retry.max_cumulative_backoff_ms:
                            decision = "backoff_budget"
                            backoff_ms = 0
                        elif backoff_ms / 1000 >= max(0.0, deadline - time.perf_counter()):
                            decision = "time_budget"
                            backoff_ms = 0
                    attempt.retry_decision = decision
                    attempt.backoff_ms = backoff_ms
                attempts.append(attempt)
                attempt_span.set_attribute("agentstorm.attempt.outcome", attempt.outcome)
                attempt_span.set_attribute(
                    "agentstorm.retry.decision", attempt.retry_decision
                )
                attempt_span.set_attribute("agentstorm.retry.backoff_ms", attempt.backoff_ms)
                attempt_span.set_attribute("agentstorm.attempt.ambiguous", attempt.ambiguous)
                if attempt.injected_fault:
                    attempt_span.set_attribute(
                        "agentstorm.fault.type", attempt.injected_fault
                    )
                if execution_error is not None:
                    attempt_span.set_error(execution_error.code)

            if execution_error is None or not attempt.retry_decision.startswith("retry_"):
                break
            with self._telemetry.start_span(
                "agentstorm.retry",
                {
                    "agentstorm.case.id": case.case_id,
                    "agentstorm.case.iteration": iteration,
                    "agentstorm.attempt.number": attempt_number,
                    "agentstorm.retry.decision": attempt.retry_decision,
                    "agentstorm.retry.backoff_ms": attempt.backoff_ms,
                },
            ):
                await asyncio.sleep(attempt.backoff_ms / 1000)
            cumulative_backoff_ms += attempt.backoff_ms
            attempt_number += 1

        provider_succeeded = execution_error is None and response is not None
        circuit_events = await self._circuit.record_terminal(
            permit,
            provider_succeeded=provider_succeeded,
            failure_category=execution_error.category if execution_error is not None else "",
        )
        if circuit_events:
            attempts[-1].circuit_events.extend(circuit_events)
            self._trace_circuit_events(circuit_events)

        provider_sequence_latency_ms = (time.perf_counter() - case_started) * 1000
        if execution_error is not None:
            return self._failed_result(
                case,
                iteration,
                execution_error,
                attempts,
                tool_path,
                provider_sequence_latency_ms,
            )
        assert response is not None
        assert self._evaluators is not None
        try:
            outcomes = await self._evaluators.evaluate(
                case,
                EvaluationContext(
                    case_id=case.case_id,
                    prompt=case.prompt,
                    output=response.output,
                    latency_ms=final_provider_latency_ms,
                    tool_path=tool_path,
                    metadata=case.metadata,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - evaluator boundary
            del exc
            return self._failed_result(
                case,
                iteration,
                ExecutionError("harness", "evaluator_failure", "harness"),
                attempts,
                tool_path,
                provider_sequence_latency_ms,
            )

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
                    "gen_ai.evaluation.score.label": "pass" if outcome.passed else "fail",
                },
            ) as evaluator_span:
                if not outcome.passed:
                    evaluator_span.set_error("assertion")
        success = all(outcome.passed for outcome in outcomes)
        input_tokens, output_tokens, usage_complete = _attempt_usage(attempts)
        input_cost, output_cost, total_cost = _costs_for_usage(
            self._config, input_tokens, output_tokens, usage_complete
        )
        failed_types = [outcome.type for outcome in outcomes if not outcome.passed]
        return CaseResult(
            case_id=case.case_id,
            iteration=iteration,
            success=success,
            latency_ms=provider_sequence_latency_ms,
            failure_kind="" if success else "assertion",
            failure_category="" if success else "evaluation",
            error_code="" if success else "assertion_failed",
            attempts=attempts,
            usage_complete=usage_complete,
            output=response.output,
            error=None if success else f"failed assertions: {', '.join(failed_types)}",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            tool_path=tool_path,
            assertions=outcomes,
            input_cost_usd=input_cost,
            output_cost_usd=output_cost,
            cost_usd=total_cost,
        )

    async def _invoke_attempt(
        self,
        case: TestCase,
        iteration: int,
        attempt_number: int,
        remaining_seconds: float,
    ) -> tuple[AdapterResponse | None, ExecutionError | None, AttemptResult, list[str]]:
        lifecycle = _CaseLifecycle(self._telemetry, self._content_sanitizer)
        provider_attributes: dict[str, str | int | bool | float] = {
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.provider.name": self._config.target.provider,
            "agentstorm.attempt.number": attempt_number,
        }
        if self._config.target.model:
            provider_attributes["gen_ai.request.model"] = self._config.target.model
        response: AdapterResponse | None = None
        execution_error: ExecutionError | None = None
        provider_error_type = ""
        with self._telemetry.start_span(
            "gen_ai.invoke_agent", provider_attributes
        ) as provider_span:
            provider_started = time.perf_counter()
            try:
                if remaining_seconds <= 0:
                    raise ExecutionError("provider", "timeout", "timeout", ambiguous=True)
                response = await asyncio.wait_for(
                    self._faults.run(
                        case, iteration, attempt_number, lifecycle=lifecycle
                    ),
                    timeout=remaining_seconds,
                )
            except Exception as exc:  # noqa: BLE001 - adapter boundary
                provider_error_type = type(exc).__name__
                execution_error = classify_adapter_exception(exc)
                response = execution_error.response
                provider_span.set_error(provider_error_type)
                provider_span.set_attribute(
                    "agentstorm.failure.category", execution_error.category
                )
                provider_span.set_attribute("agentstorm.error.code", execution_error.code)
            latency_ms = (time.perf_counter() - provider_started) * 1000
            if execution_error is None:
                lifecycle.close()
            else:
                lifecycle.close(provider_error_type)
            if response is not None and response.input_tokens is not None:
                provider_span.set_attribute("gen_ai.usage.input_tokens", response.input_tokens)
            if response is not None and response.output_tokens is not None:
                provider_span.set_attribute("gen_ai.usage.output_tokens", response.output_tokens)
            if execution_error is None and response is not None and self._content_sanitizer is not None:
                provider_span.set_attribute(
                    "agentstorm.content.output",
                    self._content_sanitizer.string(response.output),
                )

        usage_complete = (
            execution_error.usage_complete
            if execution_error is not None
            else response is not None
            and response.input_tokens is not None
            and response.output_tokens is not None
        )
        if response is not None:
            input_tokens = response.input_tokens
            output_tokens = response.output_tokens
        elif usage_complete:
            input_tokens = 0
            output_tokens = 0
        else:
            input_tokens = None
            output_tokens = None
        injected_rule = execution_error.injected_rule if execution_error is not None else ""
        injected_fault = execution_error.injected_fault if execution_error is not None else ""
        if response is not None and execution_error is None:
            injected_rule = str(response.metadata.get("agentstorm.injected_rule", ""))
            injected_fault = str(response.metadata.get("agentstorm.injected_fault", ""))
        attempt = AttemptResult(
            number=attempt_number,
            latency_ms=latency_ms,
            outcome="failed" if execution_error is not None else "succeeded",
            failure_category=execution_error.category if execution_error is not None else "",
            error_code=execution_error.code if execution_error is not None else "",
            injected_rule=injected_rule,
            injected_fault=injected_fault,
            ambiguous=execution_error.ambiguous if execution_error is not None else False,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            usage_complete=usage_complete,
            circuit_events=list(execution_error.circuit_events) if execution_error is not None else [],
        )
        return response, execution_error, attempt, lifecycle.tool_path

    def _failed_result(
        self,
        case: TestCase,
        iteration: int,
        error: ExecutionError,
        attempts: list[AttemptResult],
        tool_path: list[str],
        latency_ms: float,
    ) -> CaseResult:
        input_tokens, output_tokens, usage_complete = _attempt_usage(attempts)
        input_cost, output_cost, total_cost = _costs_for_usage(
            self._config, input_tokens, output_tokens, usage_complete
        )
        return CaseResult(
            case_id=case.case_id,
            iteration=iteration,
            success=False,
            latency_ms=latency_ms,
            failure_kind=error.failure_kind,
            failure_category=error.category,
            error_code=error.code,
            attempts=attempts,
            usage_complete=usage_complete,
            error=str(error),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            tool_path=tool_path,
            input_cost_usd=input_cost,
            output_cost_usd=output_cost,
            cost_usd=total_cost,
        )

    def _trace_circuit_events(self, events: tuple[str, ...]) -> None:
        for event in events:
            with self._telemetry.start_span(
                "agentstorm.circuit",
                {
                    "gen_ai.provider.name": self._config.target.provider,
                    "agentstorm.circuit.event": event,
                },
            ):
                pass


def _attempt_usage(attempts: list[AttemptResult]) -> tuple[int, int, bool]:
    return (
        sum(attempt.input_tokens or 0 for attempt in attempts),
        sum(attempt.output_tokens or 0 for attempt in attempts),
        all(attempt.usage_complete for attempt in attempts),
    )


def _costs_for_usage(
    config: RunConfig,
    input_tokens: int,
    output_tokens: int,
    usage_complete: bool,
) -> tuple[str | None, str | None, str | None]:
    if not usage_complete:
        return None, None, None
    return case_costs(config.target.pricing, input_tokens, output_tokens)


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
        usage_complete=all(result.usage_complete for result in results),
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
