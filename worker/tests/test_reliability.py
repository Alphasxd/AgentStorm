from __future__ import annotations

import asyncio
import unittest

from agentstorm_worker.circuit import CircuitBreaker
from agentstorm_worker.config import (
    CircuitBreakerConfig,
    DatasetConfig,
    EvaluationConfig,
    FaultRuleConfig,
    FaultScenarioConfig,
    PricingConfig,
    ReliabilityConfig,
    RetryConfig,
    RunConfig,
    SourceConfig,
    TargetConfig,
    WorkloadConfig,
)
from agentstorm_worker.execution_errors import ExecutionError
from agentstorm_worker.adapters import ToolLifecycleEvent
from agentstorm_worker.models import AdapterResponse, TestCase
from agentstorm_worker.retry import retry_backoff_ms, retry_eligibility
from agentstorm_worker.runner import WorkloadRunner


class RetryTest(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_retries_whole_agent_and_succeeds(self) -> None:
        adapter = CountingAdapter()
        result = await run_one(
            reliability(
                FaultRuleConfig("first", "rate_limit", 1, attempts=(1,)),
                retry=RetryConfig(
                    max_attempts=3,
                    initial_backoff_ms=1,
                    max_backoff_ms=10,
                    max_cumulative_backoff_ms=20,
                    jitter_ratio=0,
                ),
            ),
            adapter,
        )
        self.assertTrue(result.success)
        self.assertEqual(adapter.calls, 1)
        self.assertEqual([attempt.outcome for attempt in result.attempts], ["failed", "succeeded"])
        self.assertEqual(result.attempts[0].retry_decision, "retry_safe")
        self.assertEqual(result.attempts[0].backoff_ms, 1)
        self.assertEqual((result.input_tokens, result.output_tokens), (2, 3))

    async def test_ambiguous_timeout_and_5xx_require_opt_in(self) -> None:
        for fault, fields, code in (
            ("timeout", {"delay_ms": 0}, "timeout"),
            ("http_error", {"status_code": 503}, "http_5xx"),
        ):
            with self.subTest(fault=fault):
                blocked_adapter = CountingAdapter()
                blocked = await run_one(
                    reliability(
                        FaultRuleConfig("first", fault, 1, attempts=(1,), **fields),
                        retry=RetryConfig(max_attempts=3, jitter_ratio=0),
                    ),
                    blocked_adapter,
                )
                self.assertFalse(blocked.success)
                self.assertEqual(blocked.error_code, code)
                self.assertEqual(blocked.attempts[0].retry_decision, "ambiguous_blocked")
                self.assertEqual(blocked_adapter.calls, 0)

                allowed_adapter = CountingAdapter()
                allowed = await run_one(
                    reliability(
                        FaultRuleConfig("first", fault, 1, attempts=(1,), **fields),
                        retry=RetryConfig(
                            max_attempts=3,
                            initial_backoff_ms=1,
                            max_backoff_ms=10,
                            max_cumulative_backoff_ms=20,
                            jitter_ratio=0,
                            allow_ambiguous_retries=True,
                        ),
                    ),
                    allowed_adapter,
                )
                self.assertTrue(allowed.success)
                self.assertEqual(allowed.attempts[0].retry_decision, "retry_ambiguous")
                self.assertEqual(allowed_adapter.calls, 1)

    async def test_malformed_retry_preserves_all_attempt_usage_and_cost(self) -> None:
        adapter = CountingAdapter()
        result = await run_one(
            reliability(
                FaultRuleConfig("first", "malformed_response", 1, attempts=(1,)),
                retry=RetryConfig(
                    max_attempts=2,
                    initial_backoff_ms=1,
                    max_backoff_ms=1,
                    max_cumulative_backoff_ms=1,
                    jitter_ratio=0,
                    allow_ambiguous_retries=True,
                ),
            ),
            adapter,
            priced=True,
        )
        self.assertTrue(result.success)
        self.assertEqual(adapter.calls, 2)
        self.assertEqual((result.input_tokens, result.output_tokens), (4, 6))
        self.assertTrue(result.usage_complete)
        self.assertEqual(result.cost_usd, "0.000070000000")

    async def test_unknown_failed_usage_keeps_cost_null(self) -> None:
        result = await run_one(
            ReliabilityConfig(
                retry=RetryConfig(max_attempts=1),
            ),
            RaisingAdapter(RuntimeError("sensitive provider detail")),
            priced=True,
        )
        self.assertFalse(result.usage_complete)
        self.assertEqual((result.input_tokens, result.output_tokens), (0, 0))
        self.assertIsNone(result.cost_usd)
        self.assertNotIn("sensitive", result.error or "")

    async def test_retry_stops_at_backoff_budget(self) -> None:
        adapter = CountingAdapter()
        result = await run_one(
            reliability(
                FaultRuleConfig("all", "rate_limit", 1, attempts=(1, 2, 3)),
                retry=RetryConfig(
                    max_attempts=3,
                    initial_backoff_ms=100,
                    max_backoff_ms=100,
                    max_cumulative_backoff_ms=50,
                    jitter_ratio=0,
                ),
            ),
            adapter,
        )
        self.assertFalse(result.success)
        self.assertEqual(len(result.attempts), 1)
        self.assertEqual(result.attempts[0].retry_decision, "backoff_budget")
        self.assertEqual(adapter.calls, 0)

    async def test_retry_stops_at_case_time_budget(self) -> None:
        config = run_config(
            reliability(
                FaultRuleConfig("first", "rate_limit", 1, attempts=(1,)),
                retry=RetryConfig(
                    max_attempts=3,
                    initial_backoff_ms=100,
                    max_backoff_ms=100,
                    max_cumulative_backoff_ms=1000,
                    jitter_ratio=0,
                ),
            ),
            timeout_seconds=0.01,
        )
        results, _ = await WorkloadRunner(config, CountingAdapter()).execute(
            [TestCase("case", "hello")]
        )
        self.assertEqual(results[0].attempts[0].retry_decision, "time_budget")

    async def test_tool_failure_is_never_retried(self) -> None:
        adapter = ToolAdapter()
        result = await run_one(
            reliability(
                FaultRuleConfig(
                    "tool", "tool_error", 1, attempts=(1,), tool_name="lookup"
                ),
                retry=RetryConfig(
                    max_attempts=3,
                    allow_ambiguous_retries=True,
                ),
            ),
            adapter,
        )
        self.assertEqual(adapter.calls, 1)
        self.assertEqual(len(result.attempts), 1)
        self.assertEqual(result.failure_category, "tool")
        self.assertEqual(result.attempts[0].retry_decision, "not_retryable")

    def test_jitter_is_deterministic_and_retry_categories_are_conservative(self) -> None:
        retry = RetryConfig(
            initial_backoff_ms=100,
            max_backoff_ms=1000,
            jitter_ratio=0.2,
        )
        first = retry_backoff_ms(retry, 42, "case", 0, 2)
        self.assertEqual(first, retry_backoff_ms(retry, 42, "case", 0, 2))
        self.assertNotEqual(first, retry_backoff_ms(retry, 43, "case", 0, 2))
        self.assertEqual(
            retry_eligibility(
                ExecutionError("tool", "tool_error", "tool", ambiguous=True), retry
            ),
            "not_retryable",
        )


class CircuitBreakerTest(unittest.IsolatedAsyncioTestCase):
    async def test_open_reject_half_open_and_close(self) -> None:
        now = [0.0]
        breaker = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, open_duration_ms=1000),
            clock=lambda: now[0],
        )
        first = await breaker.acquire()
        self.assertEqual(
            await breaker.record_terminal(
                first, provider_succeeded=False, failure_category="provider"
            ),
            (),
        )
        second = await breaker.acquire()
        self.assertEqual(
            await breaker.record_terminal(
                second, provider_succeeded=False, failure_category="provider"
            ),
            ("open",),
        )
        with self.assertRaisesRegex(ExecutionError, "circuit_open"):
            await breaker.acquire()
        now[0] = 1.0
        permits = await asyncio.gather(
            breaker.acquire(), breaker.acquire(), return_exceptions=True
        )
        probes = [permit for permit in permits if not isinstance(permit, BaseException)]
        rejects = [permit for permit in permits if isinstance(permit, ExecutionError)]
        self.assertEqual(len(probes), 1)
        self.assertEqual(len(rejects), 1)
        self.assertEqual(probes[0].events, ("half_open",))
        self.assertEqual(
            await breaker.record_terminal(
                probes[0], provider_succeeded=True
            ),
            ("close",),
        )
        self.assertFalse((await breaker.acquire()).probe)

    async def test_non_provider_failures_do_not_change_consecutive_count(self) -> None:
        breaker = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, open_duration_ms=1000)
        )
        permit = await breaker.acquire()
        await breaker.record_terminal(
            permit, provider_succeeded=False, failure_category="provider"
        )
        permit = await breaker.acquire()
        await breaker.record_terminal(
            permit, provider_succeeded=False, failure_category="tool"
        )
        permit = await breaker.acquire()
        events = await breaker.record_terminal(
            permit, provider_succeeded=False, failure_category="provider"
        )
        self.assertEqual(events, ("open",))

    async def test_runner_rejects_after_threshold_without_provider_calls(self) -> None:
        config = run_config(
            reliability(
                FaultRuleConfig("all", "rate_limit", 1),
                retry=RetryConfig(max_attempts=1),
                breaker=CircuitBreakerConfig(
                    failure_threshold=2, open_duration_ms=30000
                ),
            ),
            concurrency=1,
        )
        adapter = CountingAdapter()
        results, _ = await WorkloadRunner(config, adapter).execute(
            [TestCase("a", "a"), TestCase("b", "b"), TestCase("c", "c")]
        )
        self.assertEqual([result.error_code for result in results], ["rate_limited", "rate_limited", "circuit_open"])
        self.assertEqual(results[1].attempts[0].circuit_events, ["open"])
        self.assertEqual(results[2].attempts[0].circuit_events, ["reject"])
        self.assertEqual(adapter.calls, 0)


class CancellationTest(unittest.IsolatedAsyncioTestCase):
    async def test_stop_event_cancels_inflight_and_does_not_schedule_more_cases(self) -> None:
        adapter = SlowAdapter()
        runner = WorkloadRunner(
            run_config(ReliabilityConfig(), concurrency=2, timeout_seconds=60), adapter
        )
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            runner.execute(
                [TestCase(f"case-{index}", "hello") for index in range(10)],
                stop_event=stop_event,
            )
        )
        await asyncio.wait_for(adapter.started.wait(), timeout=1)
        stop_event.set()
        results, summary = await asyncio.wait_for(task, timeout=1)

        self.assertEqual(results, [])
        self.assertEqual(summary.total, 0)
        self.assertLessEqual(adapter.calls, 2)


async def run_one(
    reliability_config: ReliabilityConfig,
    adapter: object,
    *,
    priced: bool = False,
):
    config = run_config(reliability_config, priced=priced)
    results, _ = await WorkloadRunner(config, adapter).execute(  # type: ignore[arg-type]
        [TestCase("case", "hello")]
    )
    return results[0]


def reliability(
    rule: FaultRuleConfig,
    *,
    retry: RetryConfig,
    breaker: CircuitBreakerConfig | None = None,
) -> ReliabilityConfig:
    return ReliabilityConfig(
        seed=42,
        retry=retry,
        circuit_breaker=breaker,
        scenario=FaultScenarioConfig(
            source_name="faults",
            source_key="scenario.json",
            digest="sha256:" + "a" * 64,
            rules=(rule,),
        ),
    )


def run_config(
    reliability_config: ReliabilityConfig,
    *,
    priced: bool = False,
    concurrency: int = 1,
    timeout_seconds: float = 2,
) -> RunConfig:
    return RunConfig(
        run_id="reliability",
        source=SourceConfig("default", "reliability"),
        dataset=DatasetConfig("dataset", "cases.jsonl"),
        target=TargetConfig(
            provider="fake",
            pricing=PricingConfig("2.5", "10") if priced else None,
        ),
        workload=WorkloadConfig(
            concurrency=concurrency,
            iterations=1,
            timeout_seconds=timeout_seconds,  # type: ignore[arg-type]
        ),
        evaluation=EvaluationConfig(),
        reliability=reliability_config,
    )


class CountingAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, case: TestCase, lifecycle: object | None = None) -> AdapterResponse:
        del case, lifecycle
        self.calls += 1
        return AdapterResponse("ok", input_tokens=2, output_tokens=3)


class RaisingAdapter:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def run(self, case: TestCase, lifecycle: object | None = None) -> AdapterResponse:
        del case, lifecycle
        raise self.error


class ToolAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, case: TestCase, lifecycle: object | None = None) -> AdapterResponse:
        del case
        self.calls += 1
        if lifecycle is not None:
            lifecycle.tool_started(  # type: ignore[attr-defined]
                ToolLifecycleEvent("tool-1", "lookup")
            )
        return AdapterResponse("unused", input_tokens=2, output_tokens=3)


class SlowAdapter:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()

    async def run(self, case: TestCase, lifecycle: object | None = None) -> AdapterResponse:
        del case, lifecycle
        self.calls += 1
        self.started.set()
        await asyncio.sleep(30)
        return AdapterResponse("unused")


if __name__ == "__main__":
    unittest.main()
