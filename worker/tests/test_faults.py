from __future__ import annotations

import unittest

from agentstorm_worker.adapters import ToolLifecycleEvent
from agentstorm_worker.config import FaultRuleConfig, FaultScenarioConfig
from agentstorm_worker.execution_errors import ExecutionError
from agentstorm_worker.faults import FaultInjectionMiddleware, select_fault_rule
from agentstorm_worker.models import AdapterResponse, TestCase


class FaultInjectionTest(unittest.IsolatedAsyncioTestCase):
    def test_selection_is_stable_across_scheduling_inputs(self) -> None:
        scenario = fault_scenario(
            FaultRuleConfig("half", "rate_limit", 0.5, attempts=(1, 2))
        )
        first = [
            select_fault_rule(scenario, 42, f"case-{index}", 0, 1)
            for index in range(50)
        ]
        second = [
            select_fault_rule(scenario, 42, f"case-{index}", 0, 1)
            for index in reversed(range(50))
        ]
        self.assertEqual(
            [rule is not None for rule in first],
            list(reversed([rule is not None for rule in second])),
        )
        changed = [
            select_fault_rule(scenario, 43, f"case-{index}", 0, 1)
            for index in range(50)
        ]
        self.assertNotEqual(
            [rule is not None for rule in first],
            [rule is not None for rule in changed],
        )

    def test_rule_order_and_selectors(self) -> None:
        scenario = fault_scenario(
            FaultRuleConfig(
                "specific",
                "timeout",
                1,
                case_ids=("case-a",),
                iterations=(2,),
                attempts=(3,),
                delay_ms=0,
            ),
            FaultRuleConfig("fallback", "rate_limit", 1, attempts=(3,)),
        )
        selected = select_fault_rule(scenario, 7, "case-a", 2, 3)
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected.name, "specific")
        fallback = select_fault_rule(scenario, 7, "case-b", 2, 3)
        self.assertIsNotNone(fallback)
        assert fallback is not None
        self.assertEqual(fallback.name, "fallback")
        self.assertIsNone(select_fault_rule(scenario, 7, "case-a", 2, 1))

    async def test_provider_faults_do_not_call_adapter(self) -> None:
        for fault, options, code in (
            ("timeout", {"delay_ms": 0}, "timeout"),
            ("rate_limit", {}, "rate_limited"),
            ("http_error", {"status_code": 503}, "http_5xx"),
        ):
            with self.subTest(fault=fault):
                adapter = RecordingAdapter()
                middleware = FaultInjectionMiddleware(
                    adapter,
                    fault_scenario(FaultRuleConfig("inject", fault, 1, **options)),
                    42,
                )
                with self.assertRaises(ExecutionError) as caught:
                    await middleware.run(TestCase("a", "hello"), 0, 1)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(caught.exception.injected_rule, "inject")
                self.assertEqual(adapter.calls, 0)

    async def test_malformed_response_preserves_usage(self) -> None:
        adapter = RecordingAdapter()
        middleware = FaultInjectionMiddleware(
            adapter,
            fault_scenario(FaultRuleConfig("malformed", "malformed_response", 1)),
            42,
        )
        with self.assertRaises(ExecutionError) as caught:
            await middleware.run(TestCase("a", "hello"), 0, 1)
        self.assertEqual(adapter.calls, 1)
        self.assertIsNotNone(caught.exception.response)
        assert caught.exception.response is not None
        self.assertEqual(caught.exception.response.input_tokens, 11)
        self.assertEqual(caught.exception.response.output_tokens, 7)

    async def test_latency_calls_adapter_and_tool_error_uses_lifecycle(self) -> None:
        adapter = RecordingAdapter()
        latency = FaultInjectionMiddleware(
            adapter,
            fault_scenario(FaultRuleConfig("slow", "latency", 1, delay_ms=0)),
            42,
        )
        response = await latency.run(TestCase("a", "hello"), 0, 1)
        self.assertEqual(response.metadata["agentstorm.injected_fault"], "latency")
        self.assertEqual(adapter.calls, 1)

        tool_adapter = RecordingAdapter(tool_name="safe.lookup")
        tool = FaultInjectionMiddleware(
            tool_adapter,
            fault_scenario(
                FaultRuleConfig("tool", "tool_error", 1, tool_name="safe.lookup")
            ),
            42,
        )
        with self.assertRaises(ExecutionError) as caught:
            await tool.run(TestCase("a", "hello"), 0, 1, RecordingLifecycle())
        self.assertEqual(caught.exception.category, "tool")
        self.assertEqual(caught.exception.code, "injected_tool_error")
        self.assertEqual(tool_adapter.calls, 1)


def fault_scenario(*rules: FaultRuleConfig) -> FaultScenarioConfig:
    return FaultScenarioConfig(
        source_name="faults",
        source_key="scenario.json",
        digest="sha256:" + "a" * 64,
        rules=rules,
    )


class RecordingAdapter:
    def __init__(self, tool_name: str = "") -> None:
        self.calls = 0
        self.tool_name = tool_name

    async def run(self, case: TestCase, lifecycle: object | None = None) -> AdapterResponse:
        del case
        self.calls += 1
        if self.tool_name and lifecycle is not None:
            lifecycle.tool_started(  # type: ignore[attr-defined]
                ToolLifecycleEvent("tool-1", self.tool_name)
            )
        return AdapterResponse("ok", input_tokens=11, output_tokens=7)


class RecordingLifecycle:
    def tool_started(self, event: object) -> None:
        del event

    def tool_finished(self, event: object, error_type: str = "") -> None:
        del event, error_type

    def handed_off(self, event: object) -> None:
        del event


if __name__ == "__main__":
    unittest.main()
