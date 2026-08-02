from __future__ import annotations

import os
import unittest
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from unittest.mock import patch

from agentstorm_worker.adapters import (
    AgentLifecycle,
    HandoffLifecycleEvent,
    ToolLifecycleEvent,
)
from agentstorm_worker.config import (
    DatasetConfig,
    EvaluationConfig,
    RunConfig,
    SourceConfig,
    TargetConfig,
    WorkloadConfig,
)
from agentstorm_worker.models import AdapterResponse, TestCase
from agentstorm_worker.runner import WorkloadRunner
from agentstorm_worker.telemetry import AttributeValue, NoopTelemetry, telemetry_from_environment


@dataclass
class RecordedSpan(AbstractContextManager["RecordedSpan"]):
    name: str
    attributes: dict[str, AttributeValue] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    ended: bool = False

    def __enter__(self) -> RecordedSpan:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def set_attribute(self, name: str, value: AttributeValue) -> None:
        self.attributes[name] = value

    def set_error(self, error_type: str) -> None:
        self.errors.append(error_type)

    def end(self) -> None:
        self.ended = True


class RecordingTelemetry:
    def __init__(self) -> None:
        self.spans: list[RecordedSpan] = []

    def start_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> RecordedSpan:
        span = RecordedSpan(name=name, attributes=dict(attributes))
        self.spans.append(span)
        return span

    def start_detached_span(
        self, name: str, attributes: dict[str, AttributeValue]
    ) -> RecordedSpan:
        span = RecordedSpan(name=name, attributes=dict(attributes))
        self.spans.append(span)
        return span

    def shutdown(self) -> None:
        return None


class SensitiveAdapter:
    async def run(
        self, case: TestCase, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case, lifecycle
        return AdapterResponse(
            output="private model output",
            input_tokens=7,
            output_tokens=11,
        )


class FailingAdapter:
    async def run(
        self, case: TestCase, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case, lifecycle
        raise RuntimeError("private provider error body")


class LifecycleAdapter:
    async def run(
        self, case: TestCase, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case
        assert lifecycle is not None
        tool = ToolLifecycleEvent(
            invocation_id="invocation-1",
            name="safe_lookup",
            call_id="call-1",
            agent_name="triage-agent",
        )
        lifecycle.tool_started(tool)
        lifecycle.tool_finished(tool)
        lifecycle.handed_off(
            HandoffLifecycleEvent(
                source_agent="triage-agent",
                target_agent="answer-agent",
            )
        )
        return AdapterResponse(output="private tool result")


class InterruptedToolAdapter:
    async def run(
        self, case: TestCase, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        del case
        assert lifecycle is not None
        lifecycle.tool_started(
            ToolLifecycleEvent(
                invocation_id="invocation-1",
                name="safe_lookup",
            )
        )
        raise RuntimeError("private tool failure")


def test_config() -> RunConfig:
    return RunConfig(
        run_id="run-1",
        source=SourceConfig(namespace="tests", name="run"),
        dataset=DatasetConfig(name="dataset", key="cases.jsonl"),
        target=TargetConfig(provider="fake", model="safe-model"),
        workload=WorkloadConfig(concurrency=1, iterations=1, timeout_seconds=1),
        evaluation=EvaluationConfig(),
    )


class TelemetryTest(unittest.IsolatedAsyncioTestCase):
    def test_tracing_is_disabled_by_default_and_boolean_is_strict(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsInstance(telemetry_from_environment(), NoopTelemetry)
        with patch.dict(os.environ, {"AGENTSTORM_OTEL_ENABLED": "sometimes"}, clear=True):
            with self.assertRaisesRegex(ValueError, "must be true or false"):
                telemetry_from_environment()

    async def test_spans_omit_prompt_output_and_assertion_content(self) -> None:
        telemetry = RecordingTelemetry()
        runner = WorkloadRunner(
            test_config(),
            SensitiveAdapter(),
            telemetry=telemetry,
        )
        results, _ = await runner.execute(
            [
                TestCase(
                    case_id="case-1",
                    prompt="private prompt",
                    expected_contains="private expected value",
                )
            ]
        )

        self.assertFalse(results[0].success)
        self.assertEqual(
            [span.name for span in telemetry.spans],
            ["agentstorm.case", "gen_ai.invoke_agent", "agentstorm.evaluate"],
        )
        rendered = repr([(span.attributes, span.errors) for span in telemetry.spans])
        for forbidden in (
            "private prompt",
            "private model output",
            "private expected value",
        ):
            self.assertNotIn(forbidden, rendered)
        provider_span = telemetry.spans[1]
        self.assertEqual(provider_span.attributes["gen_ai.usage.input_tokens"], 7)
        self.assertEqual(provider_span.attributes["gen_ai.usage.output_tokens"], 11)
        self.assertEqual(telemetry.spans[0].errors, ["assertion"])

    async def test_provider_error_body_is_not_recorded(self) -> None:
        telemetry = RecordingTelemetry()
        runner = WorkloadRunner(test_config(), FailingAdapter(), telemetry=telemetry)
        results, _ = await runner.execute(
            [TestCase(case_id="case-1", prompt="private prompt")]
        )

        self.assertFalse(results[0].success)
        rendered = repr([(span.attributes, span.errors) for span in telemetry.spans])
        self.assertNotIn("private provider error body", rendered)
        self.assertIn("RuntimeError", rendered)

    async def test_tool_and_handoff_lifecycle_emit_content_safe_spans(self) -> None:
        telemetry = RecordingTelemetry()
        runner = WorkloadRunner(test_config(), LifecycleAdapter(), telemetry=telemetry)

        results, _ = await runner.execute(
            [TestCase(case_id="case-1", prompt="private tool arguments")]
        )

        self.assertTrue(results[0].success)
        self.assertEqual(
            [span.name for span in telemetry.spans],
            [
                "agentstorm.case",
                "gen_ai.invoke_agent",
                "gen_ai.execute_tool",
                "agentstorm.handoff",
                "agentstorm.evaluate",
            ],
        )
        tool_span = telemetry.spans[2]
        self.assertEqual(tool_span.attributes["gen_ai.operation.name"], "execute_tool")
        self.assertEqual(tool_span.attributes["gen_ai.tool.name"], "safe_lookup")
        self.assertEqual(tool_span.attributes["gen_ai.tool.call.id"], "call-1")
        self.assertTrue(tool_span.ended)
        handoff_span = telemetry.spans[3]
        self.assertEqual(
            handoff_span.attributes["agentstorm.handoff.target_agent"], "answer-agent"
        )
        self.assertTrue(handoff_span.ended)
        rendered = repr([(span.attributes, span.errors) for span in telemetry.spans])
        self.assertNotIn("private tool arguments", rendered)
        self.assertNotIn("private tool result", rendered)

    async def test_unfinished_tool_span_closes_with_provider_error_type(self) -> None:
        telemetry = RecordingTelemetry()
        runner = WorkloadRunner(test_config(), InterruptedToolAdapter(), telemetry=telemetry)

        results, _ = await runner.execute(
            [TestCase(case_id="case-1", prompt="private prompt")]
        )

        self.assertFalse(results[0].success)
        tool_span = next(span for span in telemetry.spans if span.name == "gen_ai.execute_tool")
        self.assertEqual(tool_span.errors, ["RuntimeError"])
        self.assertTrue(tool_span.ended)
        self.assertNotIn("private tool failure", repr(tool_span))


if __name__ == "__main__":
    unittest.main()
