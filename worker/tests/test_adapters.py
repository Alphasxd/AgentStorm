from __future__ import annotations

import asyncio
import sys
import types
import unittest
from unittest.mock import patch

from agentstorm_worker.adapters import (
    AdapterFactoryContext,
    AdapterPluginError,
    AgentLifecycle,
    HandoffLifecycleEvent,
    ToolLifecycleEvent,
    create_adapter,
)
from agentstorm_worker.adapters.fake import FakeAdapter
from agentstorm_worker.adapters.openai_agents import OpenAIAgentsAdapter
from agentstorm_worker.models import TestCase

_plugin_context: AdapterFactoryContext | None = None


def valid_plugin(context: AdapterFactoryContext) -> FakeAdapter:
    global _plugin_context
    _plugin_context = context
    return FakeAdapter()


def failing_plugin(context: AdapterFactoryContext) -> FakeAdapter:
    del context
    raise RuntimeError("private plugin failure")


def invalid_plugin(context: AdapterFactoryContext) -> object:
    del context
    return object()


def synchronous_plugin(context: AdapterFactoryContext) -> object:
    del context

    class SynchronousAdapter:
        def run(self, case: TestCase) -> object:
            return case

    return SynchronousAdapter()


class RecordingLifecycle:
    def __init__(self) -> None:
        self.tools_started: list[ToolLifecycleEvent] = []
        self.tools_finished: list[tuple[ToolLifecycleEvent, str]] = []
        self.handoffs: list[HandoffLifecycleEvent] = []

    def tool_started(self, event: ToolLifecycleEvent) -> None:
        self.tools_started.append(event)

    def tool_finished(self, event: ToolLifecycleEvent, error_type: str = "") -> None:
        self.tools_finished.append((event, error_type))

    def handed_off(self, event: HandoffLifecycleEvent) -> None:
        self.handoffs.append(event)


class OpenAIAgentsAdapterTest(unittest.TestCase):
    def test_forwards_base_url_and_collects_usage(self) -> None:
        agents = types.ModuleType("agents")
        observed: dict[str, object] = {}

        class Agent:
            def __init__(self, **kwargs: object) -> None:
                observed["agent"] = kwargs

        class OpenAIProvider:
            def __init__(self, **kwargs: object) -> None:
                observed["provider_args"] = kwargs
                observed["provider_instance"] = self

        class RunConfig:
            def __init__(self, **kwargs: object) -> None:
                observed["run_config"] = kwargs

        class RunHooks:
            pass

        class Runner:
            @staticmethod
            async def run(agent: object, prompt: str, **kwargs: object) -> object:
                observed["prompt"] = prompt
                observed["run_options"] = kwargs
                hooks = kwargs.get("hooks")
                if hooks is not None:
                    context = types.SimpleNamespace(
                        tool_call_id="call-1",
                        tool_arguments='{"secret":"private tool arguments"}',
                    )
                    source = types.SimpleNamespace(name="triage-agent")
                    target = types.SimpleNamespace(name="answer-agent")
                    tool = types.SimpleNamespace(name="safe_lookup")
                    await hooks.on_tool_start(context, source, tool)
                    await hooks.on_tool_end(context, source, tool, "private tool result")
                    await hooks.on_handoff(context, source, target)
                usage = types.SimpleNamespace(input_tokens=11, output_tokens=7, requests=3)
                return types.SimpleNamespace(
                    final_output="agent response",
                    context_wrapper=types.SimpleNamespace(usage=usage),
                )

        agents.Agent = Agent
        agents.OpenAIProvider = OpenAIProvider
        agents.RunConfig = RunConfig
        agents.RunHooks = RunHooks
        agents.Runner = Runner

        model_settings = object()
        with patch.dict(sys.modules, {"agents": agents}):
            adapter = OpenAIAgentsAdapter(
                "model-test",
                "https://provider.example/v1",
                model_settings=model_settings,
            )
            lifecycle: AgentLifecycle = RecordingLifecycle()
            response = asyncio.run(
                adapter.run(TestCase(case_id="1", prompt="hello"), lifecycle=lifecycle)
            )

        self.assertEqual(observed["provider_args"], {"base_url": "https://provider.example/v1"})
        self.assertIs(observed["agent"]["model_settings"], model_settings)
        self.assertEqual(
            observed["run_config"],
            {
                "model_provider": observed["provider_instance"],
                "trace_include_sensitive_data": False,
            },
        )
        self.assertEqual(observed["prompt"], "hello")
        self.assertIn("run_config", observed["run_options"])
        self.assertEqual(response.output, "agent response")
        self.assertEqual(response.input_tokens, 11)
        self.assertEqual(response.output_tokens, 7)
        self.assertEqual(response.model_call_count, 3)
        assert isinstance(lifecycle, RecordingLifecycle)
        self.assertEqual(lifecycle.tools_started[0].name, "safe_lookup")
        self.assertEqual(lifecycle.tools_started[0].call_id, "call-1")
        self.assertEqual(len(lifecycle.tools_finished), 1)
        finished, error_type = lifecycle.tools_finished[0]
        self.assertEqual(error_type, "")
        self.assertEqual(finished.invocation_id, lifecycle.tools_started[0].invocation_id)
        self.assertIsNone(finished.arguments)
        self.assertEqual(finished.result, "private tool result")
        self.assertEqual(
            lifecycle.handoffs,
            [
                HandoffLifecycleEvent(
                    source_agent="triage-agent",
                    target_agent="answer-agent",
                )
            ],
        )
        rendered = repr((lifecycle.tools_started, lifecycle.tools_finished, lifecycle.handoffs))
        self.assertNotIn("private tool arguments", rendered)


class AdapterPluginTest(unittest.TestCase):
    def test_loads_trusted_factory_with_secret_free_context(self) -> None:
        adapter = create_adapter(
            "fake",
            model="model-test",
            base_url="https://provider.example/v1",
            adapter_entrypoint="test_adapters:valid_plugin",
        )
        self.assertIsInstance(adapter, FakeAdapter)
        self.assertEqual(
            _plugin_context,
            AdapterFactoryContext(
                provider="fake",
                model="model-test",
                base_url="https://provider.example/v1",
            ),
        )
        self.assertEqual(
            set(AdapterFactoryContext.__dataclass_fields__),
            {"provider", "model", "base_url"},
        )

    def test_rejects_import_factory_and_return_failures_without_details(self) -> None:
        for entrypoint, message in (
            ("missing_agentstorm_plugin:create", "import failed"),
            ("test_adapters:missing_factory", "not callable"),
            ("test_adapters:failing_plugin", "factory failed"),
            ("test_adapters:invalid_plugin", "invalid adapter"),
            ("test_adapters:synchronous_plugin", "invalid adapter"),
        ):
            with self.subTest(entrypoint=entrypoint):
                with self.assertRaisesRegex(AdapterPluginError, message) as raised:
                    create_adapter("fake", adapter_entrypoint=entrypoint)
                self.assertNotIn("private plugin failure", str(raised.exception))

    def test_builtin_adapter_remains_default(self) -> None:
        self.assertIsInstance(create_adapter("fake"), FakeAdapter)


if __name__ == "__main__":
    unittest.main()
