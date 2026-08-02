from __future__ import annotations

import asyncio
import sys
import types
import unittest
from unittest.mock import patch

from agentstorm_worker.adapters import AgentLifecycle, HandoffLifecycleEvent, ToolLifecycleEvent
from agentstorm_worker.adapters.openai_agents import OpenAIAgentsAdapter
from agentstorm_worker.models import TestCase


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
                usage = types.SimpleNamespace(input_tokens=11, output_tokens=7)
                return types.SimpleNamespace(
                    final_output="agent response",
                    context_wrapper=types.SimpleNamespace(usage=usage),
                )

        agents.Agent = Agent
        agents.OpenAIProvider = OpenAIProvider
        agents.RunConfig = RunConfig
        agents.RunHooks = RunHooks
        agents.Runner = Runner

        with patch.dict(sys.modules, {"agents": agents}):
            adapter = OpenAIAgentsAdapter("model-test", "https://provider.example/v1")
            lifecycle: AgentLifecycle = RecordingLifecycle()
            response = asyncio.run(
                adapter.run(TestCase(case_id="1", prompt="hello"), lifecycle=lifecycle)
            )

        self.assertEqual(
            observed["provider_args"], {"base_url": "https://provider.example/v1"}
        )
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


if __name__ == "__main__":
    unittest.main()
