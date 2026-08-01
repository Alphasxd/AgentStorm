from __future__ import annotations

import asyncio
import sys
import types
import unittest
from unittest.mock import patch

from agentstorm_worker.adapters.openai_agents import OpenAIAgentsAdapter
from agentstorm_worker.models import TestCase


class OpenAIAgentsAdapterTest(unittest.TestCase):
    def test_forwards_base_url_and_collects_usage(self) -> None:
        agents = types.ModuleType("agents")
        observed: dict[str, object] = {}

        class Agent:
            def __init__(self, **kwargs: object) -> None:
                observed["agent"] = kwargs

        class OpenAIProvider:
            def __init__(self, **kwargs: object) -> None:
                observed["provider"] = kwargs

        class RunConfig:
            def __init__(self, **kwargs: object) -> None:
                observed["run_config"] = kwargs

        class Runner:
            @staticmethod
            async def run(agent: object, prompt: str, **kwargs: object) -> object:
                observed["prompt"] = prompt
                observed["run_options"] = kwargs
                usage = types.SimpleNamespace(input_tokens=11, output_tokens=7)
                return types.SimpleNamespace(
                    final_output="agent response",
                    context_wrapper=types.SimpleNamespace(usage=usage),
                )

        agents.Agent = Agent
        agents.OpenAIProvider = OpenAIProvider
        agents.RunConfig = RunConfig
        agents.Runner = Runner

        with patch.dict(sys.modules, {"agents": agents}):
            adapter = OpenAIAgentsAdapter("model-test", "https://provider.example/v1")
            response = asyncio.run(adapter.run(TestCase(case_id="1", prompt="hello")))

        self.assertEqual(observed["provider"], {"base_url": "https://provider.example/v1"})
        self.assertEqual(observed["prompt"], "hello")
        self.assertIn("run_config", observed["run_options"])
        self.assertEqual(response.output, "agent response")
        self.assertEqual(response.input_tokens, 11)
        self.assertEqual(response.output_tokens, 7)


if __name__ == "__main__":
    unittest.main()
