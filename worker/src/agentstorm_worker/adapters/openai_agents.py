from __future__ import annotations

import itertools
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from ..execution_errors import classify_adapter_exception
from ..models import AdapterResponse, TestCase
from .base import AgentLifecycle, HandoffLifecycleEvent, ToolLifecycleEvent


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _named(value: object, fallback: str) -> str:
    return _optional_string(getattr(value, "name", None)) or fallback


def _lifecycle_hooks(base: type[Any], lifecycle: AgentLifecycle) -> object:
    class LifecycleHooks(base):
        def __init__(self) -> None:
            super().__init__()
            self._sequence = itertools.count(1)
            self._tools: dict[tuple[int, int], ToolLifecycleEvent] = {}

        async def on_tool_start(self, context: object, agent: object, tool: object) -> None:
            call_id = _optional_string(getattr(context, "tool_call_id", None))
            event = ToolLifecycleEvent(
                invocation_id=call_id or f"local-tool-{next(self._sequence)}",
                name=_named(tool, type(tool).__name__),
                call_id=call_id,
                agent_name=_named(agent, type(agent).__name__),
            )
            self._tools[(id(context), id(tool))] = event
            lifecycle.tool_started(event)

        async def on_tool_end(
            self, context: object, agent: object, tool: object, result: object
        ) -> None:
            key = (id(context), id(tool))
            event = self._tools.pop(key, None)
            if event is None:
                call_id = _optional_string(getattr(context, "tool_call_id", None))
                event = ToolLifecycleEvent(
                    invocation_id=call_id or f"local-tool-{next(self._sequence)}",
                    name=_named(tool, type(tool).__name__),
                    call_id=call_id,
                    agent_name=_named(agent, type(agent).__name__),
                )
                lifecycle.tool_started(event)
            lifecycle.tool_finished(replace(event, result=result))

        async def on_handoff(self, context: object, from_agent: object, to_agent: object) -> None:
            del context
            lifecycle.handed_off(
                HandoffLifecycleEvent(
                    source_agent=_named(from_agent, type(from_agent).__name__),
                    target_agent=_named(to_agent, type(to_agent).__name__),
                )
            )

    return LifecycleHooks()


class OpenAIAgentsAdapter:
    """Thin adapter around the optional OpenAI Agents SDK."""

    def __init__(
        self,
        model: str,
        base_url: str = "",
        *,
        name: str = "AgentStorm target",
        instructions: str = "",
        tools: list[object] | None = None,
        output_type: object | None = None,
        model_settings: object | None = None,
        serialize_output: Callable[[object], str] = str,
    ) -> None:
        if not model:
            raise ValueError("target.model is required for provider openai-agents")
        try:
            from agents import Agent, OpenAIProvider, RunConfig, RunHooks
        except ImportError as exc:
            raise RuntimeError(
                "openai-agents is not installed; install agentstorm-worker[openai]"
            ) from exc
        agent_options: dict[str, object] = {
            "name": name,
            "instructions": instructions
            or (
                "Complete the supplied task accurately. Use concise answers and do not invent "
                "tool results."
            ),
            "model": model,
        }
        if tools is not None:
            agent_options["tools"] = tools
        if output_type is not None:
            agent_options["output_type"] = output_type
        if model_settings is not None:
            agent_options["model_settings"] = model_settings
        self._agent = Agent(**agent_options)
        run_config: dict[str, object] = {"trace_include_sensitive_data": False}
        if base_url:
            run_config["model_provider"] = OpenAIProvider(base_url=base_url)
        self._run_config = RunConfig(**run_config)
        self._run_hooks = RunHooks
        self._serialize_output = serialize_output

    async def run(self, case: TestCase, lifecycle: AgentLifecycle | None = None) -> AdapterResponse:
        from agents import Runner

        run_options = {"run_config": self._run_config}
        if lifecycle is not None:
            run_options["hooks"] = _lifecycle_hooks(self._run_hooks, lifecycle)
        try:
            result = await Runner.run(self._agent, case.prompt, **run_options)
        except Exception as exc:
            raise classify_adapter_exception(exc) from exc
        usage = result.context_wrapper.usage
        requests = getattr(usage, "requests", None)
        return AdapterResponse(
            output=self._serialize_output(result.final_output),
            input_tokens=int(usage.input_tokens),
            output_tokens=int(usage.output_tokens),
            model_call_count=int(requests) if isinstance(requests, int) else None,
        )
