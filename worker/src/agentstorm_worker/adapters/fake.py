from __future__ import annotations

import asyncio

from ..models import AdapterResponse, TestCase
from .base import AgentLifecycle, HandoffLifecycleEvent, ToolLifecycleEvent


class FakeAdapter:
    """Deterministic adapter used by tests and no-cost demos."""

    def __init__(self, delay_seconds: float = 0.001) -> None:
        self._delay_seconds = delay_seconds

    async def run(
        self, case: TestCase, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        tool_event = ToolLifecycleEvent(
            invocation_id="fake-echo",
            name="fake.echo",
            agent_name="fake-router",
            arguments={"text": case.prompt},
        )
        if lifecycle is not None:
            lifecycle.tool_started(tool_event)
        await asyncio.sleep(self._delay_seconds)
        output = f"fake response: {case.prompt}"
        if lifecycle is not None:
            lifecycle.tool_finished(
                ToolLifecycleEvent(
                    invocation_id=tool_event.invocation_id,
                    name=tool_event.name,
                    agent_name=tool_event.agent_name,
                    arguments=tool_event.arguments,
                    result={"text": output},
                )
            )
            lifecycle.handed_off(
                HandoffLifecycleEvent(
                    source_agent="fake-router",
                    target_agent="fake-responder",
                )
            )
        return AdapterResponse(output=output, input_tokens=11, output_tokens=7)
