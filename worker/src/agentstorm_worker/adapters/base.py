from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..models import AdapterResponse, TestCase


@dataclass(frozen=True)
class ToolLifecycleEvent:
    """Content-free metadata for one provider tool invocation."""

    invocation_id: str
    name: str
    tool_type: str = "function"
    call_id: str | None = None
    agent_name: str | None = None


@dataclass(frozen=True)
class HandoffLifecycleEvent:
    """Content-free metadata for one agent handoff."""

    source_agent: str
    target_agent: str


class AgentLifecycle(Protocol):
    def tool_started(self, event: ToolLifecycleEvent) -> None: ...

    def tool_finished(self, event: ToolLifecycleEvent, error_type: str = "") -> None: ...

    def handed_off(self, event: HandoffLifecycleEvent) -> None: ...


class AgentAdapter(Protocol):
    async def run(
        self, case: TestCase, lifecycle: AgentLifecycle | None = None
    ) -> AdapterResponse:
        """Execute one test case."""
