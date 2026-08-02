from __future__ import annotations

from .base import AgentAdapter, AgentLifecycle, HandoffLifecycleEvent, ToolLifecycleEvent
from .fake import FakeAdapter


def create_adapter(provider: str, model: str = "", base_url: str = "") -> AgentAdapter:
    if provider == "fake":
        return FakeAdapter()
    if provider == "openai-agents":
        from .openai_agents import OpenAIAgentsAdapter

        return OpenAIAgentsAdapter(model=model, base_url=base_url)
    raise ValueError(f"unsupported provider: {provider}")


__all__ = [
    "AgentAdapter",
    "AgentLifecycle",
    "FakeAdapter",
    "HandoffLifecycleEvent",
    "ToolLifecycleEvent",
    "create_adapter",
]
