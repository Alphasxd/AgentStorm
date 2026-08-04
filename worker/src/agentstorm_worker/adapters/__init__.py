from __future__ import annotations

import importlib
import inspect

from .base import (
    AdapterFactoryContext,
    AgentAdapter,
    AgentLifecycle,
    HandoffLifecycleEvent,
    ToolLifecycleEvent,
)
from .fake import FakeAdapter


class AdapterPluginError(RuntimeError):
    """A sanitized trusted-adapter loading failure."""


def create_adapter(
    provider: str,
    model: str = "",
    base_url: str = "",
    adapter_entrypoint: str = "",
) -> AgentAdapter:
    if adapter_entrypoint:
        return _create_plugin_adapter(
            adapter_entrypoint,
            AdapterFactoryContext(provider=provider, model=model, base_url=base_url),
        )
    if provider == "fake":
        return FakeAdapter()
    if provider == "openai-agents":
        from .openai_agents import OpenAIAgentsAdapter

        return OpenAIAgentsAdapter(model=model, base_url=base_url)
    raise ValueError(f"unsupported provider: {provider}")


def _create_plugin_adapter(entrypoint: str, context: AdapterFactoryContext) -> AgentAdapter:
    try:
        module_name, function_name = entrypoint.split(":", 1)
        factory = getattr(importlib.import_module(module_name), function_name, None)
    except Exception as exc:
        raise AdapterPluginError("adapter plugin import failed") from exc
    if not callable(factory):
        raise AdapterPluginError("adapter plugin factory is not callable")
    try:
        adapter = factory(context)
    except Exception as exc:
        raise AdapterPluginError("adapter plugin factory failed") from exc
    run = getattr(adapter, "run", None)
    if adapter is None or not callable(run) or not inspect.iscoroutinefunction(run):
        raise AdapterPluginError("adapter plugin factory returned an invalid adapter")
    return adapter


__all__ = [
    "AdapterFactoryContext",
    "AdapterPluginError",
    "AgentAdapter",
    "AgentLifecycle",
    "FakeAdapter",
    "HandoffLifecycleEvent",
    "ToolLifecycleEvent",
    "create_adapter",
]
