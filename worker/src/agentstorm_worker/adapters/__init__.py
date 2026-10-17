from __future__ import annotations

import importlib
import inspect
from typing import Any

from .base import (
    AdapterFactoryContext,
    AgentAdapter,
    AgentLifecycle,
    HandoffLifecycleEvent,
    ToolLifecycleEvent,
)
from .fake import FakeAdapter
from .openai_compatible import OpenAICompatibleAdapter


class AdapterPluginError(RuntimeError):
    """A sanitized trusted-adapter loading failure."""


def create_adapter(
    provider: str,
    model: str = "",
    base_url: str = "",
    adapter_entrypoint: str = "",
    max_output_tokens: int | None = None,
    request_fields: dict[str, Any] | None = None,
    http_retry_attempts: int | None = None,
    http_retry_max_wait_seconds: int | None = None,
) -> AgentAdapter:
    context = AdapterFactoryContext(
        provider=provider,
        model=model,
        base_url=base_url,
        max_output_tokens=max_output_tokens,
        request_fields=dict(request_fields or {}),
        http_retry_attempts=http_retry_attempts,
        http_retry_max_wait_seconds=http_retry_max_wait_seconds,
    )
    if adapter_entrypoint:
        return _create_plugin_adapter(adapter_entrypoint, context)
    if provider == "fake":
        return FakeAdapter()
    if provider == "openai-compatible":
        from .openai_compatible import (
            http_retry_adapter_kwargs,
            validate_max_output_tokens,
            validate_request_fields,
        )

        fields = validate_request_fields(
            context.request_fields, reject_keys={"temperature", "max_tokens"}
        )
        if context.max_output_tokens is not None:
            fields["max_tokens"] = validate_max_output_tokens(context.max_output_tokens)
        return OpenAICompatibleAdapter(
            model=model,
            base_url=base_url,
            request_fields=fields,
            **http_retry_adapter_kwargs(
                context.http_retry_attempts, context.http_retry_max_wait_seconds
            ),
        )
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
    "OpenAICompatibleAdapter",
    "ToolLifecycleEvent",
    "create_adapter",
]
