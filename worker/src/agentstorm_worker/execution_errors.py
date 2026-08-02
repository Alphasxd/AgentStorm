from __future__ import annotations

import asyncio

from .models import AdapterResponse


class ExecutionError(RuntimeError):
    """A content-free, stable execution failure used across adapters and the harness."""

    def __init__(
        self,
        category: str,
        code: str,
        failure_kind: str,
        *,
        ambiguous: bool = False,
        safe_to_retry: bool = False,
        response: AdapterResponse | None = None,
        injected_rule: str = "",
        injected_fault: str = "",
    ) -> None:
        super().__init__(f"{category}/{code}")
        self.category = category
        self.code = code
        self.failure_kind = failure_kind
        self.ambiguous = ambiguous
        self.safe_to_retry = safe_to_retry
        self.response = response
        self.injected_rule = injected_rule
        self.injected_fault = injected_fault


def classify_adapter_exception(exc: Exception) -> ExecutionError:
    if isinstance(exc, ExecutionError):
        return exc
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return ExecutionError("provider", "timeout", "timeout", ambiguous=True)

    mapped = _classify_agents_exception(exc)
    if mapped is not None:
        return mapped
    mapped = _classify_openai_exception(exc)
    if mapped is not None:
        return mapped
    return ExecutionError("provider", "unknown_error", "provider", ambiguous=True)


def _classify_agents_exception(exc: Exception) -> ExecutionError | None:
    try:
        from agents.exceptions import (
            AgentsException,
            InputGuardrailTripwireTriggered,
            MaxTurnsExceeded,
            ModelBehaviorError,
            OutputGuardrailTripwireTriggered,
            ToolTimeoutError,
            UserError,
        )
    except ImportError:
        return None
    if isinstance(exc, ToolTimeoutError):
        return ExecutionError("tool", "tool_timeout", "tool", ambiguous=True)
    if isinstance(exc, InputGuardrailTripwireTriggered):
        return ExecutionError("evaluation", "input_guardrail_tripwire", "assertion")
    if isinstance(exc, OutputGuardrailTripwireTriggered):
        return ExecutionError("evaluation", "output_guardrail_tripwire", "assertion")
    if isinstance(exc, MaxTurnsExceeded):
        return ExecutionError("evaluation", "max_turns_exceeded", "assertion")
    if isinstance(exc, ModelBehaviorError):
        return ExecutionError("provider", "model_behavior", "provider", ambiguous=True)
    if isinstance(exc, UserError):
        return ExecutionError("harness", "sdk_user_error", "harness")
    if isinstance(exc, AgentsException):
        return ExecutionError("provider", "agents_error", "provider", ambiguous=True)
    return None


def _classify_openai_exception(exc: Exception) -> ExecutionError | None:
    try:
        from openai import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError
    except ImportError:
        return None
    if isinstance(exc, RateLimitError):
        return ExecutionError("provider", "rate_limited", "provider", safe_to_retry=True)
    if isinstance(exc, APITimeoutError):
        return ExecutionError("provider", "timeout", "timeout", ambiguous=True)
    if isinstance(exc, APIConnectionError):
        return ExecutionError("provider", "connection_error", "provider", ambiguous=True)
    if isinstance(exc, APIStatusError):
        status_code = int(exc.status_code)
        if status_code == 429:
            return ExecutionError("provider", "rate_limited", "provider", safe_to_retry=True)
        if status_code >= 500:
            return ExecutionError("provider", "http_5xx", "provider", ambiguous=True)
        return ExecutionError("provider", "http_4xx", "provider")
    return None
