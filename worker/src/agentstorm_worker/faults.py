from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace

from .adapters import AgentAdapter, AgentLifecycle, HandoffLifecycleEvent, ToolLifecycleEvent
from .config import FaultRuleConfig, FaultScenarioConfig
from .execution_errors import ExecutionError
from .models import AdapterResponse, TestCase


def select_fault_rule(
    scenario: FaultScenarioConfig | None,
    seed: int | None,
    case_id: str,
    iteration: int,
    attempt: int,
) -> FaultRuleConfig | None:
    if scenario is None or seed is None:
        return None
    for rule in scenario.rules:
        if rule.case_ids and case_id not in rule.case_ids:
            continue
        if rule.iterations and iteration not in rule.iterations:
            continue
        if attempt not in rule.attempts:
            continue
        material = "\x00".join(
            (scenario.digest, str(seed), case_id, str(iteration), str(attempt), rule.name)
        ).encode("utf-8")
        draw = int.from_bytes(hashlib.sha256(material).digest()[:8], "big") / 2**64
        if draw < rule.probability:
            return rule
    return None


class FaultInjectionMiddleware:
    def __init__(self, adapter: AgentAdapter, scenario: FaultScenarioConfig | None, seed: int | None):
        self._adapter = adapter
        self._scenario = scenario
        self._seed = seed

    async def run(
        self,
        case: TestCase,
        iteration: int,
        attempt: int,
        lifecycle: AgentLifecycle | None = None,
    ) -> AdapterResponse:
        rule = select_fault_rule(
            self._scenario, self._seed, case.case_id, iteration, attempt
        )
        if rule is None:
            return await self._adapter.run(case, lifecycle=lifecycle)
        if rule.fault == "latency":
            await asyncio.sleep((rule.delay_ms or 0) / 1000)
            response = await self._adapter.run(case, lifecycle=lifecycle)
            return _with_injection(response, rule)
        if rule.fault == "timeout":
            await asyncio.sleep((rule.delay_ms or 0) / 1000)
            raise _injected_error(
                rule,
                "provider",
                "timeout",
                "timeout",
                ambiguous=True,
                usage_complete=True,
            )
        if rule.fault == "rate_limit":
            raise _injected_error(
                rule,
                "provider",
                "rate_limited",
                "provider",
                safe_to_retry=True,
                usage_complete=True,
            )
        if rule.fault == "http_error":
            status = rule.status_code or 500
            code = "rate_limited" if status == 429 else ("http_5xx" if status >= 500 else "http_4xx")
            raise _injected_error(
                rule,
                "provider",
                code,
                "provider",
                ambiguous=status >= 500,
                safe_to_retry=status == 429,
                usage_complete=True,
            )
        wrapped_lifecycle = (
            _ToolFaultLifecycle(lifecycle, rule) if rule.fault == "tool_error" else lifecycle
        )
        response = await self._adapter.run(case, lifecycle=wrapped_lifecycle)
        if rule.fault == "malformed_response":
            raise _injected_error(
                rule,
                "provider",
                "malformed_response",
                "provider",
                ambiguous=True,
                response=response,
                usage_complete=(
                    response.input_tokens is not None and response.output_tokens is not None
                ),
            )
        return response


def _with_injection(response: AdapterResponse, rule: FaultRuleConfig) -> AdapterResponse:
    metadata = dict(response.metadata)
    metadata["agentstorm.injected_rule"] = rule.name
    metadata["agentstorm.injected_fault"] = rule.fault
    return replace(response, metadata=metadata)


def _injected_error(
    rule: FaultRuleConfig,
    category: str,
    code: str,
    failure_kind: str,
    *,
    ambiguous: bool = False,
    safe_to_retry: bool = False,
    response: AdapterResponse | None = None,
    usage_complete: bool = False,
) -> ExecutionError:
    return ExecutionError(
        category,
        code,
        failure_kind,
        ambiguous=ambiguous,
        safe_to_retry=safe_to_retry,
        response=response,
        injected_rule=rule.name,
        injected_fault=rule.fault,
        usage_complete=usage_complete,
    )


class _ToolFaultLifecycle(AgentLifecycle):
    def __init__(self, delegate: AgentLifecycle | None, rule: FaultRuleConfig) -> None:
        self._delegate = delegate
        self._rule = rule
        self._injected = False

    def tool_started(self, event: ToolLifecycleEvent) -> None:
        if self._delegate is not None:
            self._delegate.tool_started(event)
        if not self._injected and (
            not self._rule.tool_name or self._rule.tool_name == event.name
        ):
            self._injected = True
            raise _injected_error(
                self._rule, "tool", "injected_tool_error", "tool", ambiguous=True
            )

    def tool_finished(self, event: ToolLifecycleEvent, error_type: str = "") -> None:
        if self._delegate is not None:
            self._delegate.tool_finished(event, error_type)

    def handed_off(self, event: HandoffLifecycleEvent) -> None:
        if self._delegate is not None:
            self._delegate.handed_off(event)
