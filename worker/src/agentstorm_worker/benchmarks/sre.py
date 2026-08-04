from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
from dataclasses import replace
from importlib.resources import files
from typing import Any

from ..adapters.base import (
    AdapterFactoryContext,
    AgentAdapter,
    AgentLifecycle,
    ToolLifecycleEvent,
)
from ..models import AdapterResponse, TestCase

_FIXTURES = json.loads(
    files("agentstorm_worker.benchmarks").joinpath("sre_fixtures.json").read_text(encoding="utf-8")
)
_INCIDENTS: dict[str, dict[str, Any]] = {item["incident_id"]: item for item in _FIXTURES}
_TOOL_CALLS: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "agentstorm_sre_tool_calls", default=None
)


def _record(name: str, arguments: dict[str, Any]) -> None:
    calls = _TOOL_CALLS.get()
    if calls is not None:
        calls.append({"name": name, "arguments": dict(arguments)})


async def _deterministic_delay(key: str, tool_name: str) -> None:
    digest = hashlib.sha256(f"{key}\x00{tool_name}".encode()).digest()
    await asyncio.sleep((1 + digest[0] % 3) / 1000)


async def get_incident(incident_id: str) -> dict[str, Any]:
    _record("get_incident", {"incident_id": incident_id})
    await _deterministic_delay(incident_id, "get_incident")
    incident = _INCIDENTS.get(incident_id)
    if incident is None:
        raise ValueError("unknown synthetic incident")
    return {
        "incident_id": incident_id,
        "service": incident["service"],
        "window": incident["window"],
        "symptom": incident["symptom"],
    }


def _incident_for_service(service: str, window: str) -> dict[str, Any]:
    for incident in _INCIDENTS.values():
        if incident["service"] == service and incident["window"] == window:
            return incident
    raise ValueError("unknown synthetic service window")


async def query_metrics(service: str, window: str) -> dict[str, Any]:
    arguments = {"service": service, "window": window}
    _record("query_metrics", arguments)
    await _deterministic_delay(service, "query_metrics")
    incident = _incident_for_service(service, window)
    return {"service": service, "window": window, **incident["metrics"]}


async def search_logs(service: str, window: str) -> dict[str, Any]:
    arguments = {"service": service, "window": window}
    _record("search_logs", arguments)
    await _deterministic_delay(service, "search_logs")
    incident = _incident_for_service(service, window)
    return {
        "service": service,
        "window": window,
        "error_signature": incident["error_signature"],
        "entries": incident["log_entries"],
    }


async def lookup_runbook(error_signature: str) -> dict[str, str]:
    _record("lookup_runbook", {"error_signature": error_signature})
    await _deterministic_delay(error_signature, "lookup_runbook")
    for incident in _INCIDENTS.values():
        if incident["error_signature"] == error_signature:
            return {
                "error_signature": error_signature,
                "runbook": incident["runbook"],
                "recommended_action": incident["recommended_action"],
            }
    raise ValueError("unknown synthetic error signature")


def _diagnosis(incident: dict[str, Any]) -> dict[str, Any]:
    return {
        "incident_id": incident["incident_id"],
        "root_cause": incident["root_cause"],
        "severity": incident["severity"],
        "recommended_action": incident["recommended_action"],
        "evidence": [
            f"metrics:{incident['metrics']['summary']}",
            f"logs:{incident['error_signature']}",
        ],
        "runbook": incident["runbook"],
    }


class SREFakeAdapter:
    """Deterministically simulates the same four-tool lifecycle used by the real Agent."""

    async def run(self, case: TestCase, lifecycle: AgentLifecycle | None = None) -> AdapterResponse:
        incident_id = str(case.metadata.get("incident_id") or case.case_id)
        incident = _INCIDENTS.get(incident_id)
        if incident is None:
            raise ValueError("unknown synthetic incident")
        calls: list[dict[str, Any]] = []
        token = _TOOL_CALLS.set(calls)
        try:
            incident_result = await self._invoke(
                lifecycle,
                case.case_id,
                1,
                "get_incident",
                get_incident,
                {"incident_id": incident_id},
            )
            service = str(incident_result["service"])
            window = str(incident_result["window"])
            await self._invoke(
                lifecycle,
                case.case_id,
                2,
                "query_metrics",
                query_metrics,
                {"service": service, "window": window},
            )
            logs = await self._invoke(
                lifecycle,
                case.case_id,
                3,
                "search_logs",
                search_logs,
                {"service": service, "window": window},
            )
            await self._invoke(
                lifecycle,
                case.case_id,
                4,
                "lookup_runbook",
                lookup_runbook,
                {"error_signature": str(logs["error_signature"])},
            )
        finally:
            _TOOL_CALLS.reset(token)
        return AdapterResponse(
            output=json.dumps(_diagnosis(incident), sort_keys=True, separators=(",", ":")),
            input_tokens=240,
            output_tokens=96,
            model_call_count=5,
            tool_call_count=4,
            metadata={"agentstorm.sre.tool_calls": calls},
        )

    @staticmethod
    async def _invoke(
        lifecycle: AgentLifecycle | None,
        case_id: str,
        index: int,
        name: str,
        function: Any,
        arguments: dict[str, Any],
    ) -> Any:
        event = ToolLifecycleEvent(
            invocation_id=f"{case_id}-{index}",
            name=name,
            agent_name="sre-diagnostic-agent",
            arguments=arguments,
        )
        if lifecycle is not None:
            lifecycle.tool_started(event)
        result = await function(**arguments)
        if lifecycle is not None:
            lifecycle.tool_finished(replace(event, result=result))
        return result


class _SREOpenAIAdapter:
    def __init__(self, adapter: AgentAdapter) -> None:
        self._adapter = adapter

    async def run(self, case: TestCase, lifecycle: AgentLifecycle | None = None) -> AdapterResponse:
        calls: list[dict[str, Any]] = []
        token = _TOOL_CALLS.set(calls)
        try:
            response = await self._adapter.run(case, lifecycle=lifecycle)
        finally:
            _TOOL_CALLS.reset(token)
        metadata = dict(response.metadata)
        metadata["agentstorm.sre.tool_calls"] = calls
        return replace(response, metadata=metadata, tool_call_count=len(calls))


def _create_openai_adapter(context: AdapterFactoryContext) -> AgentAdapter:
    from agents import ModelSettings, function_tool
    from openai.types.shared import Reasoning
    from pydantic import BaseModel

    from ..adapters.openai_agents import OpenAIAgentsAdapter

    class SREDiagnosis(BaseModel):
        incident_id: str
        root_cause: str
        severity: str
        recommended_action: str
        evidence: list[str]
        runbook: str

    tools = [
        function_tool(get_incident),
        function_tool(query_metrics),
        function_tool(search_logs),
        function_tool(lookup_runbook),
    ]
    adapter = OpenAIAgentsAdapter(
        model=context.model,
        base_url=context.base_url,
        name="AgentStorm SRE diagnostic agent",
        instructions=(
            "Diagnose the supplied synthetic incident. Call get_incident first. Use its service "
            "and window for both query_metrics and search_logs; those two calls may be made in "
            "either order. Call lookup_runbook with the exact error_signature returned by logs. "
            "Do not repeat tools or call any other tool. Return only the structured diagnosis."
        ),
        tools=tools,
        output_type=SREDiagnosis,
        model_settings=ModelSettings(reasoning=Reasoning(effort="none"), parallel_tool_calls=True),
        serialize_output=lambda value: value.model_dump_json(),
    )
    return _SREOpenAIAdapter(adapter)


def create_adapter(context: AdapterFactoryContext) -> AgentAdapter:
    if context.provider == "fake":
        return SREFakeAdapter()
    if context.provider == "openai-agents":
        return _create_openai_adapter(context)
    raise ValueError("SRE benchmark supports fake or openai-agents providers")


def validate_tool_usage(context: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    del config
    case_id = str(context.get("case_id") or "")
    incident = _INCIDENTS.get(case_id)
    metadata = context.get("metadata")
    calls = metadata.get("agentstorm.sre.tool_calls") if isinstance(metadata, dict) else None
    if incident is None or not isinstance(calls, list):
        return {"passed": False, "message": "SRE tool evidence is missing"}
    names = [call.get("name") for call in calls if isinstance(call, dict)]
    if len(calls) != 4 or names[0:1] != ["get_incident"] or names[-1:] != ["lookup_runbook"]:
        return {"passed": False, "message": "tool count or boundary order is invalid"}
    if set(names[1:3]) != {"query_metrics", "search_logs"} or len(set(names)) != 4:
        return {"passed": False, "message": "metrics/log order or duplicate tools are invalid"}
    arguments = {call["name"]: call.get("arguments") for call in calls}
    expected_service_window = {
        "service": incident["service"],
        "window": incident["window"],
    }
    passed = (
        arguments["get_incident"] == {"incident_id": case_id}
        and arguments["query_metrics"] == expected_service_window
        and arguments["search_logs"] == expected_service_window
        and arguments["lookup_runbook"] == {"error_signature": incident["error_signature"]}
    )
    return {
        "passed": passed,
        "message": "tool arguments do not follow prior tool evidence" if not passed else "",
    }


__all__ = [
    "SREFakeAdapter",
    "create_adapter",
    "get_incident",
    "lookup_runbook",
    "query_metrics",
    "search_logs",
    "validate_tool_usage",
]
