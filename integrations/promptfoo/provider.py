"""Promptfoo Python provider for deterministic AgentStorm result replay."""

from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any

try:
    from .client import ResultAPIClient, ResultAPIError, read_token_from_environment
except ImportError:
    from client import ResultAPIClient, ResultAPIError, read_token_from_environment


_CASE_CACHE: dict[tuple[str, str, str], dict[tuple[str, int], dict[str, Any]]] = {}


def call_api(
    prompt: str, options: dict[str, Any], context: dict[str, Any]
) -> dict[str, Any]:
    del prompt
    try:
        config = options.get("config", {})
        if not isinstance(config, dict):
            raise ResultAPIError("Promptfoo provider config must be an object")
        result_api_url = config.get("resultApiUrl")
        run_id = config.get("runId")
        if not isinstance(result_api_url, str) or not isinstance(run_id, str):
            raise ResultAPIError("Promptfoo provider requires resultApiUrl and runId")
        variables = context.get("vars", {})
        if not isinstance(variables, dict):
            raise ResultAPIError("Promptfoo context vars must be an object")
        case_id = variables.get("agentstorm_case_id")
        iteration = variables.get("agentstorm_iteration")
        if (
            not isinstance(case_id, str)
            or not isinstance(iteration, int)
            or isinstance(iteration, bool)
        ):
            raise ResultAPIError("Promptfoo test requires case ID and integer iteration vars")
        token = read_token_from_environment()
        cases = _cached_cases(result_api_url, run_id, token)
        result = cases.get((case_id, iteration))
        if result is None:
            raise ResultAPIError(
                f"run {run_id!r} has no result for {case_id!r} iteration {iteration}"
            )
        return replay_response(run_id, result)
    except ResultAPIError as exc:
        return {"output": "", "error": str(exc)}


def _cached_cases(
    result_api_url: str, run_id: str, token: str
) -> dict[tuple[str, int], dict[str, Any]]:
    cache_key = (
        result_api_url,
        run_id,
        hashlib.sha256(token.encode("utf-8")).hexdigest(),
    )
    cached = _CASE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    client = ResultAPIClient(result_api_url, token)
    _, results = client.completed_replay(run_id)
    indexed: dict[tuple[str, int], dict[str, Any]] = {}
    for result in results:
        case_id = result.get("case_id")
        iteration = result.get("iteration")
        if (
            not isinstance(case_id, str)
            or not isinstance(iteration, int)
            or isinstance(iteration, bool)
        ):
            raise ResultAPIError("durable result has an invalid case identity")
        identity = (case_id, iteration)
        if identity in indexed:
            raise ResultAPIError(
                f"durable results repeat {case_id!r} iteration {iteration}"
            )
        indexed[identity] = result
    _CASE_CACHE[cache_key] = indexed
    return indexed


def replay_response(run_id: str, result: dict[str, Any]) -> dict[str, Any]:
    output = result.get("output")
    if not isinstance(output, str):
        raise ResultAPIError("durable result output must be a string")
    input_tokens = non_negative_int(result.get("input_tokens"), "input_tokens")
    output_tokens = non_negative_int(result.get("output_tokens"), "output_tokens")
    response: dict[str, Any] = {
        "output": output,
        "tokenUsage": {
            "prompt": input_tokens,
            "completion": output_tokens,
            "total": input_tokens + output_tokens,
            "numRequests": 0,
        },
        "metadata": {
            "agentstorm": {
                "replay": True,
                "run_id": run_id,
                "case_id": result.get("case_id"),
                "iteration": result.get("iteration"),
                "tool_path": result.get("tool_path", []),
                "assertions": safe_outcomes(result),
                "cost_usd": result.get("cost_usd"),
            }
        },
    }
    latency_ms = result.get("latency_ms")
    if isinstance(latency_ms, (int, float)) and not isinstance(latency_ms, bool):
        response["latencyMs"] = round(latency_ms)
    cost_usd = result.get("cost_usd")
    if cost_usd is not None:
        try:
            cost = Decimal(str(cost_usd))
        except InvalidOperation:
            raise ResultAPIError("durable result cost_usd is invalid") from None
        if not cost.is_finite() or cost < 0:
            raise ResultAPIError("durable result cost_usd is invalid")
        response["cost"] = float(cost)
    return response


def non_negative_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ResultAPIError(f"durable result {field} is invalid")
    return value


def safe_outcomes(case: dict[str, Any]) -> list[dict[str, Any]]:
    outcomes = case.get("assertions", [])
    if not isinstance(outcomes, list):
        return []
    return [
        {
            "index": outcome.get("index"),
            "type": outcome.get("type"),
            "passed": outcome.get("passed"),
            "reason_code": outcome.get("reason_code"),
        }
        for outcome in outcomes
        if isinstance(outcome, dict)
    ]


def clear_cache() -> None:
    """Testing hook; production users should not need to clear replay data."""
    _CASE_CACHE.clear()
