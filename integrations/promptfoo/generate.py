#!/usr/bin/env python3
"""Generate a Promptfoo config that replays one completed AgentStorm run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from .client import ResultAPIClient, ResultAPIError, read_token_from_environment
except ImportError:
    from client import ResultAPIClient, ResultAPIError, read_token_from_environment


def load_dataset(path: Path) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            case = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ResultAPIError(f"invalid dataset JSONL at line {line_number}") from exc
        if not isinstance(case, dict):
            raise ResultAPIError(f"dataset line {line_number} must be an object")
        case_id = str(case.get("id") or f"case-{line_number}")
        if case_id in cases:
            raise ResultAPIError(f"duplicate dataset case ID {case_id!r}")
        cases[case_id] = case
    if not cases:
        raise ResultAPIError("dataset contains no cases")
    return cases


def effective_assertions(case: dict[str, Any]) -> list[dict[str, Any]]:
    assertions: list[dict[str, Any]] = []
    legacy = case.get("expected_contains")
    if legacy is not None:
        if not isinstance(legacy, str):
            raise ResultAPIError("expected_contains must be a string")
        assertions.append({"type": "contains", "value": legacy})
    declared = case.get("assertions", [])
    if not isinstance(declared, list) or not all(isinstance(item, dict) for item in declared):
        raise ResultAPIError("assertions must be an array of objects")
    assertions.extend(declared)
    return assertions


def promptfoo_assertions(case: dict[str, Any]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []
    for assertion in effective_assertions(case):
        assertion_type = assertion.get("type")
        if assertion_type == "exact":
            mapped.append({"type": "equals", "value": assertion.get("value")})
        elif assertion_type == "contains":
            mapped.append({"type": "contains", "value": assertion.get("value")})
        elif assertion_type == "regex":
            mapped.append({"type": "regex", "value": assertion.get("pattern")})
        elif assertion_type == "json_schema":
            mapped.append({"type": "is-json", "value": assertion.get("schema")})
    return mapped


def safe_outcomes(case: dict[str, Any]) -> list[dict[str, Any]]:
    outcomes = case.get("assertions", [])
    if not isinstance(outcomes, list):
        return []
    safe: list[dict[str, Any]] = []
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue
        safe.append(
            {
                "index": outcome.get("index"),
                "type": outcome.get("type"),
                "passed": outcome.get("passed"),
                "reason_code": outcome.get("reason_code"),
            }
        )
    return safe


def build_config(
    result_api_url: str,
    run_id: str,
    dataset: dict[str, dict[str, Any]],
    result_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    provider_path = Path(__file__).with_name("provider.py").resolve().as_uri()
    tests: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for result in result_cases:
        case_id = result.get("case_id")
        iteration = result.get("iteration")
        if (
            not isinstance(case_id, str)
            or not isinstance(iteration, int)
            or isinstance(iteration, bool)
        ):
            raise ResultAPIError("durable result has an invalid case identity")
        identity = (case_id, iteration)
        if identity in seen:
            raise ResultAPIError(f"durable results repeat {case_id!r} iteration {iteration}")
        seen.add(identity)
        source = dataset.get(case_id)
        if source is None:
            raise ResultAPIError(f"durable result references unknown dataset case {case_id!r}")
        tests.append(
            {
                "vars": {
                    "agentstorm_case_id": case_id,
                    "agentstorm_iteration": iteration,
                },
                "assert": promptfoo_assertions(source),
                "metadata": {
                    "agentstorm": {
                        "run_id": run_id,
                        "case_id": case_id,
                        "iteration": iteration,
                        "latency_ms": result.get("latency_ms"),
                        "tool_path": result.get("tool_path", []),
                        "assertions": safe_outcomes(result),
                    }
                },
            }
        )
    return {
        "description": f"AgentStorm durable replay for {run_id}",
        "prompts": [
            "AgentStorm replay {{agentstorm_case_id}}#{{agentstorm_iteration}}"
        ],
        "providers": [
            {
                "id": provider_path,
                "label": "AgentStorm replay",
                "config": {"resultApiUrl": result_api_url, "runId": run_id},
            }
        ],
        "tests": tests,
    }


def generate(result_api_url: str, run_id: str, dataset_path: Path, output_path: Path) -> None:
    token = read_token_from_environment()
    client = ResultAPIClient(result_api_url, token)
    _, cases = client.completed_replay(run_id)
    config = build_config(result_api_url, run_id, load_dataset(dataset_path), cases)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    temporary.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-api-url", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        generate(args.result_api_url, args.run_id, args.dataset, args.output)
    except (ResultAPIError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
