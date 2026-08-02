from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import AssertionSpec, TestCase


@dataclass(frozen=True)
class TargetConfig:
    provider: str
    model: str = ""
    base_url: str = ""


@dataclass(frozen=True)
class WorkloadConfig:
    concurrency: int = 1
    iterations: int = 1
    timeout_seconds: int = 900


@dataclass(frozen=True)
class EvaluationConfig:
    min_success_rate: float | None = None
    max_error_rate: float | None = None
    max_p95_latency_ms: int | None = None


@dataclass(frozen=True)
class SourceConfig:
    namespace: str
    name: str


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    key: str


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    source: SourceConfig
    dataset: DatasetConfig
    target: TargetConfig
    workload: WorkloadConfig
    evaluation: EvaluationConfig


def load_run_config(path: str | Path) -> RunConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    run_id = str(raw.get("run_id") or "local-run")
    source = raw.get("source", {})
    dataset = raw.get("dataset", {})
    target = raw.get("target", {})
    workload = raw.get("workload", {})
    evaluation = raw.get("evaluation", {})
    config = RunConfig(
        run_id=run_id,
        source=SourceConfig(
            namespace=str(source.get("namespace") or "local"),
            name=str(source.get("name") or run_id),
        ),
        dataset=DatasetConfig(
            name=str(dataset.get("name") or "local-dataset"),
            key=str(dataset.get("key") or "cases.jsonl"),
        ),
        target=TargetConfig(
            provider=str(target.get("provider") or "fake"),
            model=str(target.get("model") or ""),
            base_url=str(target.get("baseURL") or target.get("base_url") or ""),
        ),
        workload=WorkloadConfig(
            concurrency=int(workload.get("concurrency") or 1),
            iterations=int(workload.get("iterations") or 1),
            timeout_seconds=int(workload.get("timeout_seconds") or 900),
        ),
        evaluation=EvaluationConfig(
            min_success_rate=_optional_float(evaluation, "minSuccessRate", "min_success_rate"),
            max_error_rate=_optional_float(evaluation, "maxErrorRate", "max_error_rate"),
            max_p95_latency_ms=_optional_int(
                evaluation, "maxP95LatencyMs", "max_p95_latency_ms"
            ),
        ),
    )
    if config.workload.concurrency < 1 or config.workload.iterations < 1:
        raise ValueError("workload concurrency and iterations must be positive")
    if config.workload.timeout_seconds < 1:
        raise ValueError("workload timeout_seconds must be positive")
    return config


def load_dataset(path: str | Path) -> list[TestCase]:
    cases: list[TestCase] = []
    case_ids: set[str] = set()
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at line {line_number}: {exc}") from exc
        prompt = raw.get("input")
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(f"dataset line {line_number} requires a non-empty string input")
        case_id = str(raw.get("id") or f"case-{line_number}")
        if case_id in case_ids:
            raise ValueError(f"duplicate dataset case id {case_id!r} at line {line_number}")
        case_ids.add(case_id)
        cases.append(
            TestCase(
                case_id=case_id,
                prompt=prompt,
                expected_contains=_optional_string(raw.get("expected_contains")),
                assertions=_assertions(raw.get("assertions"), line_number),
                metadata=_metadata(raw),
            )
        )
    if not cases:
        raise ValueError("dataset contains no cases")
    return cases


def _optional_float(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in data and data[key] is not None:
            return float(data[key])
    return None


def _optional_int(data: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        if key in data and data[key] is not None:
            return int(data[key])
    return None


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _metadata(raw: dict[str, Any]) -> dict[str, Any]:
    metadata = raw.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _assertions(value: Any, line_number: int) -> tuple[AssertionSpec, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"dataset line {line_number} assertions must be an array")
    assertions: list[AssertionSpec] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ValueError(
                f"dataset line {line_number} assertions[{index}] must be an object"
            )
        assertion_type = raw.get("type")
        if assertion_type not in {
            "exact",
            "contains",
            "regex",
            "json_schema",
            "tool_path",
            "latency",
            "python",
        }:
            raise ValueError(
                f"dataset line {line_number} assertions[{index}] has unsupported type"
            )
        allowed = {
            "exact": {"type", "value"},
            "contains": {"type", "value"},
            "regex": {"type", "pattern"},
            "json_schema": {"type", "schema"},
            "tool_path": {"type", "path"},
            "latency": {"type", "max_ms"},
            "python": {"type", "entrypoint", "config"},
        }[assertion_type]
        unknown = set(raw) - allowed
        if unknown:
            raise ValueError(
                f"dataset line {line_number} assertions[{index}] has unknown fields"
            )
        assertions.append(_assertion_spec(raw, assertion_type, line_number, index))
    return tuple(assertions)


def _assertion_spec(
    raw: dict[str, Any], assertion_type: str, line_number: int, index: int
) -> AssertionSpec:
    location = f"dataset line {line_number} assertions[{index}]"
    if assertion_type in {"exact", "contains"}:
        if not isinstance(raw.get("value"), str):
            raise ValueError(f"{location} value must be a string")
        return AssertionSpec(type=assertion_type, value=raw["value"])
    if assertion_type == "regex":
        if not isinstance(raw.get("pattern"), str):
            raise ValueError(f"{location} pattern must be a string")
        return AssertionSpec(type=assertion_type, pattern=raw["pattern"])
    if assertion_type == "json_schema":
        if not isinstance(raw.get("schema"), (dict, bool)):
            raise ValueError(f"{location} schema must be an object or boolean")
        return AssertionSpec(type=assertion_type, schema=raw["schema"])
    if assertion_type == "tool_path":
        path = raw.get("path")
        if not isinstance(path, list) or not all(
            isinstance(item, str) and item for item in path
        ):
            raise ValueError(f"{location} path must be an array of non-empty strings")
        return AssertionSpec(type=assertion_type, path=tuple(path))
    if assertion_type == "latency":
        max_ms = raw.get("max_ms")
        if isinstance(max_ms, bool) or not isinstance(max_ms, (int, float)) or max_ms <= 0:
            raise ValueError(f"{location} max_ms must be positive")
        return AssertionSpec(type=assertion_type, max_ms=float(max_ms))
    entrypoint = raw.get("entrypoint")
    if (
        not isinstance(entrypoint, str)
        or entrypoint.count(":") != 1
        or not all(entrypoint.split(":"))
    ):
        raise ValueError(f"{location} entrypoint must use module:function")
    config = raw.get("config", {})
    if not isinstance(config, dict):
        raise ValueError(f"{location} config must be an object")
    return AssertionSpec(type=assertion_type, entrypoint=entrypoint, config=config)
