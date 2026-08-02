from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .models import AssertionSpec, TestCase


@dataclass(frozen=True)
class PricingConfig:
    input_usd_per_million_tokens: str
    output_usd_per_million_tokens: str


@dataclass(frozen=True)
class TargetConfig:
    provider: str
    model: str = ""
    base_url: str = ""
    pricing: PricingConfig | None = None


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
class TelemetryRedactionConfig:
    patterns: tuple[str, ...] = ()
    metadata_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class TelemetryConfig:
    content_mode: str = "omit"
    redaction: TelemetryRedactionConfig = field(default_factory=TelemetryRedactionConfig)


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
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)


def load_run_config(path: str | Path) -> RunConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    run_id = str(raw.get("run_id") or "local-run")
    source = raw.get("source", {})
    dataset = raw.get("dataset", {})
    target = raw.get("target", {})
    workload = raw.get("workload", {})
    evaluation = raw.get("evaluation", {})
    telemetry = raw.get("telemetry", {})
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
            pricing=_pricing(target.get("pricing")),
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
        telemetry=_telemetry(telemetry),
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


_PRICE_PATTERN = re.compile(r"^(0|[1-9][0-9]{0,17})(\.[0-9]{1,12})?$")


def _pricing(value: Any) -> PricingConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("target.pricing must be an object")
    input_price = value.get("inputUSDPerMillionTokens")
    output_price = value.get("outputUSDPerMillionTokens")
    if not isinstance(input_price, str) or not _PRICE_PATTERN.fullmatch(input_price):
        raise ValueError("target.pricing.inputUSDPerMillionTokens must be a non-negative decimal")
    if not isinstance(output_price, str) or not _PRICE_PATTERN.fullmatch(output_price):
        raise ValueError("target.pricing.outputUSDPerMillionTokens must be a non-negative decimal")
    return PricingConfig(input_price, output_price)


def _telemetry(value: Any) -> TelemetryConfig:
    if value is None:
        value = {}
    if not isinstance(value, dict):
        raise ValueError("telemetry must be an object")
    content_mode = value.get("contentMode", value.get("content_mode", "omit"))
    if content_mode not in {"omit", "redacted"}:
        raise ValueError("telemetry.contentMode must be omit or redacted")
    redaction = value.get("redaction", {})
    if redaction is None:
        redaction = {}
    if not isinstance(redaction, dict):
        raise ValueError("telemetry.redaction must be an object")
    patterns = redaction.get("patterns", [])
    metadata_keys = redaction.get("metadataKeys", redaction.get("metadata_keys", []))
    if not isinstance(patterns, list) or len(patterns) > 20:
        raise ValueError("telemetry.redaction.patterns must be an array of at most 20 entries")
    parsed_patterns: list[str] = []
    for index, pattern in enumerate(patterns):
        if not isinstance(pattern, str) or len(pattern.encode("utf-8")) > 256:
            raise ValueError(
                f"telemetry.redaction.patterns[{index}] must be a string of at most 256 bytes"
            )
        try:
            re.compile(pattern)
        except re.error:
            raise ValueError(
                f"telemetry.redaction.patterns[{index}] is invalid"
            ) from None
        parsed_patterns.append(pattern)
    if not isinstance(metadata_keys, list) or not all(
        isinstance(key, str) and key for key in metadata_keys
    ):
        raise ValueError("telemetry.redaction.metadataKeys must be an array of non-empty strings")
    return TelemetryConfig(
        content_mode=str(content_mode),
        redaction=TelemetryRedactionConfig(
            patterns=tuple(parsed_patterns),
            metadata_keys=tuple(metadata_keys),
        ),
    )


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
