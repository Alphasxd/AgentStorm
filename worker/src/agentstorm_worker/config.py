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
class FaultRuleConfig:
    name: str
    fault: str
    probability: float
    case_ids: tuple[str, ...] = ()
    iterations: tuple[int, ...] = ()
    attempts: tuple[int, ...] = (1,)
    delay_ms: int | None = None
    status_code: int | None = None
    tool_name: str = ""


@dataclass(frozen=True)
class FaultScenarioConfig:
    source_name: str
    source_key: str
    digest: str
    rules: tuple[FaultRuleConfig, ...]


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 1
    initial_backoff_ms: int = 100
    max_backoff_ms: int = 2000
    max_cumulative_backoff_ms: int = 5000
    jitter_ratio: float = 0.2
    allow_ambiguous_retries: bool = False


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: int
    open_duration_ms: int


@dataclass(frozen=True)
class ReliabilityConfig:
    seed: int | None = None
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig | None = None
    scenario: FaultScenarioConfig | None = None


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
    reliability: ReliabilityConfig | None = None


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
        reliability=_reliability(raw.get("reliability")),
    )
    if config.workload.concurrency < 1 or config.workload.iterations < 1:
        raise ValueError("workload concurrency and iterations must be positive")
    if config.workload.timeout_seconds < 1:
        raise ValueError("workload timeout_seconds must be positive")
    return config


_FAULTS = {
    "latency",
    "timeout",
    "http_error",
    "malformed_response",
    "rate_limit",
    "tool_error",
}


def _reliability(value: Any) -> ReliabilityConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("reliability must be an object")
    _only_keys(value, {"seed", "retry", "circuitBreaker", "scenario"}, "reliability")
    seed = value.get("seed")
    if seed is not None and (not isinstance(seed, int) or isinstance(seed, bool) or seed < 0):
        raise ValueError("reliability.seed must be a non-negative integer")
    retry = _retry(value.get("retry", {}))
    breaker = _circuit_breaker(value.get("circuitBreaker"))
    scenario = _scenario_snapshot(value.get("scenario"))
    if scenario is not None and seed is None:
        raise ValueError("reliability.seed is required when a scenario is configured")
    return ReliabilityConfig(
        seed=seed,
        retry=retry,
        circuit_breaker=breaker,
        scenario=scenario,
    )


def _retry(value: Any) -> RetryConfig:
    if not isinstance(value, dict):
        raise ValueError("reliability.retry must be an object")
    _only_keys(
        value,
        {
            "maxAttempts",
            "initialBackoffMs",
            "maxBackoffMs",
            "maxCumulativeBackoffMs",
            "jitterRatio",
            "allowAmbiguousRetries",
        },
        "reliability.retry",
    )
    retry = RetryConfig(
        max_attempts=_strict_int(value.get("maxAttempts", 1), "maxAttempts"),
        initial_backoff_ms=_strict_int(value.get("initialBackoffMs", 100), "initialBackoffMs"),
        max_backoff_ms=_strict_int(value.get("maxBackoffMs", 2000), "maxBackoffMs"),
        max_cumulative_backoff_ms=_strict_int(
            value.get("maxCumulativeBackoffMs", 5000), "maxCumulativeBackoffMs"
        ),
        jitter_ratio=_strict_number(value.get("jitterRatio", 0.2), "jitterRatio"),
        allow_ambiguous_retries=value.get("allowAmbiguousRetries", False),
    )
    if not isinstance(retry.allow_ambiguous_retries, bool):
        raise ValueError("reliability.retry.allowAmbiguousRetries must be a boolean")
    if not 1 <= retry.max_attempts <= 10:
        raise ValueError("reliability.retry.maxAttempts must be between 1 and 10")
    if min(
        retry.initial_backoff_ms,
        retry.max_backoff_ms,
        retry.max_cumulative_backoff_ms,
    ) < 1:
        raise ValueError("reliability retry backoff values must be positive")
    if retry.initial_backoff_ms > retry.max_backoff_ms:
        raise ValueError("reliability.retry.initialBackoffMs cannot exceed maxBackoffMs")
    if not 0 <= retry.jitter_ratio <= 1:
        raise ValueError("reliability.retry.jitterRatio must be between 0 and 1")
    return retry


def _circuit_breaker(value: Any) -> CircuitBreakerConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("reliability.circuitBreaker must be an object")
    _only_keys(value, {"failureThreshold", "openDurationMs"}, "reliability.circuitBreaker")
    if set(value) != {"failureThreshold", "openDurationMs"}:
        raise ValueError("reliability.circuitBreaker requires failureThreshold and openDurationMs")
    config = CircuitBreakerConfig(
        failure_threshold=_strict_int(value["failureThreshold"], "failureThreshold"),
        open_duration_ms=_strict_int(value["openDurationMs"], "openDurationMs"),
    )
    if config.failure_threshold < 1 or config.open_duration_ms < 1:
        raise ValueError("reliability.circuitBreaker values must be positive")
    return config


def _scenario_snapshot(value: Any) -> FaultScenarioConfig | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("reliability.scenario must be an object")
    _only_keys(value, {"sourceName", "sourceKey", "digest", "scenario"}, "reliability.scenario")
    if set(value) != {"sourceName", "sourceKey", "digest", "scenario"}:
        raise ValueError("reliability.scenario snapshot is incomplete")
    source_name = _nonempty_string(value["sourceName"], "sourceName", 253)
    source_key = _nonempty_string(value["sourceKey"], "sourceKey", 253)
    digest = _nonempty_string(value["digest"], "digest", 71)
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("reliability.scenario.digest must be a sha256 digest")
    scenario = value["scenario"]
    if not isinstance(scenario, dict):
        raise ValueError("reliability.scenario.scenario must be an object")
    _only_keys(scenario, {"apiVersion", "kind", "rules"}, "FaultScenario")
    if scenario.get("apiVersion") != "agentstorm.io/v1alpha1" or scenario.get("kind") != "FaultScenario":
        raise ValueError("FaultScenario apiVersion or kind is invalid")
    raw_rules = scenario.get("rules")
    if not isinstance(raw_rules, list) or len(raw_rules) > 100:
        raise ValueError("FaultScenario.rules must be an array of at most 100 entries")
    rules = tuple(_fault_rule(rule, index) for index, rule in enumerate(raw_rules))
    names = [rule.name for rule in rules]
    if len(names) != len(set(names)):
        raise ValueError("FaultScenario rule names must be unique")
    return FaultScenarioConfig(source_name, source_key, digest, rules)


def _fault_rule(value: Any, index: int) -> FaultRuleConfig:
    path = f"FaultScenario.rules[{index}]"
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object")
    _only_keys(
        value,
        {
            "name",
            "fault",
            "probability",
            "caseIDs",
            "iterations",
            "attempts",
            "delayMs",
            "statusCode",
            "toolName",
        },
        path,
    )
    name = _nonempty_string(value.get("name"), f"{path}.name", 128)
    fault = value.get("fault")
    if fault not in _FAULTS:
        raise ValueError(f"{path}.fault is unsupported")
    probability = _strict_number(value.get("probability"), f"{path}.probability")
    if not 0 <= probability <= 1:
        raise ValueError(f"{path}.probability must be between 0 and 1")
    case_ids = _string_selector(value.get("caseIDs", []), f"{path}.caseIDs")
    iterations = _integer_selector(value.get("iterations", []), f"{path}.iterations", 0)
    attempts = _integer_selector(value.get("attempts", [1]), f"{path}.attempts", 1)
    if any(attempt > 10 for attempt in attempts):
        raise ValueError(f"{path}.attempts must be between 1 and 10")
    delay_ms = value.get("delayMs")
    status_code = value.get("statusCode")
    tool_name = value.get("toolName", "")
    if fault in {"latency", "timeout"}:
        delay_ms = _strict_int(delay_ms, f"{path}.delayMs")
        if delay_ms < 0:
            raise ValueError(f"{path}.delayMs must be non-negative")
        if status_code is not None or tool_name:
            raise ValueError(f"{path} has fields that do not apply to {fault}")
    elif fault == "http_error":
        status_code = _strict_int(status_code, f"{path}.statusCode")
        if not 400 <= status_code <= 599 or delay_ms is not None or tool_name:
            raise ValueError(f"{path}.statusCode must be between 400 and 599")
    elif fault == "tool_error":
        if not isinstance(tool_name, str) or len(tool_name.encode("utf-8")) > 512:
            raise ValueError(f"{path}.toolName must be a string of at most 512 bytes")
        if delay_ms is not None or status_code is not None:
            raise ValueError(f"{path} has fields that do not apply to tool_error")
    elif delay_ms is not None or status_code is not None or tool_name:
        raise ValueError(f"{path} has fields that do not apply to {fault}")
    return FaultRuleConfig(
        name=name,
        fault=fault,
        probability=probability,
        case_ids=case_ids,
        iterations=iterations,
        attempts=attempts,
        delay_ms=delay_ms,
        status_code=status_code,
        tool_name=tool_name,
    )


def _only_keys(value: dict[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{path} contains unknown field {unknown[0]!r}")


def _strict_int(value: Any, path: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{path} must be an integer")
    return value


def _strict_number(value: Any, path: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{path} must be a number")
    return float(value)


def _nonempty_string(value: Any, path: str, max_bytes: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{path} must be a non-empty string of at most {max_bytes} bytes")
    return value


def _string_selector(value: Any, path: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be an array")
    parsed = tuple(_nonempty_string(item, path, 512) for item in value)
    if len(parsed) != len(set(parsed)):
        raise ValueError(f"{path} must not contain duplicates")
    return parsed


def _integer_selector(value: Any, path: str, minimum: int) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be an array")
    parsed = tuple(_strict_int(item, path) for item in value)
    if any(item < minimum for item in parsed):
        raise ValueError(f"{path} values must be at least {minimum}")
    if len(parsed) != len(set(parsed)):
        raise ValueError(f"{path} must not contain duplicates")
    return parsed


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
