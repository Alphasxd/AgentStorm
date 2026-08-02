from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class AssertionSpec:
    type: str
    value: str | None = None
    pattern: str | None = None
    schema: dict[str, Any] | bool | None = None
    path: tuple[str, ...] = ()
    max_ms: float | None = None
    entrypoint: str | None = None
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AssertionOutcome:
    index: int
    type: str
    passed: bool
    reason_code: str
    message: str | None = None


@dataclass(frozen=True)
class TestCase:
    case_id: str
    prompt: str
    expected_contains: str | None = None
    assertions: tuple[AssertionSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def effective_assertions(self) -> tuple[AssertionSpec, ...]:
        legacy = (
            (AssertionSpec(type="contains", value=self.expected_contains),)
            if self.expected_contains is not None
            else ()
        )
        return legacy + self.assertions


@dataclass(frozen=True)
class AdapterResponse:
    output: str
    input_tokens: int | None = None
    output_tokens: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CaseResult:
    case_id: str
    iteration: int
    success: bool
    latency_ms: float
    failure_kind: str = ""
    failure_category: str = ""
    error_code: str = ""
    output: str = ""
    error: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    tool_path: list[str] = field(default_factory=list)
    assertions: list[AssertionOutcome] = field(default_factory=list)
    input_cost_usd: str | None = None
    output_cost_usd: str | None = None
    cost_usd: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    shard_index: int
    shard_count: int
    duration_ms: float
    total: int
    succeeded: int
    failed: int
    success_rate: float
    error_rate: float
    p95_latency_ms: float
    input_tokens: int
    output_tokens: int
    thresholds_passed: bool
    threshold_failures: list[str]
    input_cost_usd: str | None = None
    output_cost_usd: str | None = None
    cost_usd: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
