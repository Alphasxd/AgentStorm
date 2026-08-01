from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class TestCase:
    case_id: str
    prompt: str
    expected_contains: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


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
    output: str = ""
    error: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None

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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
